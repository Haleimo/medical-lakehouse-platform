"""
Medical Data Lakehouse -- main orchestration DAG.

extract_fhir -> validate_fhir -> write_bronze -> deidentify -> spark_transform
    -> data_quality -> write_silver -> build_gold -> post_load_validation

Design notes:
  * This DAG contains almost no business logic. Every task calls into the
    reusable, independently-unit-tested modules under `src/` (and, for the
    distributed transform step, submits the Spark job in `spark/jobs/`).
    That is what makes the pipeline testable outside of Airflow and keeps
    the DAG file itself simple.
  * `validate_fhir` + `write_bronze` are actually performed together by
    `run_ingestion` (validation happens per-record as part of ingestion, and
    invalid records are quarantined immediately) -- they are still exposed as
    two DAG tasks to make the pipeline stages visible in the Airflow UI and
    to keep the task graph documented in section 11 of the project spec.
  * XComs are used to pass the batch_id and lightweight metrics between
    tasks; large data never goes through XCom -- it goes through
    Bronze/Silver/Gold storage.
  * All environment-specific configuration (bucket names, S3 endpoint,
    schedule) comes from Airflow Variables / environment variables, never
    hardcoded here.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make the reusable src/ package importable from the DAG file. In the Docker
# image, PYTHONPATH already includes the repo root (see docker/Dockerfile.airflow);
# this fallback keeps the DAG importable in ad-hoc/local Airflow setups too.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.ingestion.dicom_ingest import run_dicom_ingestion  # noqa: E402
from src.ingestion.fhir_ingest import new_batch_id, run_ingestion  # noqa: E402
from src.quality.checks import DataQualityError, QualityMetrics, enforce_quality_gate  # noqa: E402
from src.quality.silver_checks import run_silver_quality_checks  # noqa: E402
from src.utils.config import get_settings  # noqa: E402
from src.utils.logging_config import get_logger, log_with_fields  # noqa: E402

logger = get_logger(__name__)

DEFAULT_ARGS = {
    "owner": "medical-data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

# Schedule is configurable via an Airflow Variable / env var so the same DAG
# definition works for a demo (manual trigger) and a "production-like" hourly run.
SCHEDULE = os.environ.get("MEDICAL_PIPELINE_SCHEDULE", "@daily")
SAMPLE_SOURCE_DIR = os.environ.get(
    "FHIR_SOURCE_DIR", os.path.join(REPO_ROOT, "data", "sample", "fhir")
)
DICOM_SOURCE_DIR = os.environ.get(
    "DICOM_SOURCE_DIR", os.path.join(REPO_ROOT, "data", "sample", "dicom")
)


def _task_extract_and_validate(**context) -> str:
    """extract_fhir + validate_fhir + write_bronze (FHIR resources and DICOM metadata).

    Validation happens per record; failures go straight to quarantine and
    never reach Bronze. The task raises `DataQualityError` -- failing the DAG
    run -- if the combined invalid ratio exceeds the configured threshold.
    """
    batch_id = new_batch_id()
    settings = get_settings()

    fhir = run_ingestion(SAMPLE_SOURCE_DIR, batch_id=batch_id)
    combined = QualityMetrics(
        total_records=fhir.metrics.total_records,
        valid_records=fhir.metrics.valid_records,
        invalid_records=fhir.metrics.invalid_records,
        rejected_records=fhir.metrics.rejected_records,
    )
    if os.path.exists(DICOM_SOURCE_DIR):
        dicom = run_dicom_ingestion(DICOM_SOURCE_DIR, batch_id=batch_id)
        for f in ("total_records", "valid_records", "invalid_records", "rejected_records"):
            setattr(combined, f, getattr(combined, f) + getattr(dicom.metrics, f))
    else:
        logger.warning("DICOM source directory not found, skipping", extra={"fields": {"path": DICOM_SOURCE_DIR}})

    try:
        enforce_quality_gate(combined, max_invalid_ratio=settings.max_invalid_ratio)
    except DataQualityError as exc:
        logger.error("ingestion quality gate failed", extra={"fields": {"batch_id": batch_id, "error": str(exc)}})
        raise

    context["ti"].xcom_push(key="batch_id", value=batch_id)
    context["ti"].xcom_push(key="ingestion_metrics", value=combined.as_dict())
    return batch_id


def _task_deidentify(**context) -> dict:
    """De-identify every Bronze Patient record written by the previous task.

    This demonstrates the de-identification step at the orchestration level;
    the heavier, distributed de-identification + transformation of the full
    batch (including non-Patient resources) happens in the Spark job invoked
    by `spark_transform`, using the same `src.deidentification` functions.
    """
    import json
    from pathlib import Path

    from src.deidentification.deidentify import deidentify_row
    from src.storage.s3_client import StorageClient

    batch_id = context["ti"].xcom_pull(key="batch_id", task_ids="extract_validate_fhir")
    settings = get_settings()
    storage = StorageClient()
    bronze_dir = Path(settings.local_data_root) / "bronze" / batch_id
    deidentified_count = 0

    for resource_file in bronze_dir.rglob("*.json"):
        record = json.loads(resource_file.read_text())
        resource = record.get("resource", {})
        resource_type = resource.get("resourceType")
        if resource_type not in {"Patient", "Observation", "Encounter", "Condition", "MedicationRequest"}:
            continue
        from src.fhir.parser import parse_resource

        flat_row = parse_resource(resource)
        deidentified = deidentify_row(resource_type, flat_row)
        key = f"{batch_id}/deidentified/{resource_type}/{resource.get('id')}.json"
        storage.write_json("silver_staging" if settings.is_local else settings.bucket_bronze, key, deidentified)
        deidentified_count += 1

    log_with_fields(logger, 20, "deidentification complete", batch_id=batch_id, records=deidentified_count)
    return {"batch_id": batch_id, "deidentified_records": deidentified_count}


def _task_spark_transform(**context) -> str:
    """Submit the Bronze -> Silver Spark job.

    In a real cluster this shells out to `spark-submit` (see
    spark/jobs/bronze_to_silver.py, and helm/spark-job-template for the K8s
    submission spec). For local development / this demo, the same PySpark
    entrypoint is invoked in local[*] mode so the whole DAG can run on a
    laptop with no Spark cluster.
    """
    import subprocess

    batch_id = context["ti"].xcom_pull(key="batch_id", task_ids="extract_validate_fhir")
    job_path = os.path.join(REPO_ROOT, "spark", "jobs", "bronze_to_silver.py")
    cmd = [sys.executable, job_path, "--batch-id", batch_id]
    logger.info("submitting spark job", extra={"fields": {"cmd": " ".join(cmd)}})
    completed = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ})
    if completed.returncode != 0:
        logger.error("spark job failed", extra={"fields": {"stdout": completed.stdout, "stderr": completed.stderr}})
        raise RuntimeError(f"spark_transform failed for batch {batch_id}: {completed.stderr[-2000:]}")
    logger.info("spark job completed", extra={"fields": {"stdout": completed.stdout[-2000:]}})
    return batch_id


def _task_data_quality(**context) -> dict:
    """Run the Silver quality checks (src/quality/silver_checks.py) and enforce the gate.

    The checks themselves live in `src` so they are unit tested against real
    Parquet fixtures; this task only wires them into Airflow.
    """
    batch_id = context["ti"].xcom_pull(key="batch_id", task_ids="extract_validate_fhir")
    settings = get_settings()
    silver_dir = Path(settings.local_data_root) / "silver" / batch_id

    metrics = run_silver_quality_checks(silver_dir)
    if metrics.total_records == 0:
        # A gate that inspected nothing must not pass: that is exactly how a
        # broken reader used to make this task a silent no-op.
        raise DataQualityError(f"data_quality found no Silver records under {silver_dir}")
    for err in metrics.errors[:20]:
        logger.warning("silver quality finding", extra={"fields": {"batch_id": batch_id, "finding": err}})
    enforce_quality_gate(metrics, max_invalid_ratio=settings.max_invalid_ratio)
    log_with_fields(logger, 20, "post-transform data quality complete", batch_id=batch_id, **metrics.as_dict())
    return metrics.as_dict()


def _task_write_silver(**context) -> str:
    """Silver tables are written by the Spark job (bronze_to_silver.py) inside
    `spark_transform`. This task is the explicit checkpoint in the graph: it
    verifies the core Silver tables exist before Gold is built.
    """
    batch_id = context["ti"].xcom_pull(key="batch_id", task_ids="extract_validate_fhir")
    settings = get_settings()
    silver_dir = Path(settings.local_data_root) / "silver" / batch_id
    missing = [t for t in ("patient", "observation") if not (silver_dir / t).exists()]
    if missing:
        raise RuntimeError(f"Expected Silver tables {missing} under {silver_dir} but they do not exist")
    present = sorted(p.name for p in silver_dir.iterdir() if p.is_dir())
    logger.info("silver layer verified", extra={"fields": {"batch_id": batch_id, "tables": present}})
    return batch_id


def _task_build_gold(**context) -> str:
    import subprocess

    batch_id = context["ti"].xcom_pull(key="batch_id", task_ids="extract_validate_fhir")
    job_path = os.path.join(REPO_ROOT, "spark", "jobs", "silver_to_gold.py")
    cmd = [sys.executable, job_path, "--batch-id", batch_id]
    completed = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ})
    if completed.returncode != 0:
        raise RuntimeError(f"build_gold failed for batch {batch_id}: {completed.stderr[-2000:]}")
    logger.info("gold layer built", extra={"fields": {"batch_id": batch_id}})
    return batch_id


def _task_post_load_validation(**context) -> None:
    from pathlib import Path

    batch_id = context["ti"].xcom_pull(key="batch_id", task_ids="extract_validate_fhir")
    settings = get_settings()
    gold_dir = Path(settings.local_data_root) / "gold" / batch_id
    expected_tables = ["patient_summary", "observation_daily_summary", "encounter_summary", "condition_summary"]
    missing = [t for t in expected_tables if not (gold_dir / t).exists()]
    if missing:
        raise RuntimeError(f"post_load_validation failed: missing Gold tables {missing} for batch {batch_id}")
    logger.info("post-load validation passed", extra={"fields": {"batch_id": batch_id}})


with DAG(
    dag_id="medical_data_pipeline",
    default_args=DEFAULT_ARGS,
    description="FHIR ingestion -> validation -> de-identification -> Spark transform -> Iceberg Silver/Gold",
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=["medical", "fhir", "lakehouse", "iceberg"],
) as dag:

    extract_validate_fhir = PythonOperator(
        task_id="extract_validate_fhir",
        python_callable=_task_extract_and_validate,
    )

    deidentify = PythonOperator(
        task_id="deidentify",
        python_callable=_task_deidentify,
    )

    spark_transform = PythonOperator(
        task_id="spark_transform",
        python_callable=_task_spark_transform,
    )

    data_quality = PythonOperator(
        task_id="data_quality",
        python_callable=_task_data_quality,
    )

    write_silver = PythonOperator(
        task_id="write_silver",
        python_callable=_task_write_silver,
    )

    build_gold = PythonOperator(
        task_id="build_gold",
        python_callable=_task_build_gold,
    )

    post_load_validation = PythonOperator(
        task_id="post_load_validation",
        python_callable=_task_post_load_validation,
    )

    (
        extract_validate_fhir
        >> deidentify
        >> spark_transform
        >> data_quality
        >> write_silver
        >> build_gold
        >> post_load_validation
    )
