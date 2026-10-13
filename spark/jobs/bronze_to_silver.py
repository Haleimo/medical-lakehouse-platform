"""
Spark job: Bronze -> Silver

    1. Read Bronze records (raw FHIR resources + DICOM metadata) for a batch.
    2. Parse each into a flat row (src.fhir.parser) and de-identify it
       (src.deidentification.deidentify).
    3. Deduplicate by natural key.
    4. Referential integrity, in two passes:
         a. patient_id must exist in patient (DICOM: patient_id must match a
            patient.mrn_hash);
         b. encounter_id must exist in the *surviving* encounters.
       Violating rows are written to QUARANTINE (with reason + metadata) and
       excluded from Silver -- they are never silently dropped.
    5. Quality gate: fail the run if the quarantined share exceeds
       MAX_INVALID_RATIO, *before* anything is written to Silver.
    6. Assert no raw PHI column can reach Silver, then write Silver.

Output target depends on RUN_MODE:
    * local   (default) -> Parquet under data/silver/<batch_id>/<table>/,
                           Spark local[*]. Needs only `pip install pyspark`.
    * cluster           -> Iceberg tables `iceberg.medical.<table>` in Nessie:
                           created on first run, then MERGE INTO by key, so
                           batches accumulate and re-runs are idempotent.
                           Needs the Iceberg/Nessie jars (config/spark-defaults.conf).
                           NOT exercised against a live cluster in this repo's
                           tests -- see spark/jobs/common.py.

In cluster mode, parent keys already in Silver from earlier batches also count
when checking references (an Observation may arrive after its Patient).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = str(Path(__file__).resolve().parents[2])
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from spark.jobs.common import (  # noqa: E402
    build_spark_session,
    coerce_rows,
    ensure_namespace,
    upsert_iceberg_table,
)
from src.deidentification.deidentify import deidentify_dicom_metadata, deidentify_row  # noqa: E402
from src.fhir.parser import parse_resource  # noqa: E402
from src.ingestion.dicom_ingest import BRONZE_SUBDIR as DICOM_BRONZE_SUBDIR  # noqa: E402
from src.ingestion.quarantine import write_quarantine_record  # noqa: E402
from src.quality.checks import (  # noqa: E402
    QualityMetrics,
    check_no_forbidden_fields,
    deduplicate,
    enforce_quality_gate,
    find_referential_violations,
)
from src.storage.s3_client import StorageClient  # noqa: E402
from src.utils.config import get_settings  # noqa: E402
from src.utils.logging_config import get_logger, log_with_fields  # noqa: E402

logger = get_logger(__name__)

__all__ = ["build_spark_session", "run"]  # build_spark_session re-exported for compatibility

RESOURCE_TABLES = {
    "Patient": "patient",
    "Observation": "observation",
    "Encounter": "encounter",
    "Condition": "condition",
    "MedicationRequest": "medication_request",
}
DICOM_TABLE = "dicom_metadata"
TABLE_KEYS = {**{t: "id" for t in RESOURCE_TABLES.values()}, DICOM_TABLE: "sop_instance_uid"}

PATIENT_FK_TABLES = ("encounter", "observation", "condition", "medication_request")
ENCOUNTER_FK_TABLES = ("observation", "condition", "medication_request")


def _silver_schemas():
    """Explicit PySpark schemas for each Silver table (match sql/silver/*.sql).

    Declared explicitly because small batches routinely contain all-null
    columns, which defeats Spark's schema inference.
    """
    from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType

    def s(name, nullable=True):
        return StructField(name, StringType(), nullable)

    common_tail = [s("batch_id"), s("source_file"), s("resource_type")]
    return {
        "patient": StructType(
            [
                s("id", False),
                StructField("active", BooleanType(), True),
                s("gender"),
                StructField("deceased_boolean", BooleanType(), True),
                s("deceased_datetime"),
                s("state"),
                s("country"),
                s("version_id"),
                s("last_updated"),
                s("mrn_hash"),
                StructField("birth_year", IntegerType(), True),
                s("age_band"),
                s("postal_code_prefix"),
            ]
            + common_tail
        ),
        "observation": StructType(
            [
                s("id", False),
                s("status"),
                s("category_code"),
                s("category_display"),
                s("code_system"),
                s("code"),
                s("code_display"),
                s("patient_id"),
                s("encounter_id"),
                s("effective_datetime"),
                StructField("value_quantity_value", DoubleType(), True),
                s("value_quantity_unit"),
                s("value_string"),
                s("interpretation_code"),
            ]
            + common_tail
        ),
        "encounter": StructType(
            [
                s("id", False),
                s("status"),
                s("class_code"),
                s("type_code"),
                s("type_display"),
                s("patient_id"),
                s("period_start"),
                s("period_end"),
                s("service_provider"),
            ]
            + common_tail
        ),
        "condition": StructType(
            [
                s("id", False),
                s("clinical_status"),
                s("verification_status"),
                s("code"),
                s("code_display"),
                s("patient_id"),
                s("encounter_id"),
                s("onset_datetime"),
                s("recorded_date"),
            ]
            + common_tail
        ),
        "medication_request": StructType(
            [
                s("id", False),
                s("status"),
                s("intent"),
                s("medication_code"),
                s("medication_display"),
                s("patient_id"),
                s("encounter_id"),
                s("authored_on"),
                s("dosage_text"),
            ]
            + common_tail
        ),
        DICOM_TABLE: StructType(
            [
                s("sop_instance_uid", False),
                s("series_instance_uid"),
                s("study_instance_uid"),
                s("patient_id"),
                s("modality"),
                s("body_part_examined"),
                s("study_date"),
                s("manufacturer"),
                s("institution_name"),
                StructField("rows", IntegerType(), True),
                StructField("columns", IntegerType(), True),
                s("pixel_spacing"),
                StructField("slice_thickness", DoubleType(), True),
                StructField("patient_birth_year", IntegerType(), True),
                s("batch_id"),
            ]
        ),
    }


def _read_json_dir(directory: Path) -> list[dict]:
    if not directory.exists():
        return []
    return [json.loads(f.read_text()) for f in sorted(directory.glob("*.json"))]


def load_bronze_records(bronze_dir: Path) -> list[dict]:
    """Bronze FHIR records (`{"resource": ..., ingestion metadata}`)."""
    records: list[dict] = []
    for resource_type in RESOURCE_TABLES:
        records += _read_json_dir(bronze_dir / resource_type)
    return records


def load_bronze_dicom(bronze_dir: Path) -> list[dict]:
    return _read_json_dir(bronze_dir / DICOM_BRONZE_SUBDIR)


def transform_batch(
    bronze_records: list[dict], dicom_records: list[dict] | None = None
) -> tuple[dict[str, list[dict]], int]:
    """Parse + de-identify + deduplicate every Bronze record.

    Returns ({table_name: rows}, duplicates_removed). Rows carry a private
    ``_ingestion_timestamp`` so a later quarantine record can keep the original
    ingestion time; it is not part of any Silver schema.
    """
    tables: dict[str, list[dict]] = {t: [] for t in TABLE_KEYS}

    for record in bronze_records:
        resource = record.get("resource", {})
        resource_type = resource.get("resourceType")
        if resource_type not in RESOURCE_TABLES:
            continue
        row = deidentify_row(resource_type, parse_resource(resource))
        row["batch_id"] = record.get("batch_id")
        row["source_file"] = record.get("source_file")
        row["_ingestion_timestamp"] = record.get("ingestion_timestamp")
        tables[RESOURCE_TABLES[resource_type]].append(row)

    for record in dicom_records or []:
        row = deidentify_dicom_metadata(record.get("resource", {}))
        row["batch_id"] = record.get("batch_id")
        row["source_file"] = record.get("source_file")
        row["_ingestion_timestamp"] = record.get("ingestion_timestamp")
        row["resource_type"] = "DicomMetadata"
        tables[DICOM_TABLE].append(row)

    duplicates = 0
    for table, key in TABLE_KEYS.items():
        deduped = deduplicate(tables[table], id_field=key)
        duplicates += len(tables[table]) - len(deduped)
        tables[table] = deduped
    return tables, duplicates


def quarantine_referential_failures(
    tables: dict[str, list[dict]],
    extra_patient_ids: set[str] | frozenset = frozenset(),
    extra_encounter_ids: set[str] | frozenset = frozenset(),
    extra_mrn_hashes: set[str] | frozenset = frozenset(),
) -> tuple[dict[str, list[dict]], list[dict]]:
    """Split rows into (clean tables, quarantined entries).

    A quarantined entry is ``{"table", "row", "fk_field", "message"}``. The
    `extra_*` sets are parent keys already present in Silver from earlier
    batches (cluster mode).
    """
    cleaned = {t: list(rows) for t, rows in tables.items()}
    quarantined: list[dict] = []

    def apply(table: str, parent_ids: set, fk_field: str) -> None:
        rows = cleaned.get(table, [])
        if not rows:
            return
        key = TABLE_KEYS[table]
        issues = find_referential_violations(rows, parent_ids, fk_field, id_field=key)
        bad = {i.row_index: i for i in issues}
        for idx, issue in bad.items():
            quarantined.append({"table": table, "row": rows[idx], "fk_field": fk_field, "message": issue.message})
        cleaned[table] = [r for i, r in enumerate(rows) if i not in bad]

    # pass 1: patient references
    patient_ids = {r["id"] for r in cleaned["patient"] if r.get("id")} | set(extra_patient_ids)
    for table in PATIENT_FK_TABLES:
        apply(table, patient_ids, "patient_id")
    mrn_hashes = {r["mrn_hash"] for r in cleaned["patient"] if r.get("mrn_hash")} | set(extra_mrn_hashes)
    apply(DICOM_TABLE, mrn_hashes, "patient_id")

    # pass 2: encounter references, against encounters that survived pass 1
    encounter_ids = {r["id"] for r in cleaned["encounter"] if r.get("id")} | set(extra_encounter_ids)
    for table in ENCOUNTER_FK_TABLES:
        apply(table, encounter_ids, "encounter_id")

    if quarantined:
        logger.warning(
            "rows failed referential integrity and will be quarantined",
            extra={"fields": {"count": len(quarantined), "tables": sorted({q["table"] for q in quarantined})}},
        )
    return cleaned, quarantined


def persist_quarantine(quarantined: list[dict], batch_id: str, storage: StorageClient | None = None) -> list[str]:
    """Write every quarantined row (already de-identified) to the quarantine area."""
    storage = storage or StorageClient()
    paths = []
    for entry in quarantined:
        row = entry["row"]
        _, path = write_quarantine_record(
            storage,
            batch_id=batch_id,
            subdir=f"referential/{entry['table']}",
            record_id=row.get(TABLE_KEYS[entry["table"]]),
            error_type="ReferentialIntegrityError",
            error_message=entry["message"],
            source_file=row.get("source_file"),
            ingestion_timestamp=row.get("_ingestion_timestamp"),
            resource_type=row.get("resource_type"),
            payload={k: v for k, v in row.items() if not k.startswith("_")},
        )
        paths.append(path)
    return paths


def assert_no_phi_leak(tables: dict[str, list[dict]]) -> None:
    for table_name, rows in tables.items():
        errors = check_no_forbidden_fields(rows)
        if errors:
            raise RuntimeError(f"PHI leak detected in silver table '{table_name}': {errors[:5]}")


def write_silver(spark, tables: dict[str, list[dict]], batch_id: str) -> dict[str, int]:
    settings = get_settings()
    schemas = _silver_schemas()
    written: dict[str, int] = {}
    if not settings.is_local:
        ensure_namespace(spark, settings.iceberg_catalog_name, "medical")

    for table_name, rows in tables.items():
        if not rows:
            logger.info("no rows for table, skipping write", extra={"fields": {"table": table_name}})
            continue
        schema = schemas[table_name]
        df = spark.createDataFrame(coerce_rows(rows, schema), schema=schema)
        if settings.is_local:
            out_path = Path(settings.local_data_root) / "silver" / batch_id / table_name
            df.write.mode("overwrite").parquet(str(out_path))
            mode = "parquet"
        else:
            full_table = f"{settings.iceberg_catalog_name}.medical.{table_name}"
            mode = upsert_iceberg_table(spark, df, full_table, table_name, key=TABLE_KEYS[table_name])
        written[table_name] = len(rows)
        log_with_fields(
            logger, 20, "wrote silver table", table=table_name, row_count=len(rows), batch_id=batch_id, mode=mode
        )
    return written


def _existing_keys(spark, table: str, column: str) -> set:
    settings = get_settings()
    full_table = f"{settings.iceberg_catalog_name}.medical.{table}"
    if not spark.catalog.tableExists(full_table):
        return set()
    # Collected to the driver: fine at portfolio scale; at real scale this
    # would be an anti-join in Spark rather than a Python set.
    return {r[0] for r in spark.table(full_table).select(column).collect() if r[0] is not None}


def run(batch_id: str) -> dict:
    settings = get_settings()
    bronze_dir = Path(settings.local_data_root) / "bronze" / batch_id
    if not bronze_dir.exists():
        raise FileNotFoundError(f"Bronze directory not found for batch {batch_id}: {bronze_dir}")

    fhir_records = load_bronze_records(bronze_dir)
    dicom_records = load_bronze_dicom(bronze_dir)
    log_with_fields(
        logger,
        20,
        "loaded bronze records",
        batch_id=batch_id,
        fhir_records=len(fhir_records),
        dicom_records=len(dicom_records),
    )

    tables, duplicates = transform_batch(fhir_records, dicom_records)
    total_rows = sum(len(r) for r in tables.values())

    spark = build_spark_session(app_name="medical-bronze-to-silver")
    try:
        extras: dict = {}
        if not settings.is_local:
            extras = {
                "extra_patient_ids": _existing_keys(spark, "patient", "id"),
                "extra_encounter_ids": _existing_keys(spark, "encounter", "id"),
                "extra_mrn_hashes": _existing_keys(spark, "patient", "mrn_hash"),
            }
        tables, quarantined = quarantine_referential_failures(tables, **extras)
        persist_quarantine(quarantined, batch_id)

        metrics = QualityMetrics(
            total_records=total_rows,
            valid_records=total_rows - len(quarantined),
            invalid_records=len(quarantined),
            duplicate_records=duplicates,
            rejected_records=len(quarantined),
        )
        log_with_fields(logger, 20, "silver quality metrics", batch_id=batch_id, **metrics.as_dict())
        enforce_quality_gate(metrics)  # raises DataQualityError; nothing has been written to Silver yet

        assert_no_phi_leak(tables)
        written = write_silver(spark, tables, batch_id)
    finally:
        spark.stop()

    summary = {"tables": written, "quarantined": len(quarantined), "duplicates_removed": duplicates}
    log_with_fields(logger, 20, "bronze_to_silver complete", batch_id=batch_id, **summary)
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze -> Silver Spark transformation")
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.batch_id), indent=2))
