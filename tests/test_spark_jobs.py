"""
Integration tests: real ingestion -> real Spark jobs (local[*]) -> Parquet.

Needs pyspark and a JVM; skipped (with a reason) when either is missing. CI has
both. What these cover that unit tests cannot: the referential-integrity
quarantine actually persists rows, the encounter_id FK is enforced, DICOM
flows to Silver joined on mrn_hash, non-LOINC observations survive to Gold,
and the quality gate stops a bad batch *before* Silver is written.
"""

import json
import shutil
import sys
from pathlib import Path

import pandas as pd
import pytest

pytest.importorskip("pyspark", reason="pyspark not installed")
if not shutil.which("java"):
    pytest.skip("no JVM on PATH; Spark integration tests skipped", allow_module_level=True)

from src.ingestion.dicom_ingest import run_dicom_ingestion  # noqa: E402
from src.ingestion.fhir_ingest import run_ingestion  # noqa: E402
from src.quality.checks import DataQualityError  # noqa: E402
from src.quality.silver_checks import run_silver_quality_checks  # noqa: E402
from src.utils.config import get_settings  # noqa: E402

SAMPLE = Path(__file__).resolve().parents[1] / "data" / "sample"
PHI_COLUMNS = {
    "ssn",
    "first_name",
    "last_name",
    "phone",
    "email",
    "address_line",
    "city",
    "mrn",
    "birth_date",
    "patient_name",
    "patient_birth_date",
    "postal_code",
}


def _obs(obs_id, patient, encounter=None, system="http://loinc.org", code="8480-6"):
    coding = {"code": code, "display": "test"}
    if system:
        coding["system"] = system
    res = {
        "resourceType": "Observation",
        "id": obs_id,
        "status": "final",
        "code": {"coding": [coding]},
        "subject": {"reference": f"Patient/{patient}"},
        "effectiveDateTime": "2026-01-05T10:00:00+00:00",
        "valueQuantity": {"value": 7, "unit": "u"},
    }
    if encounter:
        res["encounter"] = {"reference": f"Encounter/{encounter}"}
    return res


def _add_bronze(root: Path, batch_id: str, resource: dict) -> None:
    d = root / "bronze" / batch_id / resource["resourceType"]
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{resource['id']}.json").write_text(
        json.dumps(
            {
                "resource": resource,
                "ingestion_timestamp": "2026-01-05T00:00:00+00:00",
                "source_file": "handmade.json",
                "batch_id": batch_id,
            }
        )
    )


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("RUN_MODE", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("DEID_SALT", "spark-test-salt")
    monkeypatch.setenv("PYSPARK_PYTHON", sys.executable)
    get_settings(force_reload=True)
    yield tmp_path
    get_settings(force_reload=True)


def _ingest(batch_id):
    run_ingestion(SAMPLE / "fhir", batch_id=batch_id)
    run_dicom_ingestion(SAMPLE / "dicom", batch_id=batch_id)


def _silver(root, batch, table):
    return pd.read_parquet(root / "silver" / batch / table)


def test_end_to_end_with_edge_cases(env):
    from spark.jobs import bronze_to_silver, silver_to_gold

    batch = "it-batch"
    _ingest(batch)
    _add_bronze(env, batch, _obs("obs-orphan-patient", "pat-ghost"))
    _add_bronze(env, batch, _obs("obs-orphan-encounter", "pat-1001", encounter="enc-ghost"))
    _add_bronze(
        env, batch, _obs("obs-local", "pat-1001", "enc-2001", system="http://hospital.example/local", code="L1")
    )
    _add_bronze(env, batch, _obs("obs-nosystem", "pat-1001", "enc-2001", system=None, code="L2"))

    summary = bronze_to_silver.run(batch)
    assert summary["quarantined"] == 2
    assert summary["tables"]["dicom_metadata"] == 2

    # --- quarantine: persisted, with the full metadata contract
    qdir = env / "quarantine" / batch / "referential" / "observation"
    records = {json.loads(f.read_text())["record_id"]: json.loads(f.read_text()) for f in qdir.glob("*.error.json")}
    assert set(records) == {"obs-orphan-patient", "obs-orphan-encounter"}
    for rec in records.values():
        assert {
            "source_file",
            "record_id",
            "error_type",
            "error_message",
            "ingestion_timestamp",
            "batch_id",
        } <= rec.keys()
        assert rec["error_type"] == "ReferentialIntegrityError"
        assert rec["source_file"] == "handmade.json" and rec["ingestion_timestamp"] == "2026-01-05T00:00:00+00:00"
    assert "pat-ghost" in records["obs-orphan-patient"]["error_message"]
    assert "enc-ghost" in records["obs-orphan-encounter"]["error_message"]  # the encounter FK is enforced

    # --- Silver content
    obs = _silver(env, batch, "observation")
    assert set(obs["id"]) == {"obs-3001", "obs-3002", "obs-3003", "obs-3004", "obs-local", "obs-nosystem"}
    patient, dicom = _silver(env, batch, "patient"), _silver(env, batch, "dicom_metadata")
    for table in ("patient", "observation", "encounter", "condition", "medication_request", "dicom_metadata"):
        assert not PHI_COLUMNS & set(_silver(env, batch, table).columns), table
    assert set(dicom["patient_id"]) <= set(patient["mrn_hash"])  # DICOM joins on mrn_hash
    assert run_silver_quality_checks(env / "silver" / batch).invalid_records == 0

    # --- Gold keeps every Silver observation, including non-LOINC / no-system ones
    silver_to_gold.run(batch)
    gold = pd.read_parquet(env / "gold" / batch / "observation_daily_summary")
    assert gold["observation_count"].sum() == len(obs) == 6
    assert str(pd.read_parquet(env / "gold" / batch / "patient_summary")["encounter_count"].dtype) == "int64"


def test_rerunning_a_batch_is_idempotent(env):
    from spark.jobs import bronze_to_silver

    batch = "rerun"
    _ingest(batch)
    first = bronze_to_silver.run(batch)
    second = bronze_to_silver.run(batch)
    assert first == second
    assert len(_silver(env, batch, "patient")) == 3


def test_quality_gate_blocks_silver_when_too_many_rows_are_quarantined(env):
    from spark.jobs import bronze_to_silver

    batch = "bad-batch"
    _ingest(batch)
    for i in range(10):
        _add_bronze(env, batch, _obs(f"orphan-{i}", "pat-ghost"))

    with pytest.raises(DataQualityError):
        bronze_to_silver.run(batch)

    assert not (env / "silver" / batch).exists()  # nothing published from a failed gate
    assert len(list((env / "quarantine" / batch / "referential" / "observation").glob("*.json"))) == 10  # evidence kept


def test_iceberg_casts_produce_real_timestamp_and_date_columns(env):
    """The cluster-mode cast, exercised locally (no Iceberg jars needed for a cast)."""
    from pyspark.sql.types import DateType, TimestampType

    from spark.jobs.common import build_spark_session, prepare_for_iceberg

    spark = build_spark_session("cast-test")
    try:
        obs = prepare_for_iceberg(
            spark.createDataFrame([("2026-01-05T09:00:00+00:00",)], ["effective_datetime"]), "observation"
        )
        assert isinstance(obs.schema["effective_datetime"].dataType, TimestampType)
        assert obs.first()["effective_datetime"] is not None  # offset-bearing ISO string really parsed

        dicom = prepare_for_iceberg(spark.createDataFrame([("2026-01-05",)], ["study_date"]), "dicom_metadata")
        assert isinstance(dicom.schema["study_date"].dataType, DateType)
    finally:
        spark.stop()
