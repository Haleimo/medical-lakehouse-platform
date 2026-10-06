import json
from pathlib import Path

from src.ingestion.dicom_ingest import run_dicom_ingestion, validate_dicom_record

SAMPLE_DIR = Path(__file__).resolve().parents[1] / "data" / "sample" / "dicom"

VALID = {
    "sop_instance_uid": "1.2.3", "study_instance_uid": "1.2", "modality": "CT",
    "patient_id": "MRN-1", "study_date": "2026-01-05",
    "patient_name": "Doe^Jane", "patient_birth_date": "19800101",
}


def _write(tmp_path, records, name="dicom.json"):
    f = tmp_path / name
    f.write_text(json.dumps(records))
    return f


def test_valid_record_has_no_errors():
    assert validate_dicom_record(VALID) == []


def test_missing_non_key_required_field_is_rejected():
    assert validate_dicom_record({**VALID, "modality": None})


def test_record_missing_its_own_key_is_rejected():
    # Regression: rejections used to be found by parsing "id=..." out of error
    # strings, so a record with no sop_instance_uid produced id=None, matched
    # nothing, was ACCEPTED, and then crashed on record["sop_instance_uid"].
    bad = {k: v for k, v in VALID.items() if k != "sop_instance_uid"}
    errors = validate_dicom_record(bad)
    assert errors and any("sop_instance_uid" in e for e in errors)


def test_invalid_study_date_is_rejected():
    assert validate_dicom_record({**VALID, "study_date": "not-a-date"})


def test_ingestion_rejects_and_quarantines_invalid_records(tmp_settings, tmp_path):
    no_key = {k: v for k, v in VALID.items() if k != "sop_instance_uid"}
    f = _write(tmp_path, [VALID, no_key, {**VALID, "sop_instance_uid": "9.9", "modality": ""}])
    result = run_dicom_ingestion(f, batch_id="b1")

    assert result.metrics.total_records == 3
    assert result.metrics.valid_records == 1
    assert result.metrics.rejected_records == 2
    assert len(result.bronze_paths) == 1 and len(result.quarantine_paths) == 2

    for path in result.quarantine_paths:  # every rejection is persisted with full metadata
        rec = json.loads(Path(path).read_text())
        assert {"source_file", "record_id", "error_type", "error_message", "ingestion_timestamp", "batch_id"} <= rec.keys()
        assert rec["error_type"] == "ValidationError"


def test_bronze_keeps_the_raw_record_deidentification_happens_later(tmp_settings, tmp_path):
    result = run_dicom_ingestion(_write(tmp_path, [VALID]), batch_id="b2")
    stored = json.loads(Path(result.bronze_paths[0]).read_text())
    assert stored["resource"]["patient_name"] == "Doe^Jane"  # Bronze is the untouched original
    assert stored["batch_id"] == "b2" and stored["source_file"] and stored["ingestion_timestamp"]


def test_unreadable_file_is_quarantined_not_ignored(tmp_settings, tmp_path):
    f = tmp_path / "broken.json"
    f.write_text("{ not json")
    result = run_dicom_ingestion(f, batch_id="b3")
    assert result.metrics.rejected_records == 1 and len(result.quarantine_paths) == 1
    assert json.loads(Path(result.quarantine_paths[0]).read_text())["error_type"] == "FileReadError"


def test_sample_dicom_file_ingests_cleanly(tmp_settings):
    result = run_dicom_ingestion(SAMPLE_DIR, batch_id="b4")
    assert result.metrics.valid_records == 2 and result.metrics.rejected_records == 0
