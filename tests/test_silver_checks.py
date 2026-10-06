"""Tests that the Silver checks actually FIND problems.

The original DAG data_quality task read the wrong file format, saw an empty
list, and passed vacuously -- while every test stayed green. These tests feed
real Parquet and assert on findings, so a no-op implementation fails them.
"""
import pandas as pd
import pytest

from src.deidentification.deidentify import hash_identifier
from src.quality.silver_checks import read_silver_table, run_silver_quality_checks


def _write(silver_dir, table, rows):
    path = silver_dir / table
    path.mkdir(parents=True)
    pd.DataFrame(rows).to_parquet(path / "part-0.parquet")


@pytest.fixture()
def silver(tmp_path):
    d = tmp_path / "silver"
    _write(d, "patient", [{"id": "p1", "gender": "f", "mrn_hash": "h1"}, {"id": "p2", "gender": "m", "mrn_hash": "h2"}])
    _write(d, "encounter", [{"id": "e1", "patient_id": "p1", "period_start": "2026-01-05T09:00:00+00:00"}])
    _write(d, "observation", [
        {"id": "o1", "patient_id": "p1", "encounter_id": "e1", "status": "final",
         "value_quantity_value": 5.0, "value_string": None, "effective_datetime": "2026-01-05T09:00:00+00:00"},
    ])
    return d


def test_clean_silver_passes_and_actually_inspects_rows(silver):
    m = run_silver_quality_checks(silver)
    assert m.total_records == 4  # a vacuous pass would report 0
    assert m.invalid_records == 0 and m.valid_records == 4


def test_missing_silver_dir_inspects_nothing(tmp_path):
    assert run_silver_quality_checks(tmp_path / "nope").total_records == 0


def test_detects_orphan_patient_reference(silver):
    _write(silver.parent / "s2", "patient", [{"id": "p1", "mrn_hash": "h1"}])
    _write(silver.parent / "s2", "observation", [{"id": "o9", "patient_id": "ghost", "status": "preliminary"}])
    m = run_silver_quality_checks(silver.parent / "s2")
    assert m.invalid_records == 1
    assert any("ghost" in e for e in m.errors)


def test_detects_dangling_encounter_reference(silver):
    _write(silver.parent / "s3", "patient", [{"id": "p1", "mrn_hash": "h1"}])
    _write(silver.parent / "s3", "condition", [{"id": "c1", "patient_id": "p1", "encounter_id": "e-missing"}])
    m = run_silver_quality_checks(silver.parent / "s3")
    assert m.invalid_records == 1 and any("e-missing" in e for e in m.errors)


def test_detects_forbidden_phi_column(tmp_path):
    _write(tmp_path / "s", "patient", [{"id": "p1", "mrn_hash": "h1", "ssn": "123-45-6789"}])
    m = run_silver_quality_checks(tmp_path / "s")
    assert m.invalid_records == 1 and any("ssn" in e for e in m.errors)


def test_detects_duplicate_keys(tmp_path):
    _write(tmp_path / "s", "patient", [{"id": "p1", "mrn_hash": "h1"}, {"id": "p1", "mrn_hash": "h1"}])
    m = run_silver_quality_checks(tmp_path / "s")
    assert m.duplicate_records == 1 and m.invalid_records == 1


def test_null_numeric_value_counts_as_missing(tmp_path):
    # NaN is not None: without NaN->None normalization this finalized
    # observation would look like it has a value.
    _write(tmp_path / "s", "patient", [{"id": "p1", "mrn_hash": "h1"}])
    _write(tmp_path / "s", "observation", [
        {"id": "o1", "patient_id": "p1", "encounter_id": None, "status": "final",
         "value_quantity_value": float("nan"), "value_string": None}])
    m = run_silver_quality_checks(tmp_path / "s")
    assert m.invalid_records == 1


def test_detects_invalid_date(tmp_path):
    _write(tmp_path / "s", "patient", [{"id": "p1", "mrn_hash": "h1"}])
    _write(tmp_path / "s", "encounter", [{"id": "e1", "patient_id": "p1", "period_start": "garbage"}])
    assert run_silver_quality_checks(tmp_path / "s").invalid_records == 1


def test_dicom_must_link_to_a_patient_mrn_hash(tmp_path):
    good = hash_identifier("MRN-1", salt="s")
    _write(tmp_path / "s", "patient", [{"id": "p1", "mrn_hash": good}])
    _write(tmp_path / "s", "dicom_metadata", [
        {"sop_instance_uid": "1.1", "patient_id": good},
        {"sop_instance_uid": "1.2", "patient_id": "no-such-hash"},
    ])
    m = run_silver_quality_checks(tmp_path / "s")
    assert m.invalid_records == 1 and any("no-such-hash" in e for e in m.errors)


def test_read_silver_table_normalizes_nan_to_none(tmp_path):
    _write(tmp_path, "observation", [{"id": "o1", "value_quantity_value": float("nan")}])
    assert read_silver_table(tmp_path, "observation")[0]["value_quantity_value"] is None
