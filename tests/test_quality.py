import pytest

from src.quality.checks import (
    DataQualityError,
    QualityMetrics,
    check_no_forbidden_fields,
    check_observation_values,
    check_referential_integrity,
    check_required_not_null,
    check_valid_dates,
    deduplicate,
    enforce_quality_gate,
    find_duplicate_ids,
    find_missing_required,
    find_referential_violations,
)


def test_check_required_not_null_flags_missing_fields():
    rows = [{"id": "1", "status": "final"}, {"id": "2", "status": None}]
    errors = check_required_not_null(rows, ["status"])
    assert len(errors) == 1
    assert "status" in errors[0]


def test_find_duplicate_ids():
    rows = [{"id": "1"}, {"id": "2"}, {"id": "1"}]
    assert find_duplicate_ids(rows) == ["1"]


def test_deduplicate_keeps_first_occurrence():
    rows = [{"id": "1", "v": "a"}, {"id": "1", "v": "b"}, {"id": "2", "v": "c"}]
    result = deduplicate(rows)
    assert len(result) == 2
    assert result[0]["v"] == "a"


def test_check_valid_dates():
    rows = [{"id": "1", "effective_datetime": "2026-01-05T09:00:00"}, {"id": "2", "effective_datetime": "not-a-date"}]
    errors = check_valid_dates(rows, ["effective_datetime"])
    assert len(errors) == 1
    assert "2" in errors[0] or "not-a-date" in errors[0]


def test_check_referential_integrity_detects_orphan():
    children = [{"id": "obs-1", "patient_id": "p1"}, {"id": "obs-2", "patient_id": "p-missing"}]
    errors = check_referential_integrity(children, parent_ids={"p1"})
    assert len(errors) == 1
    assert "p-missing" in errors[0]


def test_check_observation_values_flags_missing_value():
    rows = [{"id": "obs-1", "status": "final", "value_quantity_value": None, "value_string": None}]
    errors = check_observation_values(rows)
    assert len(errors) == 1


def test_check_no_forbidden_fields_detects_raw_phi():
    rows = [{"id": "1", "ssn": "123-45-6789"}]
    errors = check_no_forbidden_fields(rows)
    assert len(errors) == 1
    assert "ssn" in errors[0]


def test_enforce_quality_gate_raises_when_threshold_exceeded():
    metrics = QualityMetrics(total_records=10, valid_records=5, invalid_records=5)
    with pytest.raises(DataQualityError):
        enforce_quality_gate(metrics, max_invalid_ratio=0.2)


def test_enforce_quality_gate_passes_within_threshold():
    metrics = QualityMetrics(total_records=10, valid_records=9, invalid_records=1)
    enforce_quality_gate(metrics, max_invalid_ratio=0.2)  # should not raise


def test_find_missing_required_returns_structured_issues_with_custom_key():
    rows = [{"sop_instance_uid": "1.2", "modality": None}, {"sop_instance_uid": None, "modality": "CT"}]
    issues = find_missing_required(rows, ["sop_instance_uid", "modality"], id_field="sop_instance_uid")
    assert [(i.row_index, i.record_id, i.field) for i in issues] == [(0, "1.2", "modality"), (1, None, "sop_instance_uid")]


def test_check_required_not_null_respects_id_field_in_message():
    errors = check_required_not_null([{"sop_instance_uid": "1.2", "modality": None}], ["modality"], id_field="sop_instance_uid")
    assert "id=1.2" in errors[0]


def test_find_referential_violations_null_fk_is_allowed_and_index_is_reported():
    rows = [{"id": "a", "encounter_id": None}, {"id": "b", "encounter_id": "e-missing"}, {"id": "c", "encounter_id": "e1"}]
    issues = find_referential_violations(rows, {"e1"}, fk_field="encounter_id")
    assert [(i.row_index, i.record_id) for i in issues] == [(1, "b")]
