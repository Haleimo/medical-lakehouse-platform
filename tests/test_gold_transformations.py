import pandas as pd

from src.transformations.gold import (
    condition_summary,
    encounter_summary,
    medication_summary,
    observation_daily_summary,
    patient_summary,
)


def test_patient_summary_counts_related_records():
    patients = pd.DataFrame([
        {"id": "p1", "gender": "female", "age_band": "40-49", "birth_year": 1982},
        {"id": "p2", "gender": "male", "age_band": "60-69", "birth_year": 1957},
    ])
    encounters = pd.DataFrame([{"id": "e1", "patient_id": "p1"}, {"id": "e2", "patient_id": "p1"}])
    conditions = pd.DataFrame([{"id": "c1", "patient_id": "p1"}])
    medications = pd.DataFrame([{"id": "m1", "patient_id": "p2"}])

    result = patient_summary(patients, encounters, conditions, medications)
    row_p1 = result[result["patient_id"] == "p1"].iloc[0]
    row_p2 = result[result["patient_id"] == "p2"].iloc[0]

    assert row_p1["encounter_count"] == 2
    assert row_p1["condition_count"] == 1
    assert row_p1["medication_count"] == 0
    assert row_p2["medication_count"] == 1


def test_observation_daily_summary_aggregates_by_day_and_code():
    observations = pd.DataFrame([
        {"id": "o1", "effective_datetime": "2026-01-05T09:00:00+00:00", "code_system": "http://loinc.org", "code": "8480-6",
         "code_display": "Systolic BP", "value_quantity_value": 120.0},
        {"id": "o2", "effective_datetime": "2026-01-05T10:00:00+00:00", "code_system": "http://loinc.org", "code": "8480-6",
         "code_display": "Systolic BP", "value_quantity_value": 130.0},
    ])
    result = observation_daily_summary(observations)
    assert len(result) == 1
    assert result.iloc[0]["observation_count"] == 2
    assert result.iloc[0]["avg_value"] == 125.0


def test_encounter_summary_groups_by_class_and_status():
    encounters = pd.DataFrame([
        {"class_code": "AMB", "status": "finished"},
        {"class_code": "AMB", "status": "finished"},
        {"class_code": "EMER", "status": "finished"},
    ])
    result = encounter_summary(encounters)
    amb_row = result[result["class_code"] == "AMB"].iloc[0]
    assert amb_row["encounter_count"] == 2


def test_condition_summary_counts_distinct_patients():
    conditions = pd.DataFrame([
        {"id": "c1", "code": "44054006", "code_display": "T2DM", "patient_id": "p1"},
        {"id": "c2", "code": "44054006", "code_display": "T2DM", "patient_id": "p1"},
        {"id": "c3", "code": "44054006", "code_display": "T2DM", "patient_id": "p2"},
    ])
    result = condition_summary(conditions)
    assert result.iloc[0]["condition_count"] == 3
    assert result.iloc[0]["patient_count"] == 2


def test_medication_summary_empty_input_returns_empty_frame():
    result = medication_summary(pd.DataFrame())
    assert list(result.columns) == ["medication_code", "medication_display", "prescription_count", "patient_count"]
    assert result.empty


def test_no_rows_lost_for_observations_without_a_coding_system():
    # Regression: pandas groupby drops null keys by default, so observations
    # lacking a code system vanished from Gold (3 rows in Silver -> 1 in Gold).
    obs = pd.DataFrame([
        {"id": "a", "effective_datetime": "2026-01-05T09:00:00+00:00", "code_system": "http://loinc.org",
         "code": "X1", "code_display": "d", "value_quantity_value": 1.0},
        {"id": "b", "effective_datetime": "2026-01-05T09:00:00+00:00", "code_system": "http://local",
         "code": "X1", "code_display": "d", "value_quantity_value": 2.0},
        {"id": "c", "effective_datetime": "2026-01-05T09:00:00+00:00", "code_system": None,
         "code": "X1", "code_display": "d", "value_quantity_value": 3.0},
    ])
    assert observation_daily_summary(obs)["observation_count"].sum() == len(obs)


def test_no_rows_lost_for_null_group_keys_in_other_summaries():
    enc = pd.DataFrame([{"class_code": None, "status": "finished"}, {"class_code": "AMB", "status": "finished"}])
    assert encounter_summary(enc)["encounter_count"].sum() == 2
    cond = pd.DataFrame([{"id": "1", "code": None, "code_display": None, "patient_id": "p"},
                         {"id": "2", "code": "c", "code_display": "d", "patient_id": "p"}])
    assert condition_summary(cond)["condition_count"].sum() == 2


def test_patient_summary_counts_are_integers():
    patients = pd.DataFrame([{"id": "p1", "gender": "f", "age_band": "40-49", "birth_year": 1980}])
    result = patient_summary(patients, pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    for col in ("encounter_count", "condition_count", "medication_count"):
        assert str(result[col].dtype) == "int64"
