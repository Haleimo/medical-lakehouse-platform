from src.fhir.parser import (
    parse_condition,
    parse_encounter,
    parse_medication_request,
    parse_observation,
    parse_patient,
    parse_resource,
)


def test_parse_patient_flattens_nested_fields(sample_patient_resource):
    row = parse_patient(sample_patient_resource)
    assert row["id"] == "pat-1001"
    assert row["first_name"] == "Elena"
    assert row["last_name"] == "Rivera"
    assert row["gender"] == "female"
    assert row["birth_date"] == "1982-04-17"
    assert row["mrn"] == "MRN-778812"
    assert row["ssn"] == "123-45-6789"
    assert row["phone"] == "555-201-8834"
    assert row["email"] == "elena.rivera@example.com"
    assert row["postal_code"] == "62704"


def test_parse_observation_extracts_loinc_code_and_value(sample_observation_resource):
    row = parse_observation(sample_observation_resource)
    assert row["id"] == "obs-3001"
    assert row["code"] == "8480-6"
    assert row["code_system"] == "http://loinc.org"
    assert row["patient_id"] == "pat-1001"
    assert row["encounter_id"] == "enc-2001"
    assert row["value_quantity_value"] == 128
    assert row["value_quantity_unit"] == "mmHg"
    assert row["interpretation_code"] == "N"


def test_parse_encounter():
    resource = {
        "resourceType": "Encounter",
        "id": "enc-1",
        "status": "finished",
        "class": {"code": "AMB"},
        "type": [{"coding": [{"code": "185349003", "display": "Checkup"}]}],
        "subject": {"reference": "Patient/p1"},
        "period": {"start": "2026-01-01T00:00:00Z", "end": "2026-01-01T01:00:00Z"},
    }
    row = parse_encounter(resource)
    assert row["patient_id"] == "p1"
    assert row["class_code"] == "AMB"
    assert row["type_display"] == "Checkup"


def test_parse_condition():
    resource = {
        "resourceType": "Condition",
        "id": "cond-1",
        "clinicalStatus": {"coding": [{"code": "active"}]},
        "code": {"coding": [{"code": "44054006", "display": "Type 2 diabetes"}]},
        "subject": {"reference": "Patient/p1"},
    }
    row = parse_condition(resource)
    assert row["clinical_status"] == "active"
    assert row["code"] == "44054006"
    assert row["patient_id"] == "p1"


def test_parse_medication_request():
    resource = {
        "resourceType": "MedicationRequest",
        "id": "med-1",
        "status": "active",
        "intent": "order",
        "medicationCodeableConcept": {"coding": [{"code": "860975", "display": "Metformin 500 MG"}]},
        "subject": {"reference": "Patient/p1"},
        "dosageInstruction": [{"text": "Take once daily"}],
    }
    row = parse_medication_request(resource)
    assert row["medication_code"] == "860975"
    assert row["dosage_text"] == "Take once daily"


def test_parse_resource_dispatch_adds_resource_type(sample_patient_resource):
    row = parse_resource(sample_patient_resource)
    assert row["resource_type"] == "Patient"
    assert row["id"] == "pat-1001"


def _obs(system, code="X1"):
    coding = {"code": code, "display": "Local test"}
    if system:
        coding["system"] = system
    return {
        "resourceType": "Observation", "id": "o1", "status": "final",
        "code": {"coding": [coding]}, "subject": {"reference": "Patient/p1"},
        "valueQuantity": {"value": 5, "unit": "u"},
    }


def test_observation_keeps_code_and_system_for_non_loinc_codes():
    # Regression: code_loinc used to be nulled for non-LOINC codes (and, before
    # that, was set to the code regardless of system). Either way a non-LOINC
    # code must survive parsing together with the system that qualifies it.
    row = parse_observation(_obs("http://hospital.example/local-codes", code="LOC-9"))
    assert row["code"] == "LOC-9"
    assert row["code_system"] == "http://hospital.example/local-codes"
    assert "code_loinc" not in row


def test_observation_without_coding_system_keeps_code():
    row = parse_observation(_obs(None, code="LOC-9"))
    assert row["code"] == "LOC-9"
    assert row["code_system"] is None
