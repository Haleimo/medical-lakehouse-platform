from src.fhir.validator import extract_resources, validate_resource


def test_validate_resource_accepts_valid_patient(sample_patient_resource):
    result = validate_resource(sample_patient_resource)
    assert result.is_valid
    assert result.resource_type == "Patient"


def test_validate_resource_rejects_unsupported_type():
    result = validate_resource({"resourceType": "LabReport", "id": "x"})
    assert not result.is_valid
    assert "unsupported resourceType" in result.errors[0]


def test_validate_resource_rejects_missing_required_field():
    result = validate_resource({"resourceType": "Observation", "id": "obs-1"})
    assert not result.is_valid
    assert any("status" in e for e in result.errors)


def test_validate_resource_rejects_invalid_status():
    result = validate_resource({
        "resourceType": "Observation", "id": "obs-1", "status": "bogus",
        "code": {"coding": [{"code": "1"}]},
    })
    assert not result.is_valid


def test_validate_resource_rejects_invalid_birth_date():
    result = validate_resource({"resourceType": "Patient", "id": "p1", "birthDate": "not-a-date"})
    assert not result.is_valid


def test_extract_resources_from_bundle():
    bundle = {
        "resourceType": "Bundle",
        "entry": [
            {"resource": {"resourceType": "Patient", "id": "p1"}},
            {"resource": {"resourceType": "Encounter", "id": "e1", "status": "finished"}},
        ],
    }
    resources = extract_resources(bundle)
    assert len(resources) == 2
    assert resources[0]["resourceType"] == "Patient"


def test_extract_resources_from_single_resource():
    resource = {"resourceType": "Patient", "id": "p1"}
    assert extract_resources(resource) == [resource]
