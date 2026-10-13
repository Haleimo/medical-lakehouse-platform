from src.deidentification.deidentify import (
    FORBIDDEN_SILVER_FIELDS,
    deidentify_dicom_metadata,
    deidentify_patient_row,
    deidentify_reference_ids,
    generalize_birth_date,
    generalize_postal_code,
    hash_identifier,
)
from src.fhir.parser import parse_patient


def test_hash_identifier_is_deterministic():
    h1 = hash_identifier("MRN-778812", salt="fixed-salt")
    h2 = hash_identifier("MRN-778812", salt="fixed-salt")
    assert h1 == h2
    assert h1 != "MRN-778812"
    assert len(h1) == 64  # sha256 hex digest


def test_hash_identifier_differs_with_different_salt():
    h1 = hash_identifier("MRN-778812", salt="salt-a")
    h2 = hash_identifier("MRN-778812", salt="salt-b")
    assert h1 != h2


def test_hash_identifier_handles_none():
    assert hash_identifier(None) is None


def test_generalize_postal_code():
    assert generalize_postal_code("62704") == "627"
    assert generalize_postal_code(None) is None
    assert generalize_postal_code("62") == "62"


def test_generalize_birth_date_produces_age_band():
    from datetime import date

    result = generalize_birth_date("1982-04-17", reference_date=date(2026, 1, 5))
    assert result["birth_year"] == 1982
    assert result["age_band"] == "40-49"


def test_generalize_birth_date_caps_at_90_plus():
    from datetime import date

    result = generalize_birth_date("1930-01-01", reference_date=date(2026, 1, 5))
    assert result["age_band"] == "90+"


def test_deidentify_patient_row_removes_direct_identifiers():
    row = {
        "id": "pat-1",
        "mrn": "MRN-778812",
        "ssn": "123-45-6789",
        "first_name": "Elena",
        "last_name": "Rivera",
        "phone": "555-1",
        "email": "e@x.com",
        "address_line": "482 Maple St",
        "city": "Springfield",
        "postal_code": "62704",
        "birth_date": "1982-04-17",
        "gender": "female",
        "state": "IL",
        "country": "US",
    }
    out = deidentify_patient_row(row)

    for forbidden in ["mrn", "ssn", "first_name", "last_name", "phone", "email", "address_line", "city", "birth_date"]:
        assert forbidden not in out

    assert "mrn_hash" in out and out["mrn_hash"] != row["mrn"]
    assert out["postal_code_prefix"] == "627"
    assert out["age_band"] is not None
    assert out["gender"] == "female"  # retained, low-risk
    assert not FORBIDDEN_SILVER_FIELDS.intersection(out.keys())


def test_deidentify_reference_ids_preserves_join_key():
    # patient_id must stay identical to Patient.id (also left unhashed) so
    # Silver tables remain joinable; see docstring in deidentify_reference_ids.
    row = {"id": "obs-1", "patient_id": "pat-1001", "code": "8480-6"}
    out = deidentify_reference_ids(row)
    assert out["patient_id"] == "pat-1001"
    assert out == row


def test_deidentify_dicom_metadata_removes_name_and_generalizes_dob():
    record = {
        "sop_instance_uid": "1.2.3",
        "patient_id": "pat-1001",
        "patient_name": "Rivera^Elena",
        "patient_birth_date": "19820417",
    }
    out = deidentify_dicom_metadata(record)
    assert "patient_name" not in out
    assert "patient_birth_date" not in out
    assert out["patient_birth_year"] == 1982
    assert out["patient_id"] != "pat-1001"


def test_dicom_patient_id_joins_to_patient_mrn_hash(sample_patient_resource):
    # Regression: the ERD documented dicom.patient_id == patient.mrn_hash, but the
    # sample DICOM carried the FHIR resource id, so the hashes never matched.
    # DICOM PatientID conventionally holds the MRN; hashed with the same salted
    # function it must equal the Patient table's mrn_hash.
    patient = deidentify_patient_row(parse_patient(sample_patient_resource))
    dicom = deidentify_dicom_metadata({"sop_instance_uid": "1.2", "patient_id": "MRN-778812"})
    assert dicom["patient_id"] == patient["mrn_hash"]


def test_sample_dicom_records_link_to_sample_patients():
    import json
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "data" / "sample"
    patients = set()
    for f in (root / "fhir").glob("patient_bundle_*.json"):
        for entry in json.loads(f.read_text())["entry"]:
            if entry["resource"]["resourceType"] == "Patient":
                patients.add(deidentify_patient_row(parse_patient(entry["resource"]))["mrn_hash"])
    for rec in json.loads((root / "dicom" / "sample_dicom_metadata.json").read_text()):
        assert deidentify_dicom_metadata(rec)["patient_id"] in patients
