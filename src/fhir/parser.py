"""
FHIR R4 -> flat tabular row parsers.

Each `parse_*` function takes a single FHIR resource dict (already validated
by `src.fhir.validator`) and returns a flat dict whose keys line up with the
corresponding Iceberg table schema under `sql/silver/`. Nested FHIR
structures (CodeableConcept, Reference, Period, ...) are flattened here so
the rest of the pipeline never has to deal with nested JSON again.

These functions are pure and side-effect free, which is what makes them
independently unit-testable (see tests/test_fhir_parser.py).
"""
from __future__ import annotations

from typing import Any


def _reference_id(reference_obj: dict | None) -> str | None:
    """Extract the bare id from a FHIR Reference, e.g. 'Patient/123' -> '123'."""
    if not reference_obj:
        return None
    ref = reference_obj.get("reference")
    if not ref:
        return None
    return ref.split("/")[-1]


def _codeable_concept(concept: dict | None) -> tuple[str | None, str | None, str | None]:
    """Return (code, system, display) for the first coding in a CodeableConcept."""
    if not concept:
        return None, None, None
    codings = concept.get("coding") or []
    code = system = display = None
    if codings:
        first = codings[0]
        code = first.get("code")
        system = first.get("system")
        display = first.get("display")
    display = display or concept.get("text")
    return code, system, display


def _first_or_none(items: list | None) -> Any:
    if not items:
        return None
    return items[0]


def parse_patient(resource: dict) -> dict:
    names = resource.get("name") or []
    official_name = next((n for n in names if n.get("use") == "official"), _first_or_none(names)) or {}
    telecoms = resource.get("telecom") or []
    phone = next((t.get("value") for t in telecoms if t.get("system") == "phone"), None)
    email = next((t.get("value") for t in telecoms if t.get("system") == "email"), None)
    addresses = resource.get("address") or []
    address = _first_or_none(addresses) or {}

    identifiers = resource.get("identifier") or []

    def _identifier_type_code(identifier: dict) -> str:
        coding = (identifier.get("type") or {}).get("coding") or []
        return (coding[0].get("code") if coding else "") or ""

    mrn = next((i.get("value") for i in identifiers if _identifier_type_code(i) == "MR"), None)
    if mrn is None:
        # Fall back to the first identifier if none is explicitly typed as MRN
        mrn = identifiers[0].get("value") if identifiers else None
    ssn = next(
        (i.get("value") for i in identifiers if "ssn" in (i.get("system") or "").lower()),
        None,
    )

    return {
        "id": resource.get("id"),
        "active": resource.get("active"),
        "gender": resource.get("gender"),
        "birth_date": resource.get("birthDate"),
        "deceased_boolean": resource.get("deceasedBoolean"),
        "deceased_datetime": resource.get("deceasedDateTime"),
        "mrn": mrn,
        "ssn": ssn,
        "first_name": _first_or_none(official_name.get("given")) if official_name else None,
        "last_name": official_name.get("family") if official_name else None,
        "phone": phone,
        "email": email,
        "address_line": ", ".join(address.get("line", [])) if address.get("line") else None,
        "city": address.get("city"),
        "state": address.get("state"),
        "postal_code": address.get("postalCode"),
        "country": address.get("country"),
        "version_id": (resource.get("meta") or {}).get("versionId"),
        "last_updated": (resource.get("meta") or {}).get("lastUpdated"),
    }


def parse_observation(resource: dict) -> dict:
    category = _first_or_none(resource.get("category") or [])
    category_code, _, category_display = _codeable_concept(category)
    code_code, code_system, code_display = _codeable_concept(resource.get("code"))
    value_quantity = resource.get("valueQuantity") or {}
    interpretation = _first_or_none(resource.get("interpretation") or [])
    interpretation_code, _, _ = _codeable_concept(interpretation)

    return {
        "id": resource.get("id"),
        "status": resource.get("status"),
        "category_code": category_code,
        "category_display": category_display,
        # Keep the code AND its coding system. Collapsing to a LOINC-only column
        # (nulling anything that isn't LOINC) made non-LOINC observations
        # disappear from every Gold aggregate keyed on that column; LOINC
        # queries filter on code_system = 'http://loinc.org' instead.
        "code": code_code,
        "code_system": code_system,
        "code_display": code_display,
        "patient_id": _reference_id(resource.get("subject")),
        "encounter_id": _reference_id(resource.get("encounter")),
        "effective_datetime": resource.get("effectiveDateTime"),
        "value_quantity_value": value_quantity.get("value"),
        "value_quantity_unit": value_quantity.get("unit"),
        "value_string": resource.get("valueString"),
        "interpretation_code": interpretation_code,
    }


def parse_encounter(resource: dict) -> dict:
    class_obj = resource.get("class") or {}
    type_code, _, type_display = _codeable_concept(_first_or_none(resource.get("type") or []))
    period = resource.get("period") or {}
    service_provider = _reference_id(resource.get("serviceProvider"))

    return {
        "id": resource.get("id"),
        "status": resource.get("status"),
        "class_code": class_obj.get("code"),
        "type_code": type_code,
        "type_display": type_display,
        "patient_id": _reference_id(resource.get("subject")),
        "period_start": period.get("start"),
        "period_end": period.get("end"),
        "service_provider": service_provider,
    }


def parse_condition(resource: dict) -> dict:
    clinical_status, _, _ = _codeable_concept(resource.get("clinicalStatus"))
    verification_status, _, _ = _codeable_concept(resource.get("verificationStatus"))
    code_code, _, code_display = _codeable_concept(resource.get("code"))

    return {
        "id": resource.get("id"),
        "clinical_status": clinical_status,
        "verification_status": verification_status,
        "code": code_code,
        "code_display": code_display,
        "patient_id": _reference_id(resource.get("subject")),
        "encounter_id": _reference_id(resource.get("encounter")),
        "onset_datetime": resource.get("onsetDateTime"),
        "recorded_date": resource.get("recordedDate"),
    }


def parse_medication_request(resource: dict) -> dict:
    medication_code, _, medication_display = _codeable_concept(resource.get("medicationCodeableConcept"))
    dosage = _first_or_none(resource.get("dosageInstruction") or []) or {}

    return {
        "id": resource.get("id"),
        "status": resource.get("status"),
        "intent": resource.get("intent"),
        "medication_code": medication_code,
        "medication_display": medication_display,
        "patient_id": _reference_id(resource.get("subject")),
        "encounter_id": _reference_id(resource.get("encounter")),
        "authored_on": resource.get("authoredOn"),
        "dosage_text": dosage.get("text"),
    }


PARSERS = {
    "Patient": parse_patient,
    "Observation": parse_observation,
    "Encounter": parse_encounter,
    "Condition": parse_condition,
    "MedicationRequest": parse_medication_request,
}


def parse_resource(resource: dict) -> dict:
    """Dispatch to the correct parser based on resourceType.

    Raises KeyError if the resourceType is not supported -- callers should
    validate with `src.fhir.validator.validate_resource` first.
    """
    resource_type = resource["resourceType"]
    parser = PARSERS[resource_type]
    row = parser(resource)
    row["resource_type"] = resource_type
    return row
