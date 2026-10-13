"""
FHIR R4 structural validation.

This is intentionally NOT a full FHIR profile validator (that would require
the official FHIR StructureDefinitions / a library such as `fhir.resources`
with strict mode). Instead it performs pragmatic checks that are enough to
catch malformed ingestion data in a portfolio pipeline:

  * resourceType is one of the resource types this pipeline supports
  * the required fields for that resource type are present and non-empty
  * a handful of value-level sanity checks (dates parse, codes look sane)

Anything that fails validation is routed to quarantine by the ingestion
module -- it is never silently dropped.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

SUPPORTED_RESOURCE_TYPES = {
    "Patient",
    "Observation",
    "Encounter",
    "Condition",
    "MedicationRequest",
}

# Minimal required-field contracts per resource type. These mirror the
# fields our parsers in src/fhir/parser.py actually rely on -- if a required
# field is missing here, parsing would silently produce nulls, so we reject
# up front instead.
REQUIRED_FIELDS = {
    "Patient": ["id"],
    "Observation": ["id", "status", "code"],
    "Encounter": ["id", "status"],
    "Condition": ["id", "subject"],
    "MedicationRequest": ["id", "status", "subject"],
}


@dataclass
class ValidationResult:
    is_valid: bool
    resource_type: str | None
    resource_id: str | None
    errors: list[str] = field(default_factory=list)

    def add_error(self, msg: str) -> None:
        self.errors.append(msg)
        self.is_valid = False


def _try_parse_date(value: str) -> bool:
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m", "%Y"):
        try:
            datetime.strptime(value, fmt)
            return True
        except ValueError:
            continue
    return False


def validate_resource(resource: dict) -> ValidationResult:
    """Validate a single (already-parsed) FHIR JSON resource dict."""
    resource_type = resource.get("resourceType")
    resource_id = resource.get("id")
    result = ValidationResult(is_valid=True, resource_type=resource_type, resource_id=resource_id)

    if not isinstance(resource, dict):
        result.add_error("resource is not a JSON object")
        return result

    if resource_type not in SUPPORTED_RESOURCE_TYPES:
        result.add_error(
            f"unsupported resourceType '{resource_type}'; supported types are "
            f"{sorted(SUPPORTED_RESOURCE_TYPES)}"
        )
        return result

    for required in REQUIRED_FIELDS[resource_type]:
        value = resource.get(required)
        if value in (None, "", [], {}):
            result.add_error(f"missing required field '{required}' for {resource_type}")

    # Resource-specific sanity checks
    if resource_type == "Patient":
        birth_date = resource.get("birthDate")
        if birth_date and not _try_parse_date(birth_date):
            result.add_error(f"invalid birthDate '{birth_date}'")

    if resource_type == "Observation":
        status = resource.get("status")
        valid_statuses = {
            "registered", "preliminary", "final", "amended",
            "corrected", "cancelled", "entered-in-error", "unknown",
        }
        if status not in valid_statuses:
            result.add_error(f"invalid Observation.status '{status}'")
        value_quantity = resource.get("valueQuantity")
        if value_quantity is not None and "value" not in value_quantity:
            result.add_error("valueQuantity present but missing numeric 'value'")

    if resource_type == "Encounter":
        status = resource.get("status")
        valid_statuses = {
            "planned", "arrived", "triaged", "in-progress", "onleave",
            "finished", "cancelled", "entered-in-error", "unknown",
        }
        if status not in valid_statuses:
            result.add_error(f"invalid Encounter.status '{status}'")

    if resource_type == "MedicationRequest":
        status = resource.get("status")
        valid_statuses = {
            "active", "on-hold", "cancelled", "completed",
            "entered-in-error", "stopped", "draft", "unknown",
        }
        if status not in valid_statuses:
            result.add_error(f"invalid MedicationRequest.status '{status}'")

    return result


def extract_resources(payload: dict) -> list[dict]:
    """Given a parsed JSON document, return the list of FHIR resources.

    Supports both a single resource document and a FHIR `Bundle` containing
    multiple `entry[].resource` objects.
    """
    if payload.get("resourceType") == "Bundle":
        entries = payload.get("entry", [])
        return [e["resource"] for e in entries if isinstance(e, dict) and "resource" in e]
    return [payload]
