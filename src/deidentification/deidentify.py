"""
De-identification of parsed FHIR rows.

This module performs the actual PHI transformation logic (not print
statements). It operates on the flat dict rows produced by
`src.fhir.parser`, *before* they are written to the Silver layer.

Transformation policy (documented, also see README "De-identification" section):

| Field                | Sensitive?      | Transformation                                   |
|----------------------|-----------------|---------------------------------------------------|
| mrn                  | direct identifier | deterministic SHA-256 hash (salted)              |
| ssn                  | direct identifier | removed (set to null)                            |
| first_name/last_name | direct identifier | removed (set to null)                            |
| phone                | direct identifier | removed (set to null)                            |
| email                | direct identifier | removed (set to null)                            |
| address_line, city   | direct identifier | removed (set to null)                            |
| postal_code          | quasi-identifier  | generalized to 3-digit prefix (HIPAA Safe Harbor)|
| birth_date           | quasi-identifier  | generalized to birth_year + age_band             |
| state, country       | low-risk          | kept                                              |

The salt used for hashing is read from configuration
(`DEID_SALT` env var / Kubernetes Secret) -- never hardcoded.
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime

from src.utils.config import get_settings

# Fields that are fully removed (nulled out) from Patient rows once de-identified.
REMOVED_FIELDS = ["ssn", "first_name", "last_name", "phone", "email", "address_line", "city"]

# Fields that must NEVER appear in Silver/Gold tables. Used by data-quality
# checks (src/quality/checks.py) to assert de-identification actually ran.
FORBIDDEN_SILVER_FIELDS = set(REMOVED_FIELDS) | {"mrn_raw", "birth_date"}


def hash_identifier(value: str | None, salt: str | None = None) -> str | None:
    """Deterministic (same input -> same output) salted SHA-256 hash.

    Deterministic hashing is used (rather than a random token) so the same
    patient's MRN always maps to the same pseudonymous id, which preserves
    the ability to join/aggregate data across resources without exposing the
    original MRN.
    """
    if value is None or value == "":
        return None
    settings = get_settings()
    effective_salt = salt if salt is not None else settings.deid_salt
    digest = hashlib.sha256(f"{effective_salt}:{value}".encode("utf-8")).hexdigest()
    return digest


def generalize_postal_code(postal_code: str | None) -> str | None:
    """Generalize a ZIP/postal code to a 3-character prefix (HIPAA Safe Harbor)."""
    if not postal_code:
        return None
    cleaned = postal_code.strip()
    return cleaned[:3] if len(cleaned) >= 3 else cleaned


def _parse_birth_date(birth_date: str) -> date | None:
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            parsed = datetime.strptime(birth_date, fmt)
            return parsed.date()
        except ValueError:
            continue
    return None


def generalize_birth_date(birth_date: str | None, reference_date: date | None = None) -> dict:
    """Convert an exact date of birth into a birth year + 10-year age band.

    Returns {"birth_year": int|None, "age_band": str|None}. The exact
    birth_date is never retained in the de-identified row.
    """
    if not birth_date:
        return {"birth_year": None, "age_band": None}
    parsed = _parse_birth_date(birth_date)
    if parsed is None:
        return {"birth_year": None, "age_band": None}
    ref = reference_date or date.today()
    age = ref.year - parsed.year - ((ref.month, ref.day) < (parsed.month, parsed.day))
    band_start = (age // 10) * 10
    # HIPAA Safe Harbor treats ages 90+ as a single band to avoid re-identification
    # of very elderly (rare) patients.
    age_band = "90+" if age >= 90 else f"{band_start}-{band_start + 9}"
    return {"birth_year": parsed.year, "age_band": age_band}


def deidentify_patient_row(row: dict) -> dict:
    """Return a new dict with PHI removed/masked/generalized for a Patient row."""
    out = dict(row)
    out["mrn_hash"] = hash_identifier(row.get("mrn"))
    out.pop("mrn", None)

    for field_name in REMOVED_FIELDS:
        out.pop(field_name, None)

    out.update(generalize_birth_date(row.get("birth_date")))
    out.pop("birth_date", None)

    out["postal_code_prefix"] = generalize_postal_code(row.get("postal_code"))
    out.pop("postal_code", None)

    return out


def deidentify_reference_ids(row: dict, salt: str | None = None) -> dict:
    """For non-Patient resources, pass the row through unchanged.

    Note: `patient_id` here is the internal FHIR *resource* id (e.g.
    "pat-1001"), not a Medical Record Number, Social Security Number, or any
    other Safe-Harbor identifier -- it is an opaque key assigned by the
    source system, not PHI on its own. It is deliberately left untouched
    (rather than hashed) so it keeps matching `Patient.id` in the
    de-identified Patient table (see `deidentify_patient_row`, which does
    NOT hash `id` -- only the real identifier, `mrn`). Hashing one side of a
    join key and not the other would silently break every foreign-key
    relationship in the Silver layer, which is exactly the kind of bug the
    referential-integrity data-quality check in `src.quality.checks` is
    designed to catch.
    """
    return dict(row)


def deidentify_row(resource_type: str, row: dict) -> dict:
    """Dispatch de-identification based on resource type."""
    if resource_type == "Patient":
        return deidentify_patient_row(row)
    return deidentify_reference_ids(row, salt=None)


# --- DICOM metadata de-identification (see src/ingestion/dicom_ingest.py) ---
# Scope note: this project processes DICOM *metadata* only, not pixel data.
DICOM_REMOVED_FIELDS = ["patient_name"]


def deidentify_dicom_metadata(record: dict) -> dict:
    """Remove/hash/generalize the PHI-bearing fields of a DICOM metadata record.

    - patient_name: removed
    - patient_id: DICOM PatientID (0010,0020) conventionally carries the MRN,
      so it is hashed with `hash_identifier` -- exactly the function that
      produces `patient.mrn_hash`. Imaging metadata therefore joins to the
      de-identified Patient table on `dicom_metadata.patient_id = patient.mrn_hash`
      (enforced by spark/jobs/bronze_to_silver.py). Note this link only holds
      when the DICOM PatientID really is the MRN; a site that stores something
      else there needs a different mapping.
    - patient_birth_date (DICOM DA format YYYYMMDD): generalized to birth year only.
    """
    out = dict(record)
    for f in DICOM_REMOVED_FIELDS:
        out.pop(f, None)

    if out.get("patient_id"):
        out["patient_id"] = hash_identifier(out["patient_id"])

    dob = out.pop("patient_birth_date", None)
    if dob and len(dob) >= 4 and dob[:4].isdigit():
        out["patient_birth_year"] = int(dob[:4])
    else:
        out["patient_birth_year"] = None

    return out
