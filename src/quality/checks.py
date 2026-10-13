"""
Data quality checks used across the pipeline (post-ingestion and post-Silver).

Design goals:
  * Every check returns structured results (never just True/False) so callers
    can log *why* something failed and route bad records to quarantine.
  * Checks are pure functions over lists of dicts -- usable from plain Python,
    pytest, and from PySpark (by calling them on `.collect()`-ed batches or by
    wrapping them in a Pandas UDF) without duplicating logic.
  * `enforce_quality_gate` raises `DataQualityError` when a configurable
    threshold is breached, which is what makes the Airflow task actually
    fail the DAG run instead of silently continuing on bad data.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime

from src.deidentification.deidentify import FORBIDDEN_SILVER_FIELDS
from src.utils.config import get_settings


class DataQualityError(Exception):
    """Raised when a data quality gate fails and the pipeline must stop."""


@dataclass(frozen=True)
class RowIssue:
    """A single, structured data-quality finding.

    Callers that need to act on a finding (drop/quarantine the row) use
    `row_index` directly instead of parsing it back out of a message string --
    string parsing silently breaks when the identifier is missing or contains
    unexpected characters.
    """

    row_index: int
    record_id: str | None
    field: str | None
    message: str


@dataclass
class QualityMetrics:
    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    duplicate_records: int = 0
    rejected_records: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def invalid_ratio(self) -> float:
        if self.total_records == 0:
            return 0.0
        return self.invalid_records / self.total_records

    def as_dict(self) -> dict:
        return {
            "total_records": self.total_records,
            "valid_records": self.valid_records,
            "invalid_records": self.invalid_records,
            "duplicate_records": self.duplicate_records,
            "rejected_records": self.rejected_records,
            "invalid_ratio": round(self.invalid_ratio, 4),
        }


def find_missing_required(rows: list[dict], required_fields: list[str], id_field: str = "id") -> list[RowIssue]:
    """Structured version of `check_required_not_null`: one RowIssue per missing field.

    `id_field` names the column used to identify the offending row in the
    finding (default "id"); pass the natural key for resources that have no
    "id" (e.g. "sop_instance_uid" for DICOM metadata).
    """
    issues = []
    for i, row in enumerate(rows):
        for f in required_fields:
            if row.get(f) in (None, ""):
                rid = row.get(id_field)
                issues.append(RowIssue(i, rid, f, f"row {i} (id={rid}): required field '{f}' is null/empty"))
    return issues


def check_required_not_null(rows: list[dict], required_fields: list[str], id_field: str = "id") -> list[str]:
    """Return a list of error strings for rows missing required fields."""
    return [issue.message for issue in find_missing_required(rows, required_fields, id_field)]


def find_duplicate_ids(rows: list[dict], id_field: str = "id") -> list[str]:
    """Return the list of ids that occur more than once."""
    counts = Counter(row.get(id_field) for row in rows if row.get(id_field) is not None)
    return [rid for rid, count in counts.items() if count > 1]


def deduplicate(rows: list[dict], id_field: str = "id") -> list[dict]:
    """Keep the first occurrence of each id, in input order."""
    seen: set = set()
    result = []
    for row in rows:
        rid = row.get(id_field)
        if rid is not None and rid in seen:
            continue
        if rid is not None:
            seen.add(rid)
        result.append(row)
    return result


def check_valid_dates(rows: list[dict], date_fields: list[str]) -> list[str]:
    errors = []
    date_formats = ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m", "%Y")
    for i, row in enumerate(rows):
        for f in date_fields:
            value = row.get(f)
            if value in (None, ""):
                continue
            parsed_ok = False
            for fmt in date_formats:
                try:
                    datetime.strptime(str(value).replace("Z", "+0000"), fmt)
                    parsed_ok = True
                    break
                except ValueError:
                    continue
            if not parsed_ok:
                errors.append(f"row {i} (id={row.get('id')}): invalid date '{value}' in field '{f}'")
    return errors


def find_referential_violations(
    child_rows: list[dict], parent_ids: set[str], fk_field: str = "patient_id", id_field: str = "id"
) -> list[RowIssue]:
    """Structured version of `check_referential_integrity`.

    A null foreign key is not a violation (the reference is optional in FHIR);
    a non-null key with no matching parent is.
    """
    issues = []
    for i, row in enumerate(child_rows):
        fk = row.get(fk_field)
        if fk is not None and fk not in parent_ids:
            rid = row.get(id_field)
            issues.append(
                RowIssue(i, rid, fk_field, f"row {i} (id={rid}): {fk_field}='{fk}' has no matching parent record")
            )
    return issues


def check_referential_integrity(
    child_rows: list[dict], parent_ids: set[str], fk_field: str = "patient_id", id_field: str = "id"
) -> list[str]:
    """Verify every non-null foreign key in child_rows exists in parent_ids."""
    return [i.message for i in find_referential_violations(child_rows, parent_ids, fk_field, id_field)]


def check_observation_values(rows: list[dict]) -> list[str]:
    """Observation-specific sanity check: a final/amended observation should
    carry either a numeric value or a string value."""
    errors = []
    for i, row in enumerate(rows):
        if row.get("status") in ("final", "amended", "corrected"):
            if row.get("value_quantity_value") is None and not row.get("value_string"):
                errors.append(f"row {i} (id={row.get('id')}): finalized Observation has no value")
    return errors


def check_no_forbidden_fields(rows: list[dict]) -> list[str]:
    """De-identified tables must never contain raw PHI fields."""
    errors = []
    for i, row in enumerate(rows):
        present = FORBIDDEN_SILVER_FIELDS.intersection(row.keys())
        if present:
            errors.append(f"row {i} (id={row.get('id')}): forbidden PHI field(s) present: {sorted(present)}")
    return errors


def enforce_quality_gate(metrics: QualityMetrics, max_invalid_ratio: float | None = None) -> None:
    """Raise DataQualityError if the invalid ratio exceeds the configured threshold."""
    threshold = max_invalid_ratio if max_invalid_ratio is not None else get_settings().max_invalid_ratio
    if metrics.total_records > 0 and metrics.invalid_ratio > threshold:
        raise DataQualityError(
            f"Data quality gate failed: invalid_ratio={metrics.invalid_ratio:.2%} "
            f"exceeds threshold={threshold:.2%} "
            f"({metrics.invalid_records}/{metrics.total_records} records invalid)"
        )
