"""
Post-transform data-quality checks over the Silver layer.

This used to live inside the Airflow DAG file, where it could not be unit
tested without a full Airflow install -- and a bug there (it globbed for
``*.json`` while Silver is written as Parquet, so every check ran on an empty
list and passed vacuously) went unnoticed while the test suite stayed green.
It now lives here, takes a directory of Silver Parquet tables, and is covered
by tests that assert the checks actually *find* problems (see
tests/test_silver_checks.py), not merely that they run.

Checks per table (where the columns exist):
  * required key not null
  * duplicate keys
  * no forbidden (raw PHI) columns
  * valid dates
  * foreign keys: patient_id -> patient.id, encounter_id -> encounter.id,
    and for DICOM metadata patient_id -> patient.mrn_hash
  * finalized Observations carry a value
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.quality.checks import (
    QualityMetrics,
    check_no_forbidden_fields,
    check_observation_values,
    check_valid_dates,
    find_duplicate_ids,
    find_missing_required,
    find_referential_violations,
)
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# table -> (natural key column, date columns to validate)
SILVER_TABLES: dict[str, tuple[str, list[str]]] = {
    "patient": ("id", ["deceased_datetime", "last_updated"]),
    "encounter": ("id", ["period_start", "period_end"]),
    "observation": ("id", ["effective_datetime"]),
    "condition": ("id", ["onset_datetime", "recorded_date"]),
    "medication_request": ("id", ["authored_on"]),
    "dicom_metadata": ("sop_instance_uid", ["study_date"]),
}


def read_silver_table(silver_dir: Path, table: str) -> list[dict]:
    """Read one Silver Parquet table as a list of dicts, with NaN/NaT -> None.

    Parquet nulls come back from pandas as NaN for numeric columns, and
    ``NaN in (None, "")`` is False -- without this normalization a missing
    numeric value would look present to every null check.
    """
    path = Path(silver_dir) / table
    if not path.exists():
        return []
    df = pd.read_parquet(path)
    df = df.astype(object).where(df.notna(), None)
    return df.to_dict("records")


def run_silver_quality_checks(silver_dir: Path) -> QualityMetrics:
    """Run every Silver check and return metrics (does not raise).

    Callers decide what to do with the result -- the DAG passes it to
    `enforce_quality_gate`. `invalid_records` counts distinct offending rows,
    not individual findings.
    """
    tables = {name: read_silver_table(silver_dir, name) for name in SILVER_TABLES}
    patient_ids = {r["id"] for r in tables["patient"] if r.get("id")}
    patient_mrn_hashes = {r["mrn_hash"] for r in tables["patient"] if r.get("mrn_hash")}
    encounter_ids = {r["id"] for r in tables["encounter"] if r.get("id")}

    metrics = QualityMetrics()
    invalid_rows: set[tuple[str, int]] = set()
    errors: list[str] = []

    def record(table: str, issues) -> None:
        for issue in issues:
            invalid_rows.add((table, issue.row_index))
            errors.append(f"{table}: {issue.message}")

    for table, rows in tables.items():
        if not rows:
            continue
        key, date_fields = SILVER_TABLES[table]
        metrics.total_records += len(rows)

        record(table, find_missing_required(rows, [key], id_field=key))

        dupes = find_duplicate_ids(rows, id_field=key)
        metrics.duplicate_records += len(dupes)
        errors += [f"{table}: duplicate {key} '{d}'" for d in dupes]
        seen: dict[str, int] = {}
        for i, row in enumerate(rows):
            k = row.get(key)
            if k in dupes:
                if k in seen:
                    invalid_rows.add((table, i))
                seen.setdefault(k, i)

        for msg in check_no_forbidden_fields(rows[:1]):  # column-level: one row is enough
            invalid_rows.update((table, i) for i in range(len(rows)))
            errors.append(f"{table}: {msg}")

        for msg in check_valid_dates(rows, date_fields):
            errors.append(f"{table}: {msg}")
            invalid_rows.add((table, int(msg.split()[1])))

        if table == "observation":
            for msg in check_observation_values(rows):
                errors.append(f"{table}: {msg}")
                invalid_rows.add((table, int(msg.split()[1])))

        if table in ("encounter", "observation", "condition", "medication_request"):
            record(table, find_referential_violations(rows, patient_ids, "patient_id", key))
        if table in ("observation", "condition", "medication_request"):
            record(table, find_referential_violations(rows, encounter_ids, "encounter_id", key))
        if table == "dicom_metadata":
            record(table, find_referential_violations(rows, patient_mrn_hashes, "patient_id", key))

    metrics.invalid_records = len(invalid_rows)
    metrics.valid_records = metrics.total_records - metrics.invalid_records
    metrics.errors = errors
    return metrics
