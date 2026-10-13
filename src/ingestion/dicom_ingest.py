"""
DICOM metadata ingestion.

Scope note (see README "DICOM"): this project parses and de-identifies DICOM
**metadata** (patient/study/series attributes). It does NOT process DICOM
pixel data / medical images.

In production the metadata dicts would be produced by `pydicom` from real
``.dcm`` headers; here `run_dicom_ingestion` reads already-extracted metadata
JSON (``data/sample/dicom/sample_dicom_metadata.json``) so the rest of the
pipeline can run end to end without imaging files. Dates are expected as ISO
``YYYY-MM-DD`` (a real extractor would convert the DICOM ``DA`` format).

Layering mirrors FHIR:
  * Bronze     -- the RAW record (still containing patient_name and
                  patient_birth_date) + ingestion metadata. Access-restricted.
  * Quarantine -- records failing validation, with the reason.
  * Silver     -- de-identified rows, written by spark/jobs/bronze_to_silver.py
                  via `src.deidentification.deidentify_dicom_metadata`.

`patient_id` in DICOM (tag 0010,0020) conventionally carries the MRN. It is
hashed with the same salted SHA-256 as `patient.mrn_hash`, so imaging metadata
joins to the de-identified Patient table on `patient_id = mrn_hash`, and the
Spark job enforces that link (rows that don't match are quarantined).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.ingestion.fhir_ingest import IngestionResult, new_batch_id
from src.ingestion.quarantine import write_quarantine_record
from src.quality.checks import check_valid_dates, find_missing_required
from src.storage.s3_client import StorageClient
from src.utils.config import get_settings
from src.utils.logging_config import get_logger, log_with_fields

logger = get_logger(__name__)

REQUIRED_FIELDS = ["sop_instance_uid", "study_instance_uid", "modality", "patient_id"]
KEY_FIELD = "sop_instance_uid"
BRONZE_SUBDIR = "DicomMetadata"


def validate_dicom_record(record: dict) -> list[str]:
    """Return validation error messages for one DICOM metadata record ([] = valid)."""
    if not isinstance(record, dict):
        return ["record is not a JSON object"]
    errors = [i.message for i in find_missing_required([record], REQUIRED_FIELDS, id_field=KEY_FIELD)]
    errors += check_valid_dates([record], ["study_date"])
    return errors


def ingest_dicom_records(
    records: list[dict], batch_id: str, source_file: str, storage: StorageClient
) -> tuple[list[dict], list[dict]]:
    """Validate records; valid -> Bronze (raw + metadata), invalid -> quarantine."""
    settings = get_settings()
    bronze_bucket = "bronze" if settings.is_local else settings.bucket_bronze
    accepted: list[dict] = []
    rejected: list[dict] = []

    for record in records:
        ingestion_ts = datetime.now(timezone.utc).isoformat()
        errors = validate_dicom_record(record)
        record_id = record.get(KEY_FIELD) if isinstance(record, dict) else None

        if errors:
            error_record, dest = write_quarantine_record(
                storage,
                batch_id=batch_id,
                subdir=BRONZE_SUBDIR,
                record_id=record_id,
                error_type="ValidationError",
                error_message="; ".join(errors),
                source_file=source_file,
                ingestion_timestamp=ingestion_ts,
                resource_type="DicomMetadata",
                payload=record if isinstance(record, dict) else {"raw": str(record)},
            )
            error_record["_quarantine_path"] = dest
            rejected.append(error_record)
            continue

        bronze_record = {
            "resource": record,
            "ingestion_timestamp": ingestion_ts,
            "source_file": source_file,
            "batch_id": batch_id,
        }
        dest = storage.write_json(bronze_bucket, f"{batch_id}/{BRONZE_SUBDIR}/{record_id}.json", bronze_record)
        bronze_record["_bronze_path"] = dest
        accepted.append(bronze_record)

    return accepted, rejected


def run_dicom_ingestion(source: str | Path, batch_id: str | None = None) -> IngestionResult:
    """Ingest one DICOM-metadata JSON file, or every ``*.json`` in a directory."""
    batch_id = batch_id or new_batch_id()
    source_path = Path(source)
    files = sorted(source_path.glob("*.json")) if source_path.is_dir() else [source_path]
    storage = StorageClient()
    result = IngestionResult(batch_id=batch_id)

    for file_path in files:
        try:
            payload = json.loads(file_path.read_text(encoding="utf-8"))
            records = payload if isinstance(payload, list) else [payload]
        except (OSError, json.JSONDecodeError) as exc:
            logger.error("failed to read DICOM metadata file", extra={"fields": {"source_file": str(file_path), "error": str(exc)}})
            _, dest = write_quarantine_record(
                storage,
                batch_id=batch_id,
                subdir=BRONZE_SUBDIR,
                record_id=file_path.stem,
                error_type="FileReadError",
                error_message=str(exc),
                source_file=str(file_path),
                resource_type="DicomMetadata",
            )
            result.quarantine_paths.append(dest)
            result.metrics.total_records += 1
            result.metrics.invalid_records += 1
            result.metrics.rejected_records += 1
            continue

        accepted, rejected = ingest_dicom_records(records, batch_id, str(file_path), storage)
        result.bronze_paths.extend(r["_bronze_path"] for r in accepted)
        result.quarantine_paths.extend(r["_quarantine_path"] for r in rejected)
        result.metrics.total_records += len(accepted) + len(rejected)
        result.metrics.valid_records += len(accepted)
        result.metrics.invalid_records += len(rejected)
        result.metrics.rejected_records += len(rejected)

    log_with_fields(
        logger, 20, "dicom metadata ingestion complete", batch_id=batch_id, files_processed=len(files), **result.metrics.as_dict()
    )
    return result
