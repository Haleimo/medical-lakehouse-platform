"""
Single place that writes quarantine records, so every rejection path (FHIR
validation, DICOM validation, Spark referential-integrity failures) produces
the same metadata shape:

    source_file, record_id, error_type, error_message, ingestion_timestamp,
    batch_id   (+ resource_type and the offending payload)

Nothing is silently dropped: a rejected record is either written here or the
caller raises.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from src.storage.s3_client import StorageClient
from src.utils.config import get_settings


def quarantine_bucket() -> str:
    """Quarantine bucket name (or the local layer directory in RUN_MODE=local)."""
    settings = get_settings()
    return "quarantine" if settings.is_local else settings.bucket_quarantine


def write_quarantine_record(
    storage: StorageClient,
    *,
    batch_id: str,
    subdir: str,
    record_id: str | None,
    error_type: str,
    error_message: str,
    source_file: str | None = None,
    ingestion_timestamp: str | None = None,
    resource_type: str | None = None,
    payload: dict | None = None,
) -> tuple[dict, str]:
    """Persist one rejected record. Returns (record_dict, path_or_uri_written)."""
    record = {
        "resource_type": resource_type,
        "record_id": record_id,
        "error_type": error_type,
        "error_message": error_message,
        "source_file": source_file,
        "ingestion_timestamp": ingestion_timestamp or datetime.now(timezone.utc).isoformat(),
        "batch_id": batch_id,
        "payload": payload,
    }
    key = f"{batch_id}/{subdir}/{record_id or uuid.uuid4().hex}.error.json"
    path = storage.write_json(quarantine_bucket(), key, record)
    return record, path
