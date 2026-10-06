"""
Real FHIR ingestion pipeline.

Reads FHIR JSON / Bundle files from a source directory (local sample data,
or a landing bucket in production), validates each resource, stamps
ingestion metadata, and writes:

  * valid resources  -> Bronze layer (raw, unmodified resource + metadata)
  * invalid resources -> Quarantine, with the reason for rejection

This replaces the old placeholder `print("Extracting raw FHIR bundles...")`
implementation with something that actually reads and writes data, is unit
testable, and is called by both the Airflow DAG and pytest.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from src.fhir.validator import extract_resources, validate_resource
from src.ingestion.quarantine import quarantine_bucket, write_quarantine_record
from src.quality.checks import QualityMetrics
from src.storage.s3_client import StorageClient
from src.utils.config import get_settings
from src.utils.logging_config import get_logger, log_with_fields

logger = get_logger(__name__)


@dataclass
class IngestionResult:
    batch_id: str
    metrics: QualityMetrics = field(default_factory=QualityMetrics)
    bronze_paths: list[str] = field(default_factory=list)
    quarantine_paths: list[str] = field(default_factory=list)


def new_batch_id() -> str:
    return f"batch_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"


def _ingestion_metadata(batch_id: str, source_file: str) -> dict:
    return {
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "source_file": source_file,
        "batch_id": batch_id,
    }


def ingest_file(path: Path, batch_id: str, storage: StorageClient) -> tuple[list[dict], list[dict]]:
    """Ingest a single FHIR JSON/Bundle file.

    Returns (accepted_records, rejected_records) where each record is the
    dict that was written to Bronze or Quarantine (i.e. already includes
    ingestion metadata / error metadata).
    """
    settings = get_settings()
    accepted: list[dict] = []
    rejected: list[dict] = []

    try:
        raw_text = path.read_text(encoding="utf-8")
        payload = json.loads(raw_text)
    except (OSError, json.JSONDecodeError) as exc:
        logger.error("failed to read/parse source file", extra={"fields": {"source_file": str(path), "error": str(exc)}})
        record = {
            "source_file": str(path),
            "record_id": None,
            "error_type": "FileReadError",
            "error_message": str(exc),
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "batch_id": batch_id,
        }
        storage.write_json(quarantine_bucket(), f"{batch_id}/{path.stem}.error.json", record)
        rejected.append(record)
        return accepted, rejected

    resources = extract_resources(payload)

    for resource in resources:
        validation = validate_resource(resource)
        metadata = _ingestion_metadata(batch_id, str(path))

        if validation.is_valid:
            bronze_record = {
                "resource": resource,
                **metadata,
            }
            key = f"{batch_id}/{validation.resource_type}/{validation.resource_id or uuid.uuid4().hex}.json"
            bucket = settings.bucket_bronze if not settings.is_local else "bronze"
            dest = storage.write_json(bucket, key, bronze_record)
            bronze_record["_bronze_path"] = dest
            accepted.append(bronze_record)
        else:
            error_record, dest = write_quarantine_record(
                storage,
                batch_id=batch_id,
                subdir=validation.resource_type or "unknown",
                record_id=validation.resource_id,
                error_type="ValidationError",
                error_message="; ".join(validation.errors),
                source_file=metadata["source_file"],
                ingestion_timestamp=metadata["ingestion_timestamp"],
                resource_type=validation.resource_type,
                payload=resource,
            )
            error_record["_quarantine_path"] = dest
            rejected.append(error_record)

    return accepted, rejected


def run_ingestion(source_dir: str | Path, batch_id: str | None = None) -> IngestionResult:
    """Ingest all FHIR JSON files in `source_dir`. Returns an IngestionResult
    with metrics and the list of Bronze/Quarantine paths written.
    """
    batch_id = batch_id or new_batch_id()
    storage = StorageClient()
    source_path = Path(source_dir)
    files = sorted(source_path.glob("*.json"))

    if not files:
        logger.warning("no FHIR source files found", extra={"fields": {"source_dir": str(source_path)}})

    result = IngestionResult(batch_id=batch_id)

    for file_path in files:
        accepted, rejected = ingest_file(file_path, batch_id, storage)
        result.bronze_paths.extend(r.get("_bronze_path", "") for r in accepted)
        result.quarantine_paths.extend(r.get("_quarantine_path", "") for r in rejected)
        result.metrics.total_records += len(accepted) + len(rejected)
        result.metrics.valid_records += len(accepted)
        result.metrics.invalid_records += len(rejected)
        result.metrics.rejected_records += len(rejected)

    log_with_fields(
        logger,
        20,  # logging.INFO
        "ingestion batch complete",
        batch_id=batch_id,
        source_dir=str(source_path),
        files_processed=len(files),
        **result.metrics.as_dict(),
    )
    return result


if __name__ == "__main__":
    import sys

    settings = get_settings()
    default_source = Path(settings.local_data_root) / "sample" / "fhir"
    src_dir = sys.argv[1] if len(sys.argv) > 1 else str(default_source)
    outcome = run_ingestion(src_dir)
    print(json.dumps({"batch_id": outcome.batch_id, **outcome.metrics.as_dict()}, indent=2))
