"""
Thin storage abstraction over an S3-compatible endpoint (Garage in
production, MinIO/AWS S3 would also work unchanged).

For local development (`RUN_MODE=local`, the default), writes go to the
local filesystem under `data/<layer>/...` instead of a real object store, so
the whole pipeline is runnable on a laptop with no infrastructure at all.
This is a deliberate, documented trade-off for a portfolio project -- see
README "Known limitations".

boto3 is only imported when actually needed (RUN_MODE != local) so the local
dev / test path has no hard dependency on network access or S3 credentials.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.utils.config import get_settings
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class StorageClient:
    """Writes/reads JSON and bytes objects to either S3 or local disk."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self._boto_client = None
        if not self.settings.is_local:
            self._boto_client = self._build_boto_client()

    def _build_boto_client(self):
        import boto3  # local import: only required in non-local run modes

        if not self.settings.s3_access_key or not self.settings.s3_secret_key:
            raise RuntimeError(
                "S3_ACCESS_KEY / S3_SECRET_KEY must be set (via environment variable or "
                "Kubernetes Secret) when RUN_MODE is not 'local'."
            )
        return boto3.client(
            "s3",
            endpoint_url=self.settings.s3_endpoint,
            aws_access_key_id=self.settings.s3_access_key,
            aws_secret_access_key=self.settings.s3_secret_key,
            region_name=self.settings.s3_region,
        )

    def write_json(self, bucket_or_layer: str, key: str, payload: dict | list) -> str:
        """Write a JSON payload. Returns the full path/URI it was written to."""
        body = json.dumps(payload, default=str, indent=2).encode("utf-8")
        if self.settings.is_local:
            dest = self.settings.local_path(bucket_or_layer) / key
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(body)
            logger.debug("wrote local object", extra={"fields": {"path": str(dest)}})
            return str(dest)
        self._boto_client.put_object(Bucket=bucket_or_layer, Key=key, Body=body)
        uri = f"s3://{bucket_or_layer}/{key}"
        logger.debug("wrote s3 object", extra={"fields": {"uri": uri}})
        return uri

    def read_json(self, bucket_or_layer: str, key: str) -> Any:
        if self.settings.is_local:
            path = self.settings.local_path(bucket_or_layer) / key
            return json.loads(path.read_text())
        obj = self._boto_client.get_object(Bucket=bucket_or_layer, Key=key)
        return json.loads(obj["Body"].read())

    def list_local_sample_files(self, directory: Path, pattern: str = "*.json") -> list[Path]:
        return sorted(Path(directory).glob(pattern))
