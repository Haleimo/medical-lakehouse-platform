"""
Central configuration loader.

All configuration is read from environment variables so that the same code
runs unchanged in local development (via `.env`), Docker Compose, and
Kubernetes (via ConfigMaps / Secrets). Nothing here is hardcoded to a
specific environment, and no secret has a real default value -- defaults are
either empty or clearly-fake local-dev placeholders.

Usage:
    from src.utils.config import get_settings
    settings = get_settings()
    settings.s3_endpoint
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool) -> bool:
    val = os.environ.get(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    # --- Object storage (Garage / any S3-compatible endpoint) ---
    s3_endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", "http://localhost:3900"))
    s3_access_key: str = field(default_factory=lambda: _env("S3_ACCESS_KEY", ""))
    s3_secret_key: str = field(default_factory=lambda: _env("S3_SECRET_KEY", ""))
    s3_region: str = field(default_factory=lambda: _env("S3_REGION", "garage"))
    bucket_raw: str = field(default_factory=lambda: _env("BUCKET_RAW", "medical-raw"))
    bucket_bronze: str = field(default_factory=lambda: _env("BUCKET_BRONZE", "medical-bronze"))
    bucket_quarantine: str = field(default_factory=lambda: _env("BUCKET_QUARANTINE", "medical-quarantine"))
    warehouse_path: str = field(default_factory=lambda: _env("WAREHOUSE_PATH", "s3a://nessie-warehouse/"))

    # --- Nessie / Iceberg catalog ---
    nessie_uri: str = field(default_factory=lambda: _env("NESSIE_URI", "http://localhost:19120/api/v2"))
    nessie_default_branch: str = field(default_factory=lambda: _env("NESSIE_DEFAULT_BRANCH", "main"))
    iceberg_catalog_name: str = field(default_factory=lambda: _env("ICEBERG_CATALOG_NAME", "iceberg"))

    # --- De-identification ---
    deid_salt: str = field(default_factory=lambda: _env("DEID_SALT", "local-dev-salt-change-me"))

    # --- Local filesystem fallback (used when RUN_MODE=local so the whole
    # pipeline can run on a laptop with no S3/Spark cluster available) ---
    run_mode: str = field(default_factory=lambda: _env("RUN_MODE", "local"))
    local_data_root: str = field(
        default_factory=lambda: _env("LOCAL_DATA_ROOT", str(Path(__file__).resolve().parents[2] / "data"))
    )

    # --- Data quality thresholds ---
    max_invalid_ratio: float = field(default_factory=lambda: float(_env("MAX_INVALID_RATIO", "0.2")))

    # --- Logging ---
    log_level: str = field(default_factory=lambda: _env("LOG_LEVEL", "INFO"))

    @property
    def is_local(self) -> bool:
        return self.run_mode.lower() == "local"

    def local_path(self, layer: str) -> Path:
        """Return (and create) the local filesystem path for a given layer.

        Layers: raw, bronze, silver, gold, quarantine
        """
        path = Path(self.local_data_root) / layer
        path.mkdir(parents=True, exist_ok=True)
        return path


_settings: Settings | None = None


def get_settings(force_reload: bool = False) -> Settings:
    """Return a process-wide cached Settings instance."""
    global _settings
    if _settings is None or force_reload:
        _settings = Settings()
    return _settings
