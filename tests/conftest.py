import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

SAMPLE_FHIR_DIR = ROOT / "data" / "sample" / "fhir"
SAMPLE_DICOM_FILE = ROOT / "data" / "sample" / "dicom" / "sample_dicom_metadata.json"


@pytest.fixture()
def sample_patient_resource() -> dict:
    bundle = json.loads((SAMPLE_FHIR_DIR / "patient_bundle_001.json").read_text())
    return bundle["entry"][0]["resource"]


@pytest.fixture()
def sample_observation_resource() -> dict:
    bundle = json.loads((SAMPLE_FHIR_DIR / "patient_bundle_001.json").read_text())
    return next(e["resource"] for e in bundle["entry"] if e["resource"]["resourceType"] == "Observation")


@pytest.fixture()
def tmp_settings(tmp_path, monkeypatch):
    """Point local storage at a scratch directory and reset the settings cache."""
    monkeypatch.setenv("RUN_MODE", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("DEID_SALT", "test-salt")
    from src.utils.config import get_settings

    get_settings(force_reload=True)
    yield tmp_path
    get_settings(force_reload=True)
