"""Restore data/sample from tests/fixtures/sample when a checkout omitted it.

GitLab CI previously failed with FileNotFoundError because a .gitignore of
``data/`` (or ``git rm --cached data/``) dropped the tracked FHIR/DICOM
fixtures. Tests still prefer the live files under data/sample/; this module
copies the committed copies under tests/fixtures/sample/ if they are missing.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_SAMPLE = ROOT / "tests" / "fixtures" / "sample"
SAMPLE_DIR = ROOT / "data" / "sample"
REQUIRED = (
    SAMPLE_DIR / "fhir" / "patient_bundle_001.json",
    SAMPLE_DIR / "fhir" / "patient_bundle_002.json",
    SAMPLE_DIR / "fhir" / "patient_bundle_003.json",
    SAMPLE_DIR / "fhir" / "invalid_bundle_004.json",
    SAMPLE_DIR / "dicom" / "sample_dicom_metadata.json",
)


def ensure_sample_data() -> Path:
    missing = [p for p in REQUIRED if not p.is_file()]
    if not missing:
        return SAMPLE_DIR
    if not FIXTURE_SAMPLE.is_dir():
        names = ", ".join(str(p.relative_to(ROOT)) for p in missing)
        raise FileNotFoundError(
            f"Sample fixtures missing ({names}) and {FIXTURE_SAMPLE.relative_to(ROOT)} is not present."
        )
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copytree(FIXTURE_SAMPLE, SAMPLE_DIR, dirs_exist_ok=True)
    still_missing = [p for p in REQUIRED if not p.is_file()]
    if still_missing:
        names = ", ".join(str(p.relative_to(ROOT)) for p in still_missing)
        raise FileNotFoundError(f"Failed to restore sample fixtures: {names}")
    return SAMPLE_DIR


def main() -> int:
    path = ensure_sample_data()
    print(f"sample data ready under {path}")
    for item in sorted(path.rglob("*")):
        if item.is_file():
            print(f"  {item.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except FileNotFoundError as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(1)
