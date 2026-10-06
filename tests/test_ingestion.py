from pathlib import Path

from src.ingestion.fhir_ingest import run_ingestion

SAMPLE_DIR = Path(__file__).resolve().parents[1] / "data" / "sample" / "fhir"


def test_run_ingestion_writes_bronze_and_quarantine(tmp_settings):
    result = run_ingestion(SAMPLE_DIR, batch_id="test-batch-1")

    # 3 valid bundles (6 + 5 + 4 resources) + 1 invalid bundle (3 resources, all bad)
    assert result.metrics.total_records == 18
    assert result.metrics.valid_records == 15
    assert result.metrics.invalid_records == 3
    assert len(result.bronze_paths) == 15
    assert len(result.quarantine_paths) == 3

    bronze_dir = tmp_settings / "bronze" / "test-batch-1"
    quarantine_dir = tmp_settings / "quarantine" / "test-batch-1"
    assert bronze_dir.exists()
    assert quarantine_dir.exists()

    bronze_files = list(bronze_dir.rglob("*.json"))
    assert len(bronze_files) == 15

    quarantine_files = list(quarantine_dir.rglob("*.json"))
    assert len(quarantine_files) == 3


def test_run_ingestion_on_empty_dir_is_safe(tmp_settings, tmp_path):
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    result = run_ingestion(empty_dir, batch_id="empty-batch")
    assert result.metrics.total_records == 0
    assert result.bronze_paths == []
