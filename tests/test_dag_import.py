"""
DAG import test.

Requires `apache-airflow` (deliberately NOT in requirements.txt -- see that
file's comment on why Airflow needs its own constraints-file install). If
Airflow isn't installed in the environment running pytest, this test is
skipped rather than failing the whole suite; it is expected to run for real
in the CI pipeline's dedicated "validate DAGs" step and in the Airflow
Docker image (see docker/Dockerfile.airflow and .github/workflows/ci.yml).
"""

from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow", reason="apache-airflow not installed in this environment")

REPO_ROOT = Path(__file__).resolve().parents[1]
EXPECTED_TASK_ORDER = [
    "extract_validate_fhir",
    "deidentify",
    "spark_transform",
    "data_quality",
    "write_silver",
    "build_gold",
    "post_load_validation",
]


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=str(REPO_ROOT / "dags"), include_examples=False)


def test_dagbag_has_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_medical_data_pipeline_dag_loads(dagbag):
    dag = dagbag.dags.get("medical_data_pipeline")
    assert dag is not None, "medical_data_pipeline DAG not found"


def test_dag_has_expected_tasks(dagbag):
    dag = dagbag.dags["medical_data_pipeline"]
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == set(EXPECTED_TASK_ORDER)


def test_dag_task_dependency_chain_matches_spec(dagbag):
    """extract_fhir -> validate_fhir -> write_bronze -> deidentify -> spark_transform
    -> data_quality -> write_silver -> build_gold -> post_load_validation
    (extract/validate/write_bronze are combined into extract_validate_fhir --
    see the DAG module docstring for why)."""
    dag = dagbag.dags["medical_data_pipeline"]
    for upstream_id, downstream_id in zip(EXPECTED_TASK_ORDER, EXPECTED_TASK_ORDER[1:]):
        upstream_task = dag.get_task(upstream_id)
        downstream_ids = {t.task_id for t in upstream_task.downstream_list}
        assert downstream_id in downstream_ids, f"{upstream_id} should be upstream of {downstream_id}"


def test_dag_has_retries_and_retry_delay_configured(dagbag):
    dag = dagbag.dags["medical_data_pipeline"]
    for task in dag.tasks:
        assert task.retries >= 1
        assert task.retry_delay is not None


def test_dag_is_not_scheduled_to_catch_up(dagbag):
    dag = dagbag.dags["medical_data_pipeline"]
    assert dag.catchup is False
