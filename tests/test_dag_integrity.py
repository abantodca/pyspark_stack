"""Controles estructurales minimos de los DAGs del stack local."""

from pathlib import Path

from airflow.models import DagBag

EXPECTED_DAGS = {
    "customer_etl_dag",
    "spark_wordcount_trigger",
    "spark_wordcount_trigger_hdfs",
}


def _dag_bag() -> DagBag:
    return DagBag(
        dag_folder=str(Path(__file__).parents[1] / "dags"), include_examples=False
    )


def test_dags_import_without_errors() -> None:
    assert _dag_bag().import_errors == {}


def test_expected_dags_are_present() -> None:
    assert EXPECTED_DAGS <= set(_dag_bag().dags)


def test_every_task_has_an_owner() -> None:
    for dag in _dag_bag().dags.values():
        for task in dag.tasks:
            assert task.owner
