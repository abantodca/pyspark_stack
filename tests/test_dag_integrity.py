"""Controles estructurales mínimos de los DAGs."""

from pathlib import Path

from airflow.models import DagBag


def _dag_bag() -> DagBag:
    return DagBag(
        dag_folder=str(Path(__file__).parents[1] / "dags"), include_examples=False
    )


def test_dags_import_without_errors() -> None:
    assert _dag_bag().import_errors == {}


def test_production_dag_contract() -> None:
    dag = _dag_bag().get_dag("customer_etl_emr")
    assert dag is not None
    assert dag.max_active_runs == 1
    assert {"run_customer_etl", "request_safe_stop"} <= set(dag.task_ids)
