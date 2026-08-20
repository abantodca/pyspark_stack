"""Controles estructurales minimos de los DAGs del stack local."""

from datetime import timedelta
import os
from pathlib import Path

from airflow.models import DagBag

EXPECTED_DAGS = {
    "customer_etl_dag",
    "spark_wordcount_trigger",
    "spark_wordcount_trigger_hdfs",
}

ROOT = Path(__file__).parents[1]
DAG_FOLDER = Path(os.environ.get("TEST_DAGS_FOLDER", ROOT / "dags"))


def _dag_bag() -> DagBag:
    return DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)


def test_dags_import_without_errors() -> None:
    assert _dag_bag().import_errors == {}


def test_expected_dags_are_present() -> None:
    assert EXPECTED_DAGS <= set(_dag_bag().dags)


def test_every_task_has_an_owner() -> None:
    for dag in _dag_bag().dags.values():
        for task in dag.tasks:
            assert task.owner


def test_customer_etl_has_operational_guards() -> None:
    # Leer el DAG ya parseado evita consultar la metadata DB. Estos tests deben ser
    # herméticos y poder ejecutarse con ``docker compose run --no-deps``.
    dag = _dag_bag().dags.get("customer_etl_dag")
    assert dag is not None
    assert dag.max_active_runs == 1
    task = dag.get_task("run_customer_loyalty_etl")
    assert task.retries == 2
    assert task.execution_timeout == timedelta(hours=1)
    assert task.env["RUN_DATE"] == "{{ data_interval_start | ds }}"
    assert task.env["PIPELINE_ENV"] == "{{ var.value.get('airflow_env', 'dev') }}"


def test_customer_dag_does_not_query_variables_during_parse() -> None:
    source = (DAG_FOLDER / "customer_etl_dag.py").read_text(encoding="utf-8")
    assert "Variable.get(" not in source
