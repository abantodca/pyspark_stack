from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator

from airflow.sdk import DAG

default_args = {
    "owner": "customer_etl_pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="customer_etl_dag",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 5, 8, tz="UTC"),
    schedule="@daily",  # Airflow 3: 'schedule_interval' -> 'schedule'
    catchup=False,
    max_active_runs=1,
    tags=["local", "customer", "quality-gated"],
) as dag:
    run_etl = BashOperator(
        task_id="run_customer_loyalty_etl",
        bash_command=(
            "bash /opt/spark-apps/customer_etl/shell/customer_etl_job_airflow.sh "
            '"$PIPELINE_ENV" "$RUN_DATE"'
        ),
        env={
            "PIPELINE_ENV": "{{ var.value.get('airflow_env', 'dev') }}",
            "RUN_DATE": "{{ data_interval_start | ds }}",
        },
        append_env=True,
    )
