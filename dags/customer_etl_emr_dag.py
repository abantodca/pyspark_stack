"""Pipeline productivo de referencia: Airflow en EC2, cómputo en EMR Serverless."""

from datetime import datetime, timedelta, timezone

import boto3
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.sdk import DAG, task

with DAG(
    dag_id="customer_etl_emr",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["prod", "emr", "customer"],
) as dag:
    run_emr = EmrServerlessStartJobOperator(
        task_id="run_customer_etl",
        name="customer-etl-{{ ts_nodash }}",
        application_id="{{ var.value.emr_app_id }}",
        execution_role_arn="{{ var.value.emr_job_role_arn }}",
        deferrable=True,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://{{ var.value.artifacts }}/emr/customer_etl.py",
                # El evento puede incluir la key que disparó el DAG, pero este ETL de referencia
                # procesa el dataset completo raw/customer_etl de forma idempotente por fecha.
                "entryPointArguments": [
                    "{{ dag_run.conf.get('bucket', var.value.datalake) }}",
                    "{{ ds }}",
                ],
                "sparkSubmitParameters": (
                    "--conf spark.executor.cores=2 "
                    "--conf spark.executor.memory=4g "
                    "--conf spark.dynamicAllocation.enabled=true"
                ),
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": "s3://{{ var.value.artifacts }}/emr/logs/"
                },
                "cloudWatchLoggingConfiguration": {
                    "enabled": True,
                    "logGroupName": "/aws/emr-serverless",
                },
            }
        },
    )

    @task(trigger_rule="all_done")
    def request_safe_stop() -> None:
        """Solicita el apagado; la Lambda vuelve a comprobar DAGs activos."""
        boto3.client("lambda").invoke(
            FunctionName="pyspark-stack-startstop",
            InvocationType="Event",
            Payload=b'{"action":"stop"}',
        )

    run_emr >> request_safe_stop()
