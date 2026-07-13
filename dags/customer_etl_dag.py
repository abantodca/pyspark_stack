from datetime import datetime, timedelta

# Airflow 3: DAG y Variable viven en el Task SDK (airflow.sdk);
# BashOperator se movio al provider 'standard' (preinstalado con Airflow 3).
from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    'owner': 'customer_etl_pipeline',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='customer_etl_dag',
    default_args=default_args,
    start_date=datetime(2025, 5, 8),
    schedule='@daily',            # Airflow 3: 'schedule_interval' -> 'schedule'
    catchup=False
) as dag:

    env = Variable.get("airflow_env", default="dev")   # Airflow 3: 'default_var' -> 'default'
    run_etl = BashOperator(
        task_id='run_customer_loyalty_etl',
        bash_command=f"bash /opt/spark-apps/customer_etl/shell/customer_etl_job_airflow.sh {env}"
    )

    run_etl
