from datetime import datetime

# Airflow 3: DAG desde el Task SDK y BashOperator desde el provider 'standard'.
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 0
}

with DAG('spark_wordcount_trigger_hdfs',
         default_args=default_args,
         schedule=None,           # Airflow 3: 'schedule_interval' -> 'schedule'
         catchup=False,
         tags=['spark']) as dag:

    run_spark_job = BashOperator(
        task_id='submit_wordcount',
        bash_command="""
export HADOOP_USER_NAME=root
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.pyspark.python=python3.12 \
  --conf spark.pyspark.driver.python=python3.12 \
  --conf spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode:9000 \
  /opt/spark-apps/wordcount_hdfs.py
"""
    )
