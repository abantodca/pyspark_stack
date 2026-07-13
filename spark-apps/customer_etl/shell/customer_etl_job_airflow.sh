
#!/bin/bash

# Step 1: Validate Input
if [ -z "$1" ]; then
  echo "Usage: ./run_customer_etl.sh env"
  exit 1
fi


ENV=${1:-dev}
echo $ENV

echo $HDFS_INPUT
#RUN_DATE="$1"
#LANDING_PATH="/opt/spark-apps/landing/customer_etl/"
#HDFS_INPUT="/customer_etl/input"
#HDFS_OUTPUT="/customer_etl/output/loyalty_snapshot_${RUN_DATE}"
#FINAL_CSV="/opt/spark-apps/shared_output/customer_etl/loyalty_snapshot_${RUN_DATE}.csv"

echo " Running Customer ETL for: $RUN_DATE"
echo " Final CSV will be stored at: $FINAL_CSV"

# Step 2: Detect if running inside container


# modified recently since previous if condition was throwing error for few students(NEW)
if [ -f /.dockerenv ]; then
#if grep -qE 'docker|containerd' /proc/1/cgroup; then
  echo " Detected: Running INSIDE container (Airflow or Jupyter)"

  export PATH=$PATH:/opt/hadoop/bin
  # La raiz de HDFS pertenece a root:supergroup; el usuario 'airflow' no puede escribir en '/'.
  # Se opera como 'root' (dueno de '/') para poder crear /customer_etl/... y subir los inputs.
  export HADOOP_USER_NAME=root

  source /opt/spark-apps/customer_etl/config/env.sh "$ENV"

  echo $env
  echo 'HDFS_INPUT' $HDFS_INPUT
   
  echo " Uploading to HDFS..."
  hdfs dfs -rm -r -f ${HDFS_INPUT}
  hdfs dfs -mkdir -p ${HDFS_INPUT}
  
  hdfs dfs -put "${LANDING_PATH}/customers.csv" ${HDFS_INPUT}/
  hdfs dfs -put "${LANDING_PATH}/products.json" ${HDFS_INPUT}/
  hdfs dfs -put "${LANDING_PATH}/orders.csv" ${HDFS_INPUT}/

  #echo "  Running Spark job..."
  
  #added newly args to spark script since all hardcoding has been removed in pyspark script as well(NEW) 
  spark-submit --master spark://spark-master:7077 \
	  --conf spark.pyspark.python=python3.12 \
	  --conf spark.pyspark.driver.python=python3.12 \
	  /opt/spark-apps/customer_etl/scripts/customer_etl_job.py \
	  $ENV "$RUN_DATE" "$HDFS_INPUT" "$HDFS_OUTPUT"

  echo " Exporting merged CSV to local path..."
  
  # added newly since hdfs merge from inside container is not possible. hence generating in tmp location and copying
  # it to final csv(NEW)

  CONSOLIDATED_CSV="/tmp/loyalty_snapshot_${RUN_DATE}.csv"
  
  hdfs dfs -getmerge "${HDFS_OUTPUT}/part*" "$CONSOLIDATED_CSV"
  cp "$CONSOLIDATED_CSV" "$FINAL_CSV"

else
  echo " Detected: Running OUTSIDE container (Ubuntu host)"

  echo " Uploading to HDFS via docker exec..."
  
  source "$(dirname "$0")/../config/env.sh" "$ENV"

  docker exec hdfs-namenode hdfs dfs -rm -r -f ${HDFS_INPUT}
  docker exec hdfs-namenode hdfs dfs -mkdir -p ${HDFS_INPUT}
  #docker exec hdfs-namenode hdfs dfs -put "${LANDING_PATH}/*" ${HDFS_INPUT}/
  docker exec hdfs-namenode hdfs dfs -put "${LANDING_PATH}/customers.csv" ${HDFS_INPUT}/
  docker exec hdfs-namenode hdfs dfs -put "${LANDING_PATH}/products.json" ${HDFS_INPUT}/
  docker exec hdfs-namenode hdfs dfs -put "${LANDING_PATH}/orders.csv" ${HDFS_INPUT}/


  echo " Submitting Spark job via docker exec..."

  #added newly args to spark script since all hardcoding has been removed in pyspark script as well(NEW)
  docker exec spark-master spark-submit /opt/spark-apps/customer_etl/scripts/customer_etl_job.py \
	  $ENV "$RUN_DATE" "$HDFS_INPUT" "$HDFS_OUTPUT"

  echo "Merging HDFS output to host shared folder..."
  #mkdir -p "/opt/spark-apps/shared_output/customer_etl/"
  docker exec hdfs-namenode hdfs dfs -getmerge ${HDFS_OUTPUT}/part-* "$FINAL_CSV"
fi

echo " Done. Output available at: $FINAL_CSV"

