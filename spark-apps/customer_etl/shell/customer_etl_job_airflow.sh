#!/usr/bin/env bash

set -Eeuo pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: customer_etl_job_airflow.sh <dev|prod> <run_date:YYYY-MM-DD>" >&2
  exit 1
fi

ENV="$1"
RUN_DATE="$2"

if [ -f /.dockerenv ]; then
  echo "[INFO] ejecución dentro de contenedor"

  source /opt/spark-apps/customer_etl/config/env.sh "$ENV" "$RUN_DATE"

  echo "[INFO] cargando lote aislado en $HDFS_INPUT"
  python /opt/spark-apps/customer_etl/scripts/hdfs_io.py \
    load "$HDFS_INPUT" "$LANDING_PATH"
  python /opt/spark-apps/customer_etl/scripts/hdfs_io.py \
    prepare-output "$HDFS_OUTPUT"

  # En modo simple HDFS toma el usuario del proceso del driver (airflow). El job
  # publica con la identidad dedicada que posee únicamente el directorio output.
  export HADOOP_USER_NAME="${HDFS_OUTPUT_USER:-spark}"
  spark-submit --master spark://spark-master:7077 \
    --conf spark.pyspark.python=python3.12 \
    --conf spark.pyspark.driver.python=python3.12 \
    /opt/spark-apps/customer_etl/scripts/customer_etl_job.py \
    "$ENV" "$RUN_DATE" "$HDFS_INPUT" "$HDFS_OUTPUT"

  python /opt/spark-apps/customer_etl/scripts/hdfs_io.py \
    validate-output "$HDFS_OUTPUT"
  echo "[INFO] resultado validado en HDFS: $HDFS_OUTPUT"

else
  echo "[INFO] ejecución desde el host"
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
  source "$SCRIPT_DIR/../config/env.sh" "$ENV" "$RUN_DATE"

  docker exec hdfs-namenode hdfs dfs -rm -r -f "$HDFS_INPUT"
  docker exec hdfs-namenode hdfs dfs -mkdir -p "$HDFS_INPUT"
  docker exec hdfs-namenode hdfs dfs -put "${LANDING_PATH}/customers.csv" "$HDFS_INPUT/"
  docker exec hdfs-namenode hdfs dfs -put "${LANDING_PATH}/products.json" "$HDFS_INPUT/"
  docker exec hdfs-namenode hdfs dfs -put "${LANDING_PATH}/orders.csv" "$HDFS_INPUT/"

  HDFS_OUTPUT_PARENT="${HDFS_OUTPUT%/*}"
  docker exec hdfs-namenode hdfs dfs -mkdir -p "$HDFS_OUTPUT_PARENT"
  docker exec hdfs-namenode hdfs dfs -chown spark "$HDFS_OUTPUT_PARENT"

  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.pyspark.python=python3.12 \
    --conf spark.pyspark.driver.python=python3.12 \
    /opt/spark-apps/customer_etl/scripts/customer_etl_job.py \
    "$ENV" "$RUN_DATE" "$HDFS_INPUT" "$HDFS_OUTPUT"

  OUTPUT_DIR="$PROJECT_ROOT/spark-apps/shared_output/customer_etl"
  FINAL_CSV="$OUTPUT_DIR/loyalty_snapshot_${RUN_DATE}.csv"
  mkdir -p "$OUTPUT_DIR"
  TEMP_OUTPUT_DIR="$(mktemp -d "$OUTPUT_DIR/.loyalty_snapshot_${RUN_DATE}.XXXXXX")"
  TEMP_CSV="$TEMP_OUTPUT_DIR/output.csv"
  TEMP_CRC="$TEMP_OUTPUT_DIR/.output.csv.crc"
  FINAL_CRC="$OUTPUT_DIR/.$(basename "$FINAL_CSV").crc"
  cleanup_output_temp() {
    rm -f -- "$TEMP_CSV" "$TEMP_CRC"
    rmdir -- "$TEMP_OUTPUT_DIR" 2>/dev/null || true
  }
  trap cleanup_output_temp EXIT
  docker exec hdfs-namenode hdfs dfs -getmerge \
    "${HDFS_OUTPUT}/part*" \
    "/opt/spark-apps/shared_output/customer_etl/$(basename "$TEMP_OUTPUT_DIR")/output.csv"
  test -s "$TEMP_CSV"
  mv -f -- "$TEMP_CSV" "$FINAL_CSV"
  # Hadoop genera checksums para el filesystem local del contenedor; el CSV
  # compartido no los necesita y un sidecar previo no debe sobrevivir al reemplazo.
  rm -f -- "$TEMP_CRC" "$FINAL_CRC"
  rmdir -- "$TEMP_OUTPUT_DIR"
  trap - EXIT
  echo "[INFO] resultado disponible en $FINAL_CSV"
fi
