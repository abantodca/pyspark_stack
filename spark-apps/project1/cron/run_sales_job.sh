#!/bin/bash

# Use today's date as run_date
RUN_DATE=$(date +%F)

echo "🚀 Running Spark job for $RUN_DATE..."

docker exec spark-master spark-submit /opt/spark-apps/scripts/sales_etl_job.py "$RUN_DATE"

echo "✅ Completed for $RUN_DATE"
