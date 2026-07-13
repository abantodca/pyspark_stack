
#!/bin/bash

# Check for argument
if [ -z "$1" ]; then
  echo " Usage: ./run_sales_job.sh <YYYY-MM-DD>"
  exit 1
fi

RUN_DATE="$1"

echo "Triggering sales_etl_job.py for $RUN_DATE..."

# Run spark-submit inside spark-master container
docker exec spark-master spark-submit /opt/spark-apps/scripts/sales_etl_job.py "$RUN_DATE"

echo "Job completed for $RUN_DATE"
