#!/usr/bin/env bash

set -euo pipefail

# Usage: source env.sh dev 2026-08-19 OR source env.sh prod 2026-08-19
ENV="${1:?falta env (dev o prod)}"
RUN_DATE="${2:?falta run_date (YYYY-MM-DD)}"

case "$ENV" in
dev|prod) ;;
*) echo "env debe ser 'dev' o 'prod': $ENV" >&2; return 2 2>/dev/null || exit 2 ;;
esac
if [[ ! "$RUN_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] ||
  [ "$(date -d "$RUN_DATE" +%F 2>/dev/null || true)" != "$RUN_DATE" ]; then
  echo "run_date debe tener formato YYYY-MM-DD: $RUN_DATE" >&2
  return 2 2>/dev/null || exit 2
fi

if [ "$ENV" = "prod" ]; then
  # Prod HDFS paths
  LANDING_PATH="/opt/spark-apps/landing/customer_etl/"
  HDFS_INPUT="/prod/customer_etl/input/run_date=${RUN_DATE}"
  HDFS_OUTPUT="/prod/customer_etl/output/loyalty_snapshot_${RUN_DATE}"
else
  # Dev HDFS paths (default)
  LANDING_PATH="/opt/spark-apps/landing/customer_etl/"
  HDFS_INPUT="/customer_etl/input/run_date=${RUN_DATE}"
  HDFS_OUTPUT="/customer_etl/output/loyalty_snapshot_${RUN_DATE}"
fi
# Export all variables so Python/PySpark can read them
export ENV
export RUN_DATE
export LANDING_PATH
export HDFS_INPUT
export HDFS_OUTPUT
