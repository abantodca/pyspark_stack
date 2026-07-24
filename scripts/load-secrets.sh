#!/usr/bin/env bash
# Materializa el .env efímero de producción desde SSM Parameter Store.
set -euo pipefail
umask 077

cd "$(dirname "$0")/.."

PARAMETER_PREFIX="${PARAMETER_PREFIX:-/pyspark-stack}"
AWS_DEPLOY_REGION="${AWS_REGION:-us-east-1}"

get_secret() {
  aws ssm get-parameter \
    --name "${PARAMETER_PREFIX}/$1" \
    --with-decryption \
    --query Parameter.Value \
    --output text \
    --region "$AWS_DEPLOY_REGION"
}

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
EMR_APP_ID="$(aws emr-serverless list-applications \
  --query "applications[?name=='pyspark-stack-spark'].id | [0]" \
  --output text \
  --region "$AWS_DEPLOY_REGION")"

if [[ -z "$EMR_APP_ID" || "$EMR_APP_ID" == "None" ]]; then
  echo "No se encontró la aplicación EMR Serverless pyspark-stack-spark." >&2
  exit 1
fi

cat >.env <<EOF
POSTGRES_USER=airflow
POSTGRES_DB=airflow
POSTGRES_PASSWORD=$(get_secret postgres_password)
AIRFLOW_JWT_SECRET=$(get_secret airflow_jwt_secret)
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=$(get_secret airflow_admin_password)
EMR_APP_ID=$EMR_APP_ID
EMR_JOB_ROLE_ARN=arn:aws:iam::$ACCOUNT_ID:role/pyspark-stack-emr-serverless-job
DATALAKE_BUCKET=pyspark-stack-datalake-$ACCOUNT_ID
ARTIFACTS_BUCKET=pyspark-stack-artifacts-$ACCOUNT_ID
EOF

chmod 600 .env
echo ".env de producción generado con permisos 0600."
