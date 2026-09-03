#!/usr/bin/env bash
# Sourcear en la terminal: source ./scripts/prod-env.sh
# Ejecutar con --check solo informa el contexto; no puede exportarlo al proceso padre.
_pe_sourced=0; (return 0 2>/dev/null) && _pe_sourced=1
# Raíz del repo: BASH_SOURCE si lo sourcea bash; el shell de task no la define, ahí se busca hacia arriba.
_pe_self="${BASH_SOURCE[0]:-}"
if [ -n "$_pe_self" ]; then
  _pe_root="$(CDPATH= cd "$(dirname "$_pe_self")/.." && pwd)"
else
  _pe_root="$PWD"; _pe_dir="$PWD"
  while [ "$_pe_dir" != / ]; do
    [ -f "$_pe_dir/Taskfile.yml" ] && { _pe_root="$_pe_dir"; break; }
    _pe_dir="$(dirname "$_pe_dir")"
  done
fi
_pe_infra="${INFRA_DIR:-$_pe_root/infra/envs/prod}"

# Overrides exclusivamente locales (perfil AWS, clave SSH). Debe ser un archivo de asignaciones
# controlado por el operador, fuera de Git; Terraform nunca toma valores de aquí.
_pe_overrides="${PROD_ENV_FILE:-$_pe_infra/prod.env}"
if [ -r "$_pe_overrides" ]; then
  set -a
  . "$_pe_overrides"
  set +a
fi

# Solo valores locales; los identificadores de AWS siempre llegan como outputs.
export AWS_PAGER=""
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="$AWS_REGION"
export SSH_KEY="${SSH_KEY:-$HOME/.ssh/pyspark_stack}"
export SSH_USER="${SSH_USER:-ec2-user}"
export REMOTE_DIR="${REMOTE_DIR:-/home/$SSH_USER/pyspark_stack}"
export COMPOSE_PROD="${COMPOSE_PROD:-docker-compose.prod.yml}"

if [ -d "$_pe_infra/.terraform" ]; then
  _pe_json="$(terraform -chdir="$_pe_infra" output -json)" || {
    echo "prod-env: terraform output falló; ejecute init o revise el state" >&2
    [ "$_pe_sourced" -eq 1 ] && return 1 || exit 1
  }
  if [ "$(printf '%s' "$_pe_json" | jq 'length')" -gt 0 ]; then
    eval "$(printf '%s' "$_pe_json" | jq -r '
      to_entries[]
      | select(.key | test("^[A-Za-z_][A-Za-z0-9_]*$"))
      | (.key | ascii_upcase) as $key
      | (.value.value | if type == "string" then . else tojson end) as $value
      | "export \($key)=\($value | @sh)"')"
  else
    echo "prod-env: contexto parcial — el state aún no tiene outputs" >&2
  fi
else
  echo "prod-env: contexto parcial — todavía no existe state inicializado" >&2
fi

# Derivadas: solo aparecen cuando existe su valor base.
[ -n "${ARTIFACTS_BUCKET:-}" ] && export EMR_ENTRYPOINTS_URI="s3://$ARTIFACTS_BUCKET/emr" EMR_LOGS_URI="s3://$ARTIFACTS_BUCKET/emr/logs" ATHENA_RESULTS_URI="s3://$ARTIFACTS_BUCKET/athena-results"
[ -n "${DATALAKE_BUCKET:-}" ] && export RAW_URI="s3://$DATALAKE_BUCKET/raw" CURATED_URI="s3://$DATALAKE_BUCKET/curated"
[ -n "${PUBLIC_IP:-}" ] && export SSH_TARGET="$SSH_USER@$PUBLIC_IP" SSH="ssh -i $SSH_KEY" RSYNC_SSH="ssh -i $SSH_KEY"

if [ "${1:-}" = "--check" ]; then
  _pe_strict=0
  [ "${2:-}" = "--strict" ] && _pe_strict=1
  _pe_missing=""
  printf 'Contexto de producción (fuente: terraform; lectura fresca del state)\n'
  for _pe_var in AWS_REGION NAME_PREFIX ACCOUNT_ID INSTANCE_ID PUBLIC_IP DATALAKE_BUCKET ARTIFACTS_BUCKET EMR_APP_ID SQS_TRIGGER_QUEUE_URL AIRFLOW_URL; do
    eval "_pe_value=\${$_pe_var:-}"
    printf '%-24s %s\n' "$_pe_var" "${_pe_value:-— (sin definir aún)}"
    [ -n "${_pe_value:-}" ] || _pe_missing="$_pe_missing $_pe_var"
  done
  if [ -n "$_pe_missing" ]; then
    echo "prod-env: contexto parcial; faltan:$_pe_missing" >&2
    # El modo normal informa durante el recorrido incremental. --strict es para un gate final.
    if [ "$_pe_strict" -eq 1 ]; then
      [ "$_pe_sourced" -eq 1 ] && return 1 || exit 1
    fi
  else
    echo 'prod-env: contexto completo'
  fi
fi
unset _pe_json _pe_value _pe_var _pe_missing _pe_overrides _pe_infra _pe_root _pe_self _pe_dir _pe_sourced _pe_strict
