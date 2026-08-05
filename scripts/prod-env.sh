#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/prod-env.sh — única fuente de verdad del contexto de producción.
#
# Convierte los outputs de Terraform en variables de entorno, para que TODOS los
# comandos de las guías sean copy-paste sin editar nada: ni IDs, ni IP, ni
# nombres de bucket, ni account id, ni región.
#
#   source ./scripts/prod-env.sh              # modo terraform (por defecto)
#   PROD_ENV_SOURCE=discover source ./scripts/prod-env.sh   # sin state (guía 02b)
#   ./scripts/prod-env.sh --check             # valida y muestra; NO exporta
#   PROD_ENV_REFRESH=1 source ./scripts/prod-env.sh         # ignora la caché
#
# CÓMO ESCALA (leer antes de agregar un recurso):
#   El bucle de abajo NO tiene una lista de variables: exporta *todo* lo que
#   `terraform output -json` devuelva, pasando el nombre a MAYÚSCULAS. Por eso,
#   para que un recurso nuevo esté disponible en los comandos, el único paso es
#   declarar su `output` en infra/prod/outputs.tf. Este archivo no se toca.
#       output "sqs_trigger_queue_url" { ... }   →   $SQS_TRIGGER_QUEUE_URL
#   Solo se edita acá lo que Terraform no puede saber (rutas locales: la clave
#   SSH) o lo que hay que descubrir sin state (modo discover).
# ─────────────────────────────────────────────────────────────────────────────
set -uo pipefail

# ── Detección de `source` vs ejecución directa ───────────────────────────────
# Ejecutado directo, los export mueren con el proceso: solo tiene sentido para
# --check. Sourceado, exporta en la shell actual.
__pe_sourced=0
(return 0 2>/dev/null) && __pe_sourced=1

__pe_die() { printf '\033[31mprod-env: %s\033[0m\n' "$*" >&2; return 1; }

# ── Raíz del repo: el script funciona desde cualquier directorio ─────────────
# Sin esto, `terraform -chdir=infra/prod` obliga a estar parado en la raíz.
__pe_self="${BASH_SOURCE[0]:-$0}"
PROD_ENV_ROOT="$(cd "$(dirname "$__pe_self")/.." && pwd)"
export PROD_ENV_ROOT
INFRA_DIR="${INFRA_DIR:-$PROD_ENV_ROOT/infra/prod}"

# ── Overrides locales, opcionales y no versionados ───────────────────────────
# infra/prod/prod.env sirve para apuntar a otra cuenta/entorno sin tocar el
# script: SSH_KEY=..., AWS_PROFILE=..., NAME_PREFIX=...
# shellcheck disable=SC1091
[ -f "$INFRA_DIR/prod.env" ] && . "$INFRA_DIR/prod.env"

# ── Valores que Terraform no conoce (son de TU máquina), todos overridables ──
export SSH_KEY="${SSH_KEY:-$HOME/.ssh/pyspark_stack}"
export SSH_USER="${SSH_USER:-ec2-user}"
export REMOTE_DIR="${REMOTE_DIR:-/home/$SSH_USER/pyspark_stack}"
export COMPOSE_PROD="${COMPOSE_PROD:-docker-compose.prod.yml}"
export AWS_PAGER=""   # sin esto, cada `aws ... --output text` abre el pager

PROD_ENV_SOURCE="${PROD_ENV_SOURCE:-terraform}"
PROD_ENV_TTL="${PROD_ENV_TTL:-900}"   # segundos de caché; 0 = siempre fresco

# ── Dependencias ─────────────────────────────────────────────────────────────
for __pe_bin in jq aws; do
  command -v "$__pe_bin" >/dev/null 2>&1 || { __pe_die "falta '$__pe_bin' en el PATH"; return 1 2>/dev/null || exit 1; }
done

# ─────────────────────────────────────────────────────────────────────────────
# MODO terraform — el normal (guía 02). Una sola lectura del state.
# ─────────────────────────────────────────────────────────────────────────────
__pe_load_terraform() {
  command -v terraform >/dev/null 2>&1 || { __pe_die "falta terraform; probá PROD_ENV_SOURCE=discover"; return 1; }
  [ -d "$INFRA_DIR" ] || { __pe_die "no existe $INFRA_DIR (¿ya corriste §4-§5?)"; return 1; }

  # Caché: `terraform output` baja el state de S3 en cada llamada (~1-2 s). Con
  # 20 comandos por sesión eso se nota; el TTL lo evita sin quedar obsoleto.
  local cache="${TMPDIR:-/tmp}/pyspark-stack-prod-env.$(id -u).json"
  local fresh=0
  if [ "$PROD_ENV_TTL" -gt 0 ] && [ -z "${PROD_ENV_REFRESH:-}" ] && [ -s "$cache" ]; then
    local age=$(( $(date +%s) - $(stat -c %Y "$cache" 2>/dev/null || echo 0) ))
    [ "$age" -lt "$PROD_ENV_TTL" ] && fresh=1
  fi

  if [ "$fresh" -eq 0 ]; then
    local json
    json="$(terraform -chdir="$INFRA_DIR" output -json 2>/dev/null)" || {
      __pe_die "terraform output falló; ¿corriste 'terraform -chdir=$INFRA_DIR init'?"; return 1; }
    [ "$(printf '%s' "$json" | jq 'length')" -gt 0 ] || {
      __pe_die "el state no tiene outputs; falta 'terraform apply' o falta outputs.tf"; return 1; }
    printf '%s' "$json" > "$cache" && chmod 600 "$cache"
    PROD_ENV_AGE=0
  else
    # Expuesto para que --check pueda avisar: un contexto leído de caché puede ser
    # anterior al último `terraform apply` y no incluir los outputs nuevos.
    PROD_ENV_AGE=$(( $(date +%s) - $(stat -c %Y "$cache" 2>/dev/null || echo 0) ))
  fi
  export PROD_ENV_AGE

  # El bucle genérico: cada output se vuelve una variable. Agregar un output
  # basta para tener la variable — este script no cambia nunca por eso.
  # @sh cita el valor: nombres con espacios o comillas no rompen el eval.
  local exports
  exports="$(jq -r '
    to_entries[]
    | select(.key | test("^[A-Za-z_][A-Za-z0-9_]*$"))
    | .key as $k
    | (if (.value.value | type) == "string" then .value.value else (.value.value | tojson) end) as $v
    | "export \($k | ascii_upcase)=\($v | @sh)"
  ' "$cache")" || { __pe_die "no pude parsear los outputs"; return 1; }
  eval "$exports"

  # AWS_REGION no siempre viene del output (depende de qué secciones aplicaste):
  # sin él, el AWS CLI usa el perfil por defecto y podés operar en otra región.
  export AWS_REGION="${AWS_REGION:-${TF_REGION:-us-east-1}}"
  export AWS_DEFAULT_REGION="$AWS_REGION"
}

# ─────────────────────────────────────────────────────────────────────────────
# MODO discover — sin Terraform (guía 02b, infra creada a mano en la consola).
# Descubre lo mismo por tags y por convención de nombres.
# ─────────────────────────────────────────────────────────────────────────────
__pe_load_discover() {
  export AWS_REGION="${AWS_REGION:-us-east-1}"
  export AWS_DEFAULT_REGION="$AWS_REGION"
  export NAME_PREFIX="${NAME_PREFIX:-pyspark-stack}"

  ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)" || {
    __pe_die "sin credenciales AWS válidas (aws configure)"; return 1; }
  export ACCOUNT_ID

  export INSTANCE_ID="${INSTANCE_ID:-$(aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=${NAME_PREFIX}-node" \
              "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query 'Reservations[0].Instances[0].InstanceId' --output text 2>/dev/null)}"

  export PUBLIC_IP="${PUBLIC_IP:-$(aws ec2 describe-addresses \
    --filters "Name=tag:Name,Values=${NAME_PREFIX}-eip" \
    --query 'Addresses[0].PublicIp' --output text 2>/dev/null)}"
  # Sin tag en la EIP, caemos a la IP asociada a la instancia.
  if [ -z "$PUBLIC_IP" ] || [ "$PUBLIC_IP" = "None" ]; then
    export PUBLIC_IP="$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" \
      --query 'Reservations[0].Instances[0].PublicIpAddress' --output text 2>/dev/null)"
  fi

  export SECURITY_GROUP_ID="${SECURITY_GROUP_ID:-$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=${NAME_PREFIX}-sg" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)}"

  export DATALAKE_BUCKET="${NAME_PREFIX}-datalake-${ACCOUNT_ID}"
  export ARTIFACTS_BUCKET="${NAME_PREFIX}-artifacts-${ACCOUNT_ID}"
  export EMR_JOB_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${NAME_PREFIX}-emr-serverless-job"

  export EMR_APP_ID="${EMR_APP_ID:-$(aws emr-serverless list-applications \
    --query "applications[?name=='${NAME_PREFIX}-spark'].id | [0]" --output text 2>/dev/null)}"

  export SQS_TRIGGER_QUEUE_URL="${SQS_TRIGGER_QUEUE_URL:-$(aws sqs get-queue-url \
    --queue-name "${NAME_PREFIX}-trigger-events" --query QueueUrl --output text 2>/dev/null)}"

  export LAMBDA_STARTSTOP_NAME="${NAME_PREFIX}-startstop"
  export LAMBDA_TRIGGER_NAME="${NAME_PREFIX}-trigger-airflow"
  export GLUE_DATABASE="${NAME_PREFIX//-/_}_analytics"
  export ATHENA_WORKGROUP="${NAME_PREFIX}-analytics"
}

# ── Carga ────────────────────────────────────────────────────────────────────
case "$PROD_ENV_SOURCE" in
  terraform) __pe_load_terraform || { [ "$__pe_sourced" -eq 1 ] && return 1 || exit 1; } ;;
  discover)  __pe_load_discover  || { [ "$__pe_sourced" -eq 1 ] && return 1 || exit 1; } ;;
  *)         __pe_die "PROD_ENV_SOURCE debe ser 'terraform' o 'discover'"; [ "$__pe_sourced" -eq 1 ] && return 1 || exit 1 ;;
esac

# ── Variables derivadas: se calculan una vez acá, no en cada comando ─────────
# Ojo: solo se derivan si su base existe, para no crear rutas tipo "s3:///emr/".
[ -n "${ARTIFACTS_BUCKET:-}" ] && {
  export EMR_ENTRYPOINTS_URI="s3://${ARTIFACTS_BUCKET}/emr"
  export EMR_LOGS_URI="s3://${ARTIFACTS_BUCKET}/emr/logs"
  export ATHENA_RESULTS_URI="s3://${ARTIFACTS_BUCKET}/athena-results"
}
[ -n "${DATALAKE_BUCKET:-}" ] && {
  export RAW_URI="s3://${DATALAKE_BUCKET}/raw"
  export CURATED_URI="s3://${DATALAKE_BUCKET}/curated"
}
[ -n "${PUBLIC_IP:-}" ] && {
  export SSH_TARGET="${SSH_USER}@${PUBLIC_IP}"
  # Un solo lugar define las opciones de SSH: los comandos usan $SSH y $RSYNC_SSH.
  export SSH="ssh -i $SSH_KEY"
  export RSYNC_SSH="ssh -i $SSH_KEY"
}

# ── Verificación ─────────────────────────────────────────────────────────────
# La lista crece con la guía: una variable pasa a REQUIRED recién cuando la
# sección que crea su recurso ya se aplicó. Por eso el check avisa, no aborta.
prod_env_check() {
  local required="AWS_REGION NAME_PREFIX ACCOUNT_ID INSTANCE_ID PUBLIC_IP"
  local optional="DATALAKE_BUCKET ARTIFACTS_BUCKET EMR_APP_ID SQS_TRIGGER_QUEUE_URL AIRFLOW_URL"
  local missing="" v
  for v in $required; do
    [ -n "${!v:-}" ] && [ "${!v}" != "None" ] || missing="$missing $v"
  done
  local origen="lectura fresca del state"
  if [ "${PROD_ENV_AGE:-0}" -gt 0 ]; then
    origen="caché de hace ${PROD_ENV_AGE}s — si aplicaste después, recargá con PROD_ENV_REFRESH=1"
  fi
  printf '\033[1mContexto de producción\033[0m  (fuente: %s · región: %s)\n' "$PROD_ENV_SOURCE" "$AWS_REGION"
  [ "$PROD_ENV_SOURCE" = "terraform" ] && printf '  \033[2m%s\033[0m\n' "$origen"
  for v in $required $optional; do
    printf '  %-24s %s\n' "$v" "${!v:-— (sin definir aún: falta su sección)}"
  done
  [ -f "$SSH_KEY" ] || printf '\n\033[33m  aviso: no existe la clave SSH %s (definila con SSH_KEY=...)\033[0m\n' "$SSH_KEY"
  if [ -n "$missing" ]; then
    printf '\n\033[31m  faltan obligatorias:%s\033[0m\n' "$missing"; return 1
  fi
  printf '\n\033[32m  ok: contexto completo\033[0m\n'
}

if [ "$__pe_sourced" -eq 0 ] || [ "${1:-}" = "--check" ]; then
  prod_env_check
fi

unset __pe_self __pe_bin __pe_sourced
