#!/usr/bin/env bash
# scripts/prod-env.sh — única fuente de verdad del contexto de producción.
#
#   source ./scripts/prod-env.sh                            # modo terraform (por defecto)
#   PROD_ENV_SOURCE=discover source ./scripts/prod-env.sh   # sin state (guía 02b)
#   ./scripts/prod-env.sh --check                           # valida y muestra; NO exporta
#   PROD_ENV_REFRESH=1 source ./scripts/prod-env.sh         # ignora la caché
__pe_sourced=0
(return 0 2>/dev/null) && __pe_sourced=1

# `set -u` solo al ejecutar: al sourcear se lo comería la shell de quien lo carga.
if [ "$__pe_sourced" -eq 0 ]; then
  set -uo pipefail
fi

# ─── avisos ───────────────────────────────────────────────────────────────────
__pe_msg() {
  local abre='' cierra=''
  if [ -t 2 ]; then          # sin color fuera de la terminal: el CI captura esto a un log
    abre=$'\033['"$1"'m'
    cierra=$'\033[0m'
  fi
  printf '%sprod-env: %s%s\n' "$abre" "${*:2}" "$cierra" >&2
}
__pe_die() { __pe_msg 31 "$@"; return 1; }
__pe_warn() { __pe_msg 33 "$@"; }
# Contexto parcial: la infra todavía no existe. NO es un error — el mismo `source` tiene que
# funcionar en §1 y en §22; lo único que cambia es cuántas variables trae.
__pe_partial() {
  PROD_ENV_PARTIAL=1
  export PROD_ENV_PARTIAL
  __pe_warn "contexto parcial — $1. Cargué solo lo local (SSH, región); recargá con PROD_ENV_REFRESH=1 después del apply"
}
__pe_require() {
  local bin
  for bin in "$@"; do
    command -v "$bin" >/dev/null 2>&1 || __pe_die "falta '$bin' en el PATH" || return 1
  done
}

# ─── contexto local ───────────────────────────────────────────────────────────
__pe_self="${BASH_SOURCE[0]:-$0}"
PROD_ENV_ROOT="$(cd "$(dirname "$__pe_self")/.." && pwd)"
export PROD_ENV_ROOT
INFRA_DIR="${INFRA_DIR:-$PROD_ENV_ROOT/infra/envs/prod}"

# Overrides locales, opcionales y NO versionados (otra cuenta, otro perfil, otra clave).
if [ -f "$INFRA_DIR/prod.env" ]; then
  # shellcheck disable=SC1091
  . "$INFRA_DIR/prod.env"
fi

# Lo que Terraform no puede saber porque es de TU máquina. Todo overridable.
export SSH_KEY="${SSH_KEY:-$HOME/.ssh/pyspark_stack}"
export SSH_USER="${SSH_USER:-ec2-user}"
export REMOTE_DIR="${REMOTE_DIR:-/home/$SSH_USER/pyspark_stack}"
export COMPOSE_PROD="${COMPOSE_PROD:-docker-compose.prod.yml}"
export AWS_PAGER=""                             # o cada `aws ... --output text` abre el pager
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="$AWS_REGION"
PROD_ENV_SOURCE="${PROD_ENV_SOURCE:-terraform}"
PROD_ENV_TTL="${PROD_ENV_TTL:-900}"             # segundos de caché; 0 = siempre fresco

# ─── carga: terraform ─────────────────────────────────────────────────────────
__pe_age_of() {
  local mtime
  mtime="$(stat -c %Y "$1" 2>/dev/null || stat -f %m "$1" 2>/dev/null || echo 0)"
  echo $(( $(date +%s) - mtime ))
}

__pe_fetch_outputs() {
  local destino="$1" err="$1.err" json
  if ! json="$(terraform -chdir="$INFRA_DIR" output -json 2>"$err")"; then
    __pe_die "terraform output falló ($(grep . "$err" | tail -n1)); ¿corriste 'terraform -chdir=$INFRA_DIR init'?"
    rm -f "$err"
    return 1
  fi
  rm -f "$err"
  # 2 = el state existe pero todavía no publicó outputs (falta el primer apply). Lo distingue
  # del 1 para que el caller avise en vez de abortar.
  [ "$(printf '%s' "$json" | jq 'length')" -gt 0 ] || return 2
  # umask en subshell: el archivo nace 600, sin ventana legible por el resto del host.
  ( umask 077; printf '%s' "$json" > "$destino" ) || __pe_die "no pude escribir $destino" || return 1
  chmod 600 "$destino" 2>/dev/null
}

# EL MOTOR: cada output se vuelve una variable, sin lista que mantener. Las reservadas se
# saltan con aviso, porque un output `path` dejaría sin PATH a quien sourcea.
__pe_exports_from() {
  jq -r '
    ["PATH","HOME","IFS","PS1","SHELL","USER","PWD","TMPDIR","ENV","BASH_SOURCE","LD_PRELOAD","LD_LIBRARY_PATH"] as $res
    | to_entries[]
    | select(.key | test("^[A-Za-z_][A-Za-z0-9_]*$"))
    | (.key | ascii_upcase) as $k
    | (if (.value.value | type) == "string" then .value.value else (.value.value | tojson) end) as $v
    | if ($res | index($k))
      then "__pe_warn " + (("output " + .key + ": ignorado, pisaría $" + $k) | @sh)
      else "export \($k)=\($v | @sh)"
      end
  ' "$1"
}

__pe_load_terraform() {
  __pe_require jq aws || return 1
  command -v terraform >/dev/null 2>&1 || __pe_die "falta terraform; probá PROD_ENV_SOURCE=discover" || return 1
  # Antes de §4-§5 no hay composición ni state que leer. Avisar y seguir: los comandos de esas
  # secciones no usan ninguna variable del contrato, así que el `source` no tiene que romperse.
  if [ ! -d "$INFRA_DIR" ]; then
    __pe_partial "todavía no existe $INFRA_DIR (lo crea §3.0 y lo llenan §4-§5)"
    return 0
  fi
  if [ ! -d "$INFRA_DIR/.terraform" ]; then
    __pe_partial "$INFRA_DIR existe pero sin inicializar (falta 'task infra:init', §4)"
    return 0
  fi
  # Una caché por INFRA_DIR: dos entornos no pueden pisarse el contexto.
  local clave cache exports
  clave="$(printf '%s' "$INFRA_DIR" | cksum | cut -d' ' -f1)"
  cache="${TMPDIR:-/tmp}/pyspark-stack-prod-env.$(id -u).$clave.json"
  # `terraform output` baja el state de S3 en cada llamada (~1-2 s): de ahí el TTL.
  PROD_ENV_AGE="$(__pe_age_of "$cache")"
  if [ ! -s "$cache" ] || [ -n "${PROD_ENV_REFRESH:-}" ] || [ "$PROD_ENV_AGE" -ge "$PROD_ENV_TTL" ]; then
    __pe_fetch_outputs "$cache"
    case $? in
      0) PROD_ENV_AGE=0 ;;
      2) __pe_partial "el state no publicó outputs todavía (los crea el primer apply de §5)"
         return 0 ;;
      *) return 1 ;;
    esac
  fi
  export PROD_ENV_AGE
  exports="$(__pe_exports_from "$cache")" || __pe_die "no pude parsear los outputs" || return 1
  eval "$exports"
}

# ─── carga: discover (sin Terraform, guía 02b; mismos nombres de variable) ─────
__pe_load_discover() {
  __pe_require aws || return 1
  export NAME_PREFIX="${NAME_PREFIX:-pyspark-stack}"
  ACCOUNT_ID="${ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}" \
    || __pe_die "sin credenciales AWS válidas (aws configure)" || return 1
  export ACCOUNT_ID
  # Un solo describe-instances trae id e IP. Con el id ya dado, la IP sale de ESA instancia.
  local -a filtro=(--filters "Name=tag:Name,Values=${NAME_PREFIX}-node"
                             "Name=instance-state-name,Values=pending,running,stopping,stopped")
  if [ -n "${INSTANCE_ID:-}" ]; then
    filtro=(--instance-ids "$INSTANCE_ID")
  fi
  local ec2
  ec2="$(aws ec2 describe-instances "${filtro[@]}" \
    --query 'Reservations[0].Instances[0].[InstanceId,PublicIpAddress]' --output text 2>/dev/null)"
  export INSTANCE_ID="${INSTANCE_ID:-$(printf '%s\n' "$ec2" | awk '{print $1}')}"
  export PUBLIC_IP="${PUBLIC_IP:-$(printf '%s\n' "$ec2" | awk '{print $2}')}"
  export SECURITY_GROUP_ID="${SECURITY_GROUP_ID:-$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=${NAME_PREFIX}-sg" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)}"
  export EMR_APP_ID="${EMR_APP_ID:-$(aws emr-serverless list-applications \
    --query "applications[?name=='${NAME_PREFIX}-spark'].id | [0]" --output text 2>/dev/null)}"
  export SQS_TRIGGER_QUEUE_URL="${SQS_TRIGGER_QUEUE_URL:-$(aws sqs get-queue-url \
    --queue-name "${NAME_PREFIX}-trigger-events" --query QueueUrl --output text 2>/dev/null)}"
  export DATALAKE_BUCKET="${NAME_PREFIX}-datalake-${ACCOUNT_ID}"
  export ARTIFACTS_BUCKET="${NAME_PREFIX}-artifacts-${ACCOUNT_ID}"
  export EMR_JOB_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${NAME_PREFIX}-emr-serverless-job"
  export LAMBDA_STARTSTOP_NAME="${NAME_PREFIX}-startstop"
  export LAMBDA_TRIGGER_NAME="${NAME_PREFIX}-trigger-airflow"
  export GLUE_DATABASE="${NAME_PREFIX//-/_}_analytics"
  export ATHENA_WORKGROUP="${NAME_PREFIX}-analytics"
}

# ─── ejecución ────────────────────────────────────────────────────────────────
# Se limpia ANTES de cargar: sourcear dos veces en la misma terminal —una antes del apply y
# otra después— dejaría el flag viejo pegado y `--check` diría "parcial" con el contexto
# completo. El estado lo decide esta carga, no la anterior.
unset PROD_ENV_PARTIAL

# `exit` dentro de un `source` mataría la terminal de quien lo cargó: por eso el `case`
# entero decide en un solo lugar cómo abortar.
case "$PROD_ENV_SOURCE" in
  terraform) __pe_load_terraform ;;
  discover)  __pe_load_discover ;;
  *)         __pe_die "PROD_ENV_SOURCE debe ser 'terraform' o 'discover'" ;;
esac || { [ "$__pe_sourced" -eq 1 ] && return 1 || exit 1; }

export AWS_DEFAULT_REGION="$AWS_REGION"   # `aws_region` es un output: si cambió, el CLI la sigue

# Derivadas, solo si su base existe: si no, quedarían rutas rotas tipo "s3:///emr/" en las
# secciones que todavía no se aplicaron.
if [ -n "${ARTIFACTS_BUCKET:-}" ]; then
  export EMR_ENTRYPOINTS_URI="s3://${ARTIFACTS_BUCKET}/emr"
  export EMR_LOGS_URI="s3://${ARTIFACTS_BUCKET}/emr/logs"
  export ATHENA_RESULTS_URI="s3://${ARTIFACTS_BUCKET}/athena-results"
fi
if [ -n "${DATALAKE_BUCKET:-}" ]; then
  export RAW_URI="s3://${DATALAKE_BUCKET}/raw"
  export CURATED_URI="s3://${DATALAKE_BUCKET}/curated"
fi
if [ -n "${PUBLIC_IP:-}" ]; then
  export SSH_TARGET="${SSH_USER}@${PUBLIC_IP}"
  export SSH="ssh -i $SSH_KEY"
  export RSYNC_SSH="$SSH"                 # rsync lo quiere como string en -e
fi

# ─── chequeo ──────────────────────────────────────────────────────────────────
# Una variable pasa a `required` recién cuando su sección ya se aplicó; el resto avisa nomás.
prod_env_check() {
  local required="AWS_REGION NAME_PREFIX ACCOUNT_ID INSTANCE_ID PUBLIC_IP"
  local optional="DATALAKE_BUCKET ARTIFACTS_BUCKET EMR_APP_ID SQS_TRIGGER_QUEUE_URL AIRFLOW_URL"
  local missing="" v origen="lectura fresca del state"
  local neg='' dim='' roj='' ver='' amb='' fin=''
  if [ -t 1 ]; then
    neg=$'\033[1m'; dim=$'\033[2m'; roj=$'\033[31m'
    ver=$'\033[32m'; amb=$'\033[33m'; fin=$'\033[0m'
  fi
  for v in $required; do
    [ -n "${!v:-}" ] && [ "${!v}" != "None" ] || missing="$missing $v"
  done
  if [ "${PROD_ENV_AGE:-0}" -gt 0 ]; then
    origen="caché de hace ${PROD_ENV_AGE}s — si aplicaste después, recargá con PROD_ENV_REFRESH=1"
  fi
  printf '%sContexto de producción%s  (fuente: %s · región: %s)\n' "$neg" "$fin" "$PROD_ENV_SOURCE" "$AWS_REGION"
  if [ -n "${PROD_ENV_PARTIAL:-}" ]; then
    printf '  %sparcial: la infra todavía no está aplicada%s\n' "$dim" "$fin"
  elif [ "$PROD_ENV_SOURCE" = "terraform" ]; then
    printf '  %s%s%s\n' "$dim" "$origen" "$fin"
  fi
  for v in $required $optional; do
    printf '  %-24s %s\n' "$v" "${!v:-— (sin definir aún: falta su sección)}"
  done
  if [ ! -f "$SSH_KEY" ]; then
    printf '\n%s  aviso: no existe la clave SSH %s (definila con SSH_KEY=...)%s\n' "$amb" "$SSH_KEY" "$fin"
  fi
  if [ -n "$missing" ]; then
    if [ -n "${PROD_ENV_PARTIAL:-}" ]; then
      # Esperado hasta el apply de §5: el gate sigue en 1, pero sin dar a entender que rompiste algo.
      printf '\n%s  sin aplicar todavía:%s — normal antes de §5; seguí la guía y recargá después del apply%s\n' \
        "$amb" "$missing" "$fin"
    else
      printf '\n%s  faltan obligatorias:%s%s\n' "$roj" "$missing" "$fin"
    fi
    return 1
  fi
  printf '\n%s  ok: contexto completo%s\n' "$ver" "$fin"
}

# Ejecutado: el estado del chequeo es el del script — `task release:check` lo usa de gate.
if [ "$__pe_sourced" -eq 0 ]; then
  prod_env_check
  exit $?
fi

# Sourceado: solo sobrevive `prod_env_check`; el resto ensuciaría la shell de quien lo cargó.
unset __pe_self __pe_sourced
unset -f __pe_msg __pe_die __pe_warn __pe_partial __pe_require __pe_age_of \
         __pe_fetch_outputs __pe_exports_from __pe_load_terraform __pe_load_discover

# Último comando a propósito: `source ... --check && algo` encadena con el chequeo.
if [ "${1:-}" = "--check" ]; then
  prod_env_check
fi
