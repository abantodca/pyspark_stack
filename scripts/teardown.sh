#!/usr/bin/env bash
# scripts/teardown.sh — teardown completo y REVERSIBLE (§8.5). Saca las 3 guardas anti-destroy
# SOLO para esta corrida, destruye, y las repone al final (éxito o error) — así el repo vuelve a
# quedar protegido contra un `destroy` accidental futuro. No edites ec2.tf/s3.tf/bootstrap/main.tf
# a mano para esto: correlo a él, así no se cruza con tu trabajo normal de `apply`.
#
# Uso:
#   ./scripts/teardown.sh              # destruye infra/prod (deja el bucket del tfstate)
#   ./scripts/teardown.sh --all        # además destruye infra/bootstrap (el bucket del tfstate)
#   ./scripts/teardown.sh --restore    # si quedó a mitad de camino: repone las 3 guardas y sale
#   Agregá -y/--yes para saltear la confirmación (uso no interactivo).
set -euo pipefail
cd "$(dirname "$0")/.."

DESTROY_BOOTSTRAP=false
AUTO_YES=false
RESTORE_ONLY=false
for arg in "$@"; do
  case "$arg" in
    --all) DESTROY_BOOTSTRAP=true ;;
    -y|--yes) AUTO_YES=true ;;
    --restore) RESTORE_ONLY=true ;;
    *) echo "Argumento desconocido: $arg" >&2; exit 1 ;;
  esac
done

EC2_TF="infra/prod/ec2.tf"
S3_TF="infra/prod/s3.tf"
BOOTSTRAP_TF="infra/bootstrap/main.tf"

# --- toggle de las guardas: reemplazos de texto exacto, uno-a-uno, verificados (falla si no matchea
# exactamente 1 vez — mejor que corromper el archivo en silencio si alguien lo editó después). ---
toggle() {
  local direction="$1"   # off = sacar guardas | on = reponerlas
  python3 - "$direction" "$EC2_TF" "$S3_TF" "$BOOTSTRAP_TF" <<'PY'
import sys

direction, ec2_tf, s3_tf, bootstrap_tf = sys.argv[1:5]

def swap(path, guarded, unguarded, to):
    with open(path, encoding="utf-8") as f:
        text = f.read()
    src, dst = (guarded, unguarded) if to == "off" else (unguarded, guarded)
    n = text.count(src)
    if n == 0:
        if text.count(dst) == 1:
            return  # ya está en el estado pedido (idempotente)
        raise SystemExit(f"{path}: no encontré el bloque esperado — revisalo a mano (¿se editó el archivo?)")
    if n > 1:
        raise SystemExit(f"{path}: el bloque esperado aparece {n} veces, esperaba 1 — abortando por seguridad")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text.replace(src, dst))

EC2_GUARDED = '''  lifecycle {
    prevent_destroy = true # el disco de estado (Postgres/Prometheus/Loki) NO se borra por accidente
  }
}
resource "aws_volume_attachment" "data" {'''
EC2_UNGUARDED = '''  # lifecycle { prevent_destroy = true }   # <- sacado por scripts/teardown.sh (temporal)
}
resource "aws_volume_attachment" "data" {'''
swap(ec2_tf, EC2_GUARDED, EC2_UNGUARDED, direction)

S3_GUARDED = '''resource "aws_s3_bucket" "datalake" {
  bucket = local.datalake
  # OJO con la semántica: prevent_destroy NO saltea este recurso, ABORTA el `terraform destroy`
  # entero. Para el teardown hay que borrar esta línea primero. Y aun así el destroy falla con
  # BucketNotEmpty: el bucket tiene versionado, así que además de vaciarlo hay que borrar las
  # versiones y los delete markers (o agregar `force_destroy = true`). Usá `scripts/teardown.sh`
  # (§8.5) en vez de editar esto a mano: lo hace y lo revierte solo.
  lifecycle { prevent_destroy = true }
}
resource "aws_s3_bucket" "artifacts" { bucket = local.artifacts }'''
S3_UNGUARDED = '''resource "aws_s3_bucket" "datalake" {
  bucket = local.datalake
  # lifecycle { prevent_destroy = true }   # <- sacado por scripts/teardown.sh (temporal)
  force_destroy = true                     # <- ídem, para poder vaciar el versionado (temporal)
}
resource "aws_s3_bucket" "artifacts" {
  bucket        = local.artifacts
  force_destroy = true                     # <- sacado/puesto por scripts/teardown.sh (temporal)
}'''
swap(s3_tf, S3_GUARDED, S3_UNGUARDED, direction)

BOOT_GUARDED = '''resource "aws_s3_bucket" "tfstate" {
  bucket = local.state_bucket
  lifecycle { prevent_destroy = true }
}'''
BOOT_UNGUARDED = '''resource "aws_s3_bucket" "tfstate" {
  bucket        = local.state_bucket
  # lifecycle { prevent_destroy = true }   # <- sacado por scripts/teardown.sh (temporal)
  force_destroy = true                     # <- ídem, para poder vaciar el versionado (temporal)
}'''
swap(bootstrap_tf, BOOT_GUARDED, BOOT_UNGUARDED, direction)
PY
}

restore() {
  echo "→ Reponiendo las 3 guardas anti-destroy (prevent_destroy + force_destroy)..."
  toggle on
  echo "  listo — ec2.tf / s3.tf / bootstrap/main.tf quedaron como antes de correr esto."
}

if $RESTORE_ONLY; then
  restore
  exit 0
fi

trap restore EXIT

echo "Esto va a DESTRUIR infra/prod (EC2 + EBS /data + S3 datalake/artifacts + EMR Serverless + Lambdas + red)$($DESTROY_BOOTSTRAP && echo ' y también infra/bootstrap (bucket del tfstate)')."
if ! $AUTO_YES; then
  read -r -p "Escribí 'destruir' para confirmar: " CONFIRM
  [ "$CONFIRM" = "destruir" ] || { echo "Cancelado."; trap - EXIT; exit 1; }
fi

echo "→ Cancelando job runs de EMR Serverless activos (si hay)..."
APP_ID=$(terraform -chdir=infra/prod output -raw emr_app_id 2>/dev/null || true)
if [ -n "$APP_ID" ]; then
  for JOB_ID in $(aws emr-serverless list-job-runs --application-id "$APP_ID" \
      --states RUNNING PENDING SCHEDULED --query 'jobRuns[].id' --output text 2>/dev/null); do
    echo "  cancelando job $JOB_ID"
    aws emr-serverless cancel-job-run --application-id "$APP_ID" --job-run-id "$JOB_ID"
  done
fi

echo "→ Sacando las guardas..."
toggle off

echo "→ terraform apply, ACOTADO a los buckets (solo actualiza force_destroy en el state — sin"
echo "  -target tocaría también cualquier otro cambio pendiente sin relación con el teardown)..."
terraform -chdir=infra/prod apply -auto-approve \
  -target=aws_s3_bucket.datalake -target=aws_s3_bucket.artifacts

echo "→ terraform destroy infra/prod..."
terraform -chdir=infra/prod destroy -auto-approve

# state list DE ACÁ, todavía con el backend de infra/bootstrap vivo: si se corre después de destruir
# bootstrap, el bucket que guarda ESTE state ya no existe y el comando falla con NoSuchBucket (no
# porque algo haya salido mal, sino porque le estás pidiendo leer un state cuyo backend ya se borró).
echo "  (state de infra/prod tras el destroy — vacío = destruido por completo)"
terraform -chdir=infra/prod state list

if $DESTROY_BOOTSTRAP; then
  echo "→ terraform destroy infra/bootstrap (bucket del tfstate)..."
  terraform -chdir=infra/bootstrap apply -auto-approve -target=aws_s3_bucket.tfstate
  terraform -chdir=infra/bootstrap destroy -auto-approve
fi

echo "→ Verificando en AWS directo (no vía terraform state: si tiraste --all, el backend de"
echo "  infra/prod ya no existe) que no quedó nada facturando..."
aws ec2 describe-instances --filters "Name=tag:Name,Values=pyspark-stack-node" \
  --query 'Reservations[].Instances[].State.Name' --output text
aws s3 ls | grep pyspark-stack || echo "  sin buckets pyspark-stack restantes"
aws emr-serverless list-applications --query "applications[?name=='pyspark-stack-spark']"

SNAPS=$(aws ec2 describe-snapshots --owner-ids self \
  --filters "Name=tag:Name,Values=pyspark-stack-data" --query 'Snapshots[].SnapshotId' --output text)
if [ -n "$SNAPS" ]; then
  echo "→ Snapshots del EBS /data que DLM creó (Terraform no los borra — quedan facturando poco pero"
  echo "  indefinidamente si no los tirás):"
  echo "$SNAPS"
  if $AUTO_YES; then
    for S in $SNAPS; do aws ec2 delete-snapshot --snapshot-id "$S"; done
    echo "  borrados."
  else
    read -r -p "  ¿Borrarlos ahora? [y/N] " DEL
    if [ "$DEL" = "y" ] || [ "$DEL" = "Y" ]; then
      for S in $SNAPS; do aws ec2 delete-snapshot --snapshot-id "$S"; done
      echo "  borrados."
    fi
  fi
fi

echo "Teardown completo."
