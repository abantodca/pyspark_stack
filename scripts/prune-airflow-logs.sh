#!/usr/bin/env bash
set -euo pipefail

log_dir="${AIRFLOW_LOG_DIR:-/opt/airflow/logs}"
retention_days="${AIRFLOW_LOCAL_LOG_RETENTION_DAYS:-30}"
max_size_mb="${AIRFLOW_LOCAL_LOG_MAX_SIZE_MB:-1024}"
interval_minutes="${AIRFLOW_LOG_CLEANUP_INTERVAL_MINUTES:-15}"

# Este proceso borra archivos: acepte solo los roots previstos y nunca siga un enlace simbolico.
case "$log_dir" in
  /opt/airflow/logs|/data/airflow-logs|/tmp/pyspark-stack-log-*) ;;
  *) echo "AIRFLOW_LOG_DIR fuera de los roots permitidos: $log_dir" >&2; exit 2 ;;
esac
if [[ -L "$log_dir" ]]; then
  echo "AIRFLOW_LOG_DIR no puede ser un enlace simbolico: $log_dir" >&2
  exit 2
fi

case "$retention_days" in
  ''|*[!0-9]*) echo "AIRFLOW_LOCAL_LOG_RETENTION_DAYS debe ser un entero >= 1" >&2; exit 2 ;;
esac
case "$max_size_mb" in
  ''|*[!0-9]*) echo "AIRFLOW_LOCAL_LOG_MAX_SIZE_MB debe ser un entero >= 1" >&2; exit 2 ;;
esac
case "$interval_minutes" in
  ''|*[!0-9]*) echo "AIRFLOW_LOG_CLEANUP_INTERVAL_MINUTES debe ser un entero >= 1" >&2; exit 2 ;;
esac
if (( retention_days < 1 || max_size_mb < 1 || interval_minutes < 1 )); then
  echo "La retencion, el tamano maximo y el intervalo deben ser >= 1" >&2
  exit 2
fi
max_size_bytes=$((max_size_mb * 1024 * 1024))

prune_to_size() {
  local current_size entry file file_size
  current_size="$(du -sb "$log_dir" | awk '{print $1}')"
  (( current_size <= max_size_bytes )) && return 0

  # NUL como separador mantiene seguros los paths con espacios; se eliminan los mas antiguos.
  while IFS= read -r -d '' entry; do
    file="${entry#*$'\t'}"
    file_size="$(stat -c %s -- "$file" 2>/dev/null || printf '0')"
    rm -f -- "$file"
    current_size=$((current_size - file_size))
    (( current_size <= max_size_bytes )) && break
  done < <(find "$log_dir" -xdev -type f -printf '%T@\t%p\0' | sort -z -n)
}

prune_once() {
  mkdir -p "$log_dir"
  find "$log_dir" -xdev -type f -mtime "+$retention_days" -delete
  prune_to_size
  find "$log_dir" -xdev -mindepth 1 -depth -type d -empty -delete
}

prune_once
if [[ "${1:-}" == "--once" ]]; then
  exit 0
fi

while true; do
  sleep "${interval_minutes}m"
  prune_once
done
