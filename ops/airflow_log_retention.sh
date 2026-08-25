#!/usr/bin/env bash
set -euo pipefail

readonly LOG_DIR="/opt/airflow/logs"
readonly RETENTION_DAYS="${AIRFLOW_LOCAL_LOG_RETENTION_DAYS:-30}"
readonly MAX_SIZE_MB="${AIRFLOW_LOCAL_LOG_MAX_SIZE_MB:-1024}"
readonly INTERVAL_MINUTES="${AIRFLOW_LOG_CLEANUP_INTERVAL_MINUTES:-15}"

for value in "$RETENTION_DAYS" "$MAX_SIZE_MB" "$INTERVAL_MINUTES"; do
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "log retention values must be positive integers" >&2
    exit 2
  fi
done

mkdir -p "$LOG_DIR"

directory_size_bytes() {
  du -sb "$LOG_DIR" | awk '{print $1}'
}

trim_to_size_limit() {
  local -r max_bytes=$((MAX_SIZE_MB * 1024 * 1024))
  local current_bytes
  current_bytes="$(directory_size_bytes)"
  (( current_bytes <= max_bytes )) && return 0

  while IFS= read -r -d '' entry; do
    local path="${entry#* }"
    rm -f -- "$path"
    current_bytes="$(directory_size_bytes)"
    (( current_bytes <= max_bytes )) && break
  done < <(find "$LOG_DIR" -type f -printf '%T@ %p\0' | sort -z -n)
}

prune_once() {
  find "$LOG_DIR" -type f -mtime "+$RETENTION_DAYS" -delete
  trim_to_size_limit
  find "$LOG_DIR" -depth -type d -empty -delete
}

case "${1:-}" in
  --once)
    prune_once
    exit 0
    ;;
  "") ;;
  *)
    echo "usage: $0 [--once]" >&2
    exit 2
    ;;
esac

while true; do
  prune_once
  sleep "$((INTERVAL_MINUTES * 60))"
done
