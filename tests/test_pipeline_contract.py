from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).parents[1]


def test_shell_pipeline_is_fail_fast() -> None:
    script = ROOT / "spark-apps/customer_etl/shell/customer_etl_job_airflow.sh"
    lines = script.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "#!/usr/bin/env bash"
    assert "set -Eeuo pipefail" in lines[:5]
    source = "\n".join(lines)
    assert "hdfs_io.py" in source
    assert 'prepare-output "$HDFS_OUTPUT"' in source
    assert 'HADOOP_USER_NAME="${HDFS_OUTPUT_USER:-spark}"' in source
    assert "hdfs dfs" not in source.split("else", 1)[0]
    assert "docker exec spark-master /opt/spark/bin/spark-submit" in source
    assert 'hdfs dfs -chown spark "$HDFS_OUTPUT_PARENT"' in source
    assert "mktemp -d" in source
    assert 'mv -f -- "$TEMP_CSV" "$FINAL_CSV"' in source


def test_env_contract_uses_supplied_run_date() -> None:
    config = ROOT / "spark-apps/customer_etl/config/env.sh"
    command = (
        f'source "{config}" dev 2026-08-19 && '
        "printf '%s|%s|%s' \"$RUN_DATE\" \"$HDFS_INPUT\" \"$HDFS_OUTPUT\""
    )
    result = subprocess.run(
        ["bash", "-c", command], check=True, capture_output=True, text=True
    )
    assert result.stdout == (
        "2026-08-19|/customer_etl/input/run_date=2026-08-19|"
        "/customer_etl/output/loyalty_snapshot_2026-08-19"
    )


def test_env_contract_rejects_unknown_environment() -> None:
    config = ROOT / "spark-apps/customer_etl/config/env.sh"
    result = subprocess.run(
        ["bash", "-c", f'source "{config}" qa 2026-08-19'],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "env debe ser" in result.stderr


def test_env_contract_rejects_impossible_date() -> None:
    config = ROOT / "spark-apps/customer_etl/config/env.sh"
    result = subprocess.run(
        ["bash", "-c", f'source "{config}" dev 2026-02-30'],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "run_date debe" in result.stderr
