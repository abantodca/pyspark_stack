from __future__ import annotations

import json
from pathlib import Path

import pytest

import hdfs_io


def test_hdfs_path_must_be_absolute_and_cannot_traverse() -> None:
    assert hdfs_io._safe_hdfs_path("/customer_etl/input") == "/customer_etl/input"
    with pytest.raises(ValueError):
        hdfs_io._safe_hdfs_path("customer_etl/input")
    with pytest.raises(ValueError):
        hdfs_io._safe_hdfs_path("/customer_etl/../secrets")


def test_load_batch_replaces_partition_and_uploads_exact_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in ("customers.csv", "products.json", "orders.csv"):
        (tmp_path / name).write_text("test\n", encoding="utf-8")

    requests: list[tuple[str, str, str, dict[str, str]]] = []
    uploads: list[tuple[str, str]] = []

    def fake_request(path: str, operation: str, method: str, **params: str) -> bytes:
        requests.append((path, operation, method, params))
        return b""

    def fake_upload(local_file: Path, destination: str) -> None:
        uploads.append((local_file.name, destination))

    monkeypatch.setattr(hdfs_io, "_request", fake_request)
    monkeypatch.setattr(hdfs_io, "_upload", fake_upload)

    hdfs_io.load_batch("/customer_etl/input/run_date=2026-08-19", tmp_path)

    assert requests == [
        (
            "/customer_etl/input/run_date=2026-08-19",
            "DELETE",
            "DELETE",
            {"recursive": "true"},
        ),
        ("/customer_etl/input/run_date=2026-08-19", "MKDIRS", "PUT", {}),
    ]
    assert uploads == [
        (
            name,
            f"/customer_etl/input/run_date=2026-08-19/{name}",
        )
        for name in ("customers.csv", "products.json", "orders.csv")
    ]


def test_load_batch_rejects_incomplete_landing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        hdfs_io,
        "_request",
        lambda *args, **kwargs: pytest.fail("HDFS no debe tocarse"),
    )
    with pytest.raises(FileNotFoundError, match="faltan archivos"):
        hdfs_io.load_batch("/customer_etl/input/run_date=2026-08-19", tmp_path)


def test_prepare_output_delegates_only_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[tuple[str, str, str, dict[str, str]]] = []

    def fake_request(path: str, operation: str, method: str, **params: str) -> bytes:
        requests.append((path, operation, method, params))
        return b""

    monkeypatch.setattr(hdfs_io, "_request", fake_request)
    hdfs_io.prepare_output("/customer_etl/output/loyalty_snapshot_2026-08-19")

    assert requests == [
        ("/customer_etl/output", "MKDIRS", "PUT", {}),
        ("/customer_etl/output", "SETOWNER", "PUT", {"owner": "spark"}),
    ]
    with pytest.raises(ValueError, match="directorio dedicado"):
        hdfs_io.prepare_output("/snapshot.csv")


def test_output_requires_one_nonempty_part(monkeypatch: pytest.MonkeyPatch) -> None:
    valid = {
        "FileStatuses": {
            "FileStatus": [
                {"pathSuffix": "_SUCCESS", "length": 0},
                {"pathSuffix": "part-00000.csv", "length": 42},
            ]
        }
    }
    monkeypatch.setattr(
        hdfs_io,
        "_request",
        lambda *args, **kwargs: json.dumps(valid).encode(),
    )
    hdfs_io.validate_output("/customer_etl/output/loyalty_snapshot_2026-08-19")

    invalid = {
        "FileStatuses": {
            "FileStatus": [{"pathSuffix": "part-00000.csv", "length": 0}]
        }
    }
    monkeypatch.setattr(
        hdfs_io,
        "_request",
        lambda *args, **kwargs: json.dumps(invalid).encode(),
    )
    with pytest.raises(RuntimeError, match="salida inválida"):
        hdfs_io.validate_output("/customer_etl/output/loyalty_snapshot_2026-08-19")
