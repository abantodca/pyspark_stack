#!/usr/bin/env python3
"""Operaciones mínimas WebHDFS para el pipeline local, sin instalar el CLI completo."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path, PurePosixPath
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen


BASE_URL = os.environ.get("HDFS_WEB_URL", "http://hdfs-namenode:9870").rstrip("/")
USER = os.environ.get("HDFS_USER", "root")
OUTPUT_USER = os.environ.get("HDFS_OUTPUT_USER", "spark")


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


def _safe_hdfs_path(value: str) -> str:
    path = PurePosixPath(value)
    if not value.startswith("/") or ".." in path.parts:
        raise ValueError("la ruta HDFS debe ser absoluta y no contener '..'")
    return str(path)


def _url(path: str, operation: str, **params: str) -> str:
    query = urlencode({"op": operation, "user.name": USER, **params})
    return f"{BASE_URL}/webhdfs/v1{quote(_safe_hdfs_path(path), safe='/')}?{query}"


def _request(path: str, operation: str, method: str, **params: str) -> bytes:
    request = Request(
        _url(path, operation, **params),
        data=b"" if method in {"PUT", "POST"} else None,
        method=method,
    )
    with urlopen(request, timeout=30) as response:  # noqa: S310 - URL interna controlada
        return response.read()


def _upload(local_file: Path, destination: str) -> None:
    initial = Request(
        _url(destination, "CREATE", overwrite="true"), data=b"", method="PUT"
    )
    try:
        build_opener(_NoRedirect).open(initial, timeout=30)
    except HTTPError as error:
        if error.code not in {307, 308} or not error.headers.get("Location"):
            raise
        target = error.headers["Location"]
    else:
        raise RuntimeError("WebHDFS CREATE no devolvió la redirección esperada")

    request = Request(target, data=local_file.read_bytes(), method="PUT")
    with urlopen(request, timeout=60) as response:  # noqa: S310 - redirect de WebHDFS
        response.read()


def load_batch(hdfs_directory: str, landing_directory: Path) -> None:
    required = ("customers.csv", "products.json", "orders.csv")
    missing = [name for name in required if not (landing_directory / name).is_file()]
    if missing:
        raise FileNotFoundError(f"faltan archivos de landing: {missing}")

    try:
        _request(hdfs_directory, "DELETE", "DELETE", recursive="true")
    except HTTPError as error:
        if error.code != 404:
            raise
    _request(hdfs_directory, "MKDIRS", "PUT")
    for name in required:
        _upload(landing_directory / name, f"{hdfs_directory}/{name}")


def prepare_output(hdfs_directory: str) -> None:
    """Crea el padre de salida y delega únicamente ese espacio al usuario Spark."""
    output = PurePosixPath(_safe_hdfs_path(hdfs_directory))
    parent = str(output.parent)
    if parent == "/":
        raise ValueError("la salida HDFS debe estar dentro de un directorio dedicado")
    _request(parent, "MKDIRS", "PUT")
    _request(parent, "SETOWNER", "PUT", owner=OUTPUT_USER)


def validate_output(hdfs_directory: str) -> None:
    payload = json.loads(_request(hdfs_directory, "LISTSTATUS", "GET"))
    statuses = payload.get("FileStatuses", {}).get("FileStatus", [])
    parts = [item for item in statuses if item.get("pathSuffix", "").startswith("part-")]
    if len(parts) != 1 or int(parts[0].get("length", 0)) <= 0:
        raise RuntimeError(
            f"salida inválida: se esperaba un part-* no vacío y se obtuvo {parts}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    load = subparsers.add_parser("load")
    load.add_argument("hdfs_directory")
    load.add_argument("landing_directory", type=Path)
    prepare = subparsers.add_parser("prepare-output")
    prepare.add_argument("hdfs_directory")
    validate = subparsers.add_parser("validate-output")
    validate.add_argument("hdfs_directory")
    args = parser.parse_args()

    if args.command == "load":
        load_batch(args.hdfs_directory, args.landing_directory)
    elif args.command == "prepare-output":
        prepare_output(args.hdfs_directory)
    else:
        validate_output(args.hdfs_directory)


if __name__ == "__main__":
    main()
