#!/usr/bin/env python3
"""Gate local: rechaza secretos débiles antes de arrancar Docker Compose."""

from __future__ import annotations

import os
import stat
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT / ".env"
REQUIREMENTS = {
    "POSTGRES_PASSWORD": (24, {"airflow", "postgres", "password"}),
    "AIRFLOW_JWT_SECRET": (32, {"change-me-in-prod", "secret"}),
    "AIRFLOW_ADMIN_PASSWORD": (24, {"admin", "airflow", "password"}),
    "JUPYTER_TOKEN": (32, {"admin", "jupyter", "token"}),
}


def read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.split("#", 1)[0].strip()
    return values


def main() -> int:
    if not ENV_FILE.is_file():
        raise SystemExit("Falta .env: copie .env.example y genere los cuatro secretos")
    mode = stat.S_IMODE(ENV_FILE.stat().st_mode)
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise SystemExit(f".env debe ser privado (chmod 600); modo actual: {mode:o}")

    values = read_env(ENV_FILE)
    failures: list[str] = []
    for key, (minimum, weak_values) in REQUIREMENTS.items():
        value = values.get(key, "")
        if len(value) < minimum:
            failures.append(f"{key}: mínimo {minimum} caracteres")
        elif value.lower() in weak_values:
            failures.append(f"{key}: valor conocido no permitido")
    if failures:
        raise SystemExit("Entorno local inseguro:\n- " + "\n- ".join(failures))

    print("Entorno local: secretos presentes, no triviales y .env privado")
    return os.EX_OK


if __name__ == "__main__":
    raise SystemExit(main())
