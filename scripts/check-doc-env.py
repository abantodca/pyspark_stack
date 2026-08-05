#!/usr/bin/env python3
"""Valida el contrato de variables de entorno de las guías (02 §3.1).

Comprueba siete invariantes que rompen una guía copy-paste e incremental:

1.  ORDEN: ninguna variable se usa en un comando ANTES de la sección que la crea.
    Es el error más caro de esta guía: el bloque se copia, la variable está vacía
    y el comando corre contra el lugar equivocado en vez de fallar.
2.  NOMBRES: toda variable usada existe — es un `output` de Terraform declarado en
    la guía, la define `scripts/prod-env.sh`, o se asigna en el mismo bloque.
3.  FENCES: los ``` abren y cierran en pares (un fence roto se come el resto del doc).
4.  SINTAXIS: cada bloque bash parsea con `bash -n`. Detecta prosa que quedó dentro de
    un bloque de código y comillas o heredocs desbalanceados por una edición.
5.  LITERALES: ningún comando lleva un nombre de recurso, un account id, una IP ni
    una ruta SSH escritos a mano. Es la promesa central del contrato: si vuelve a
    aparecer un `<acct>` o un `pyspark-stack-datalake-...`, el bloque dejó de ser
    copy-paste y falla —o peor, corre contra la cuenta equivocada—.
6.  INVENTARIO `.env`: toda variable que el Compose de producción interpola figura en
    la tabla de §13.4, que dice qué sección la publica. Sin esto, agregar una
    variable al Compose y olvidarse de publicarla en SSM rompe el arranque.
7.  DERIVA: el prod-env.sh embebido en la guía exporta lo mismo que el del repo.

Uso:  python3 scripts/check-doc-env.py
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOADER = ROOT / "scripts" / "prod-env.sh"

# Bloques que la guía muestra pero que NO corren en tu terminal con el contexto
# cargado: código que se ejecuta en la EC2, en CI, o dentro de un heredoc remoto.
SHELL_LANGS = {"bash", "sh", "shell"}

# Variables del entorno POSIX o del runner de CI: no salen del contrato.
AMBIENT = {
    "HOME", "PATH", "USER", "PWD", "SHELL", "TMPDIR", "LANG", "TERM", "UID",
    "BASH_SOURCE", "FUNCNAME", "IFS", "OSTYPE", "RANDOM", "SECONDS", "PS1",
    "GITHUB_OUTPUT", "GITHUB_ENV", "GITHUB_SHA", "GITHUB_REF", "RUNNER_OS",
    "AWS_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
    "AWS_DEFAULT_REGION", "AWS_PAGER", "EDITOR", "COMMIT_SHA",
}

# Documenta código histórico tal cual se escribió; no participa del contrato.
SKIP_DOCS = {"06-historial-de-incidentes.md"}

OUTPUT_RE = re.compile(r'^\s*output\s+"([a-z0-9_]+)"', re.M)
EXPORT_RE = re.compile(r"^\s*export\s+([A-Z_][A-Z0-9_]*)=", re.M)
# `${VAR}` o `$VAR`. El grupo 2 captura el operador que sigue al nombre dentro de
# `${...}`: si es `:-`, `:=`, `:?` o `-`, la expresión trae su propio default y no
# depende del contrato (`${PARAMETER_PREFIX:-/pyspark-stack}`).
USE_RE = re.compile(
    r"\$\{([A-Za-z_][A-Za-z0-9_]*)(:?[-=?+])?[^}]*\}"
    r"|\$([A-Za-z_][A-Za-z0-9_]*)"
)
# Asignaciones dentro del propio bloque: VAR=..., export VAR=..., for VAR in, read -r VAR.
# Sin anclar a inicio de línea: en la guía hay asignaciones encadenadas con && dentro
# de comandos remotos.
ASSIGN_RE = re.compile(
    r"(?:^|[;&|(]\s*|^\s*)(?:export\s+|local\s+|declare\s+)?([A-Za-z_][A-Za-z0-9_]*)="
    r"|\bfor\s+([A-Za-z_][A-Za-z0-9_]*)\s+in\b"
    r"|\bread\s+(?:-r\s+)?(?:[A-Za-z_][A-Za-z0-9_]*\s+)*([A-Za-z_][A-Za-z0-9_]*)",
    re.M,
)
# Las guías embeben bloques dentro de citas (> ```bash). Hay que sacar el prefijo
# antes de analizar, o toda asignación citada parece inexistente.
QUOTE_RE = re.compile(r"^\s*>\s?", re.M)

# Literales que el contrato prohíbe dentro de un comando. La clave es el motivo que
# se le muestra a quien rompa la regla, no solo el patrón.
FORBIDDEN = [
    (re.compile(r"pyspark-stack-(datalake|artifacts|spark|node|startstop|trigger|sg|"
                r"scheduler|analytics|daily|emr|key|ec2)"),
     "nombre de recurso literal: usá la variable del output (rompe si cambia name_prefix)"),
    (re.compile(r"<acct>|<account-id>|<elastic-ip>|<tu-elastic-ip>|<emr-app-id>|<i-x"),
     "placeholder sin resolver: si se copia tal cual, ejecuta contra un destino inválido"),
    (re.compile(r"ec2-user@|-i\s+~/\.ssh/"),
     "usuario o clave SSH literal: usá \"$SSH_TARGET\" y $SSH"),
    (re.compile(r"\baws sts get-caller-identity --query Account\b"),
     "el account id ya viene en $ACCOUNT_ID: no lo vuelvas a resolver por comando"),
]


def loader_vars(text: str) -> set[str]:
    """Variables que prod-env.sh deja disponibles."""
    names = set(EXPORT_RE.findall(text))
    # `NAME="$(...)"` seguido de `export NAME` (patrón usado para ACCOUNT_ID).
    names |= set(re.findall(r"^\s*export\s+([A-Z_][A-Z0-9_]*)\s*$", text, re.M))
    return names


def iter_code_blocks(text: str, path: Path) -> tuple[list[tuple[int, str, str]], list[str]]:
    """Devuelve (bloques, errores_de_fence). Bloque = (línea_inicio, lang, cuerpo)."""
    blocks: list[tuple[int, str, str]] = []
    errors: list[str] = []
    lines = text.split("\n")
    i = 0
    while i < len(lines):
        stripped = lines[i].lstrip("> ").rstrip()
        if stripped.startswith("```"):
            lang = stripped[3:].strip().lower()
            start = i + 1
            body: list[str] = []
            i += 1
            closed = False
            while i < len(lines):
                inner = lines[i].lstrip("> ").rstrip()
                if inner == "```":
                    closed = True
                    break
                body.append(lines[i])
                i += 1
            if not closed:
                errors.append(f"{path.name}:{start}: fence ``` abierto y nunca cerrado (lang={lang or 'sin lang'})")
                break
            blocks.append((start, lang, "\n".join(body)))
        i += 1
    return blocks, errors


def main() -> int:
    if not LOADER.exists():
        print(f"falta {LOADER.relative_to(ROOT)}: el contrato no existe")
        return 1

    loader_text = LOADER.read_text(encoding="utf-8")
    provided = loader_vars(loader_text) | AMBIENT
    problems: list[str] = []

    for doc in sorted((ROOT / "docs").glob("*.md")):
        if doc.name in SKIP_DOCS:
            continue
        text = doc.read_text(encoding="utf-8")
        blocks, fence_errors = iter_code_blocks(text, doc)
        problems.extend(fence_errors)

        # Línea en la que cada output queda declarado en ESTE documento.
        declared: dict[str, int] = {}
        for match in OUTPUT_RE.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            declared.setdefault(match.group(1).upper(), line)

        # El bloque que contiene el propio prod-env.sh define, no consume.
        for start, lang, body in blocks:
            if lang not in SHELL_LANGS:
                continue
            if "prod-env.sh — única fuente de verdad" in body or "__pe_load_terraform" in body:
                continue

            body = QUOTE_RE.sub("", body)

            # `bash -n` parsea sin ejecutar. Un bloque con prosa adentro (fence mal
            # cerrado) o una comilla suelta falla acá, antes de que alguien lo copie.
            check = subprocess.run(
                ["bash", "-n"], input=body, text=True, capture_output=True
            )
            if check.returncode:
                detail = check.stderr.strip().splitlines()
                problems.append(
                    f"{doc.name}:{start}: el bloque bash no parsea — "
                    f"{detail[-1] if detail else 'error de sintaxis'}"
                )

            # Literales prohibidos. Se ignoran los comentarios: ahí un nombre
            # concreto suele ser justamente la explicación de por qué no va suelto.
            for offset, raw in enumerate(body.split("\n")):
                if raw.lstrip().startswith("#"):
                    continue
                for pattern, reason in FORBIDDEN:
                    if pattern.search(raw):
                        problems.append(
                            # start es la línea del ```; el cuerpo empieza en la siguiente.
                            f"{doc.name}:{start + 1 + offset}: literal en un comando — {reason}\n"
                            f"      {raw.strip()[:96]}"
                        )
                        break

            local_names = {n for group in ASSIGN_RE.findall(body) for n in group if n}

            # Un $VAR dentro de un comentario es documentación, no una ejecución: no puede
            # romper nada y suele ser justamente la explicación de por qué la variable importa.
            # Se neutraliza conservando la longitud, para no descolocar los números de línea.
            scan = "\n".join(
                "" if raw.lstrip().startswith("#") else raw for raw in body.split("\n")
            )
            for match in USE_RE.finditer(scan):
                name = match.group(1) or match.group(3)
                has_default = bool(match.group(2))
                if not name or not name.isupper() or has_default:
                    continue
                if name in local_names or name in provided:
                    continue
                line = start + scan.count("\n", 0, match.start()) + 1
                if name not in declared:
                    problems.append(
                        f"{doc.name}:{line}: ${name} no existe: ni output declarado, "
                        f"ni exportada por prod-env.sh, ni asignada en el bloque"
                    )
                elif declared[name] > line:
                    problems.append(
                        f"{doc.name}:{line}: ${name} se usa ANTES de declararse "
                        f"(su output aparece recién en la línea {declared[name]}) — "
                        f"al copiar el bloque estaría vacía"
                    )

    # 6 — inventario del .env contra lo que el Compose realmente interpola.
    guide02 = ROOT / "docs" / "02-produccion-aws-terraform.md"
    if guide02.exists():
        gtext = guide02.read_text(encoding="utf-8")

        # Variables citadas en la tabla de inventario (celdas con `VAR` en backticks).
        inventory: set[str] = set()
        for row in re.findall(r"^\|.*\|$", gtext, re.M):
            first = row.split("|")[1] if row.count("|") > 2 else ""
            inventory |= set(re.findall(r"`([A-Z][A-Z0-9_]{2,})`", first))

        # Variables que interpola el Compose de producción (§14.1/§14.2 y el override HTTPS).
        # `$${VAR}` es un escape de Compose: lo resuelve la shell del contenedor, no el .env.
        compose: set[str] = set()
        for start, lang, body in iter_code_blocks(gtext, guide02)[0]:
            if lang != "yaml" or "services:" not in body:
                continue
            # Sin los comentarios: ahí un ${VAR} suele ser la explicación de la regla,
            # no una interpolación real que el .env deba satisfacer.
            live = "\n".join(l for l in body.split("\n") if not l.lstrip().startswith("#"))
            compose |= set(re.findall(r"(?<!\$)\$\{([A-Z_][A-Z0-9_]*)", live))

        # Las de build-args del Dockerfile no viven en el .env del host.
        compose -= {"AIRFLOW_VERSION", "PYTHON_VERSION"}
        for name in sorted(compose - inventory):
            problems.append(
                f"02-produccion-aws-terraform.md: el Compose interpola ${name} "
                f"pero no está en el inventario del .env (§13.4): nadie dice qué sección lo publica"
            )

    # Deriva entre el script del repo y la copia embebida en la guía.
    guide = ROOT / "docs" / "02-produccion-aws-terraform.md"
    if guide.exists():
        gtext = guide.read_text(encoding="utf-8")
        marker = "prod-env.sh — única fuente de verdad"
        if marker in gtext and "__pe_load_terraform" in gtext:
            chunk = gtext[gtext.index(marker):]
            chunk = QUOTE_RE.sub("", chunk[: chunk.index("\n```")])
            embedded = loader_vars(chunk)
            only_repo = loader_vars(loader_text) - embedded
            only_doc = embedded - loader_vars(loader_text)
            for name in sorted(only_repo):
                problems.append(f"deriva: prod-env.sh exporta ${name} pero la guía no lo documenta")
            for name in sorted(only_doc):
                problems.append(f"deriva: la guía documenta ${name} pero prod-env.sh no lo exporta")

    if problems:
        print("Contrato de variables de entorno: FALLA\n")
        for item in problems:
            print(f"- {item}")
        return 1

    print("Contrato de variables de entorno: OK")
    print(f"  {len(provided - AMBIENT)} variables provistas por prod-env.sh")
    return 0


if __name__ == "__main__":
    sys.exit(main())
