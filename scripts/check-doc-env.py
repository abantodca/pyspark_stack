#!/usr/bin/env python3
"""Valida el contrato ejecutable y editorial de las guías (02 §3.1).

Comprueba once invariantes que rompen una guía copy-paste e incremental:

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
8.  TASKFILE: cada task de los bloques YAML de la guía está LITERAL en Taskfile.yml. Esos
    bloques son la fuente de verdad; sin este chequeo el archivo se quedó sin `prod:wait`,
    `prod:smoke` ni `release:*`, y la guía las manda a correr igual.
9.  ORDEN DE TASKS: ninguna `task X` se invoca en la guía ANTES del bloque que la pega.
    Es el mismo invariante que el 1, aplicado al Taskfile: la guía lo entrega por partes
    (§3.0b, §5.5, §6.4, §8, §10.1, §13.4, §15) y un `task prod:deploy` citado antes de §5.5
    manda a correr algo que el lector todavía no tiene en su archivo.
10. DOS COMANDOS: cada bloque de ejecución visible contiene como máximo dos comandos raíz.
    Los scripts y los desplegables «Qué corre por dentro» conservan el detalle completo.
11. CUATRO LÍNEAS: cada explicación visible tiene como máximo cuatro líneas. El detalle largo
    vive en tablas, código o desplegables dentro de la misma guía.

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

# La guía 02 es UNA guía incremental en un solo archivo: un `output` declarado en §5 está
# disponible para los comandos de §8, pero no al revés. El control de orden se hace sobre el
# documento entero, en su orden de lectura. Ruta relativa a `docs/`.
GUIDE_02 = "02-produccion-aws-terraform.md"

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


def normalize_yaml(text: str) -> str:
    """Sin espacios al final de línea: una edición invisible no debe hacer fallar la comparación."""
    return "\n".join(line.rstrip() for line in text.split("\n"))


def yaml_tasks(body: str) -> dict[str, str]:
    """Parte un bloque `tasks:` en {nombre: cuerpo}, sin los comentarios de separación."""
    tasks: dict[str, str] = {}
    name: str | None = None
    buf: list[str] = []

    def cerrar() -> None:
        if name is None:
            return
        lines = buf[:]   # los `# ── sección ──` quedan pegados a la task anterior
        while lines and re.fullmatch(r"\s*(#.*)?", lines[-1]):
            lines.pop()
        tasks[name] = normalize_yaml("\n".join(lines))

    for line in body.split("\n"):
        match = re.match(r"^  ([a-z][a-z0-9:._-]*):\s*$", line)
        if match:
            cerrar()
            name, buf = match.group(1), [line]
        elif name is not None:
            buf.append(line)
    cerrar()
    return tasks


def editorial_problems(text: str) -> list[str]:
    """Comprueba el formato corto de la guía 02 sin limitar código ni desplegables."""
    problems: list[str] = []
    lines = text.splitlines()
    in_fence = False
    fence_lang = ""
    fence_start = 0
    fence_body: list[str] = []
    details = 0
    fence_details = 0
    paragraph: list[str] = []
    paragraph_start = 0
    paragraph_quote: bool | None = None

    def close_paragraph() -> None:
        nonlocal paragraph, paragraph_start, paragraph_quote
        if len(paragraph) > 4:
            problems.append(
                f"{GUIDE_02}:{paragraph_start}: explicación de {len(paragraph)} líneas; máximo 4"
            )
        paragraph = []
        paragraph_start = 0
        paragraph_quote = None

    def root_commands(body: list[str]) -> list[str]:
        commands: list[str] = []
        heredoc: str | None = None
        for raw in body:
            line = re.sub(r"^\s*>\s?", "", raw)
            stripped = line.strip()
            if heredoc:
                if stripped == heredoc:
                    heredoc = None
                continue
            if not stripped or stripped.startswith("#"):
                continue
            marker = re.search(r"<<-?\s*['\"]?([A-Za-z_][A-Za-z0-9_]*)['\"]?", line)
            if marker:
                heredoc = marker.group(1)
            if line[:1].isspace() or stripped.startswith(("--", "|", "&&", "||")):
                continue
            if stripped in {"then", "else", "fi", "do", "done", "esac", "}", ")"}:
                continue
            commands.append(stripped)
        return commands

    for number, line in enumerate([*lines, ""], start=1):
        stripped = line.strip()
        bare = re.sub(r"^>\s?", "", stripped)

        if bare.startswith("```"):
            close_paragraph()
            if not in_fence:
                in_fence = True
                fence_lang = bare[3:].strip().lower()
                fence_start = number
                fence_body = []
                fence_details = details
            else:
                first = next(
                    (re.sub(r"^\s*>\s?", "", row).strip() for row in fence_body if row.strip()),
                    "",
                )
                if (
                    fence_lang in SHELL_LANGS
                    and fence_details == 0
                    and not first.startswith("#!")
                ):
                    commands = root_commands(fence_body)
                    if len(commands) > 2:
                        problems.append(
                            f"{GUIDE_02}:{fence_start}: bloque visible con {len(commands)} "
                            "comandos; máximo 2"
                        )
                in_fence = False
            continue
        if in_fence:
            fence_body.append(line)
            continue

        if "<details" in line:
            close_paragraph()
            details += 1
            continue
        if "</details>" in line:
            close_paragraph()
            details = max(0, details - 1)
            continue
        if details:
            continue

        quoted = stripped.startswith(">")
        prose = bool(bare) and not re.match(
            r"^(#{1,6}\s|[-*+]\s|\d+\.\s|\||---$|<|\[!)", bare
        )
        if prose and (paragraph_quote is None or paragraph_quote == quoted):
            if not paragraph:
                paragraph_start = number
            paragraph.append(line)
            paragraph_quote = quoted
        else:
            close_paragraph()
            if prose:
                paragraph_start = number
                paragraph = [line]
                paragraph_quote = quoted

    return problems


def main() -> int:
    if not LOADER.exists():
        print(f"falta {LOADER.relative_to(ROOT)}: el contrato no existe")
        return 1

    loader_text = LOADER.read_text(encoding="utf-8")
    provided = loader_vars(loader_text) | AMBIENT
    problems: list[str] = []

    # Cada documento se revisa entero y por separado: dentro de uno, el orden de lectura es el
    # orden de las líneas.
    familias: list[list[Path]] = [
        [d] for d in sorted((ROOT / "docs").rglob("*.md")) if d.name not in SKIP_DOCS
    ]

    for familia in familias:
      # Primera pasada: dónde se declara cada output en el documento. Tiene que estar completa
      # antes de revisar un solo bloque; si se fuera llenando sobre la marcha, un output
      # declarado más abajo se reportaría como inexistente en vez de como lo que realmente es:
      # un uso adelantado.
      declared: dict[str, tuple[int, int]] = {}
      for orden, doc in enumerate(familia):
          text = doc.read_text(encoding="utf-8")
          for match in OUTPUT_RE.finditer(text):
              line = text.count("\n", 0, match.start()) + 1
              declared.setdefault(match.group(1).upper(), (orden, line))

      # Segunda pasada: los bloques de comandos.
      for orden, doc in enumerate(familia):
        text = doc.read_text(encoding="utf-8")
        blocks, fence_errors = iter_code_blocks(text, doc)
        problems.extend(fence_errors)

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
                elif declared[name] > (orden, line):
                    d_orden, d_line = declared[name]
                    donde = (
                        f"la línea {d_line}"
                        if d_orden == orden
                        else f"{familia[d_orden].name}:{d_line}, que se lee después"
                    )
                    problems.append(
                        f"{doc.name}:{line}: ${name} se usa ANTES de declararse "
                        f"(su output aparece recién en {donde}) — "
                        f"al copiar el bloque estaría vacía"
                    )

    # 6 — inventario del .env contra lo que el Compose realmente interpola. La tabla vive en
    # §13.4 y los Compose en §5.5/§14: secciones distintas del mismo documento.
    inventory: set[str] = set()
    compose: set[str] = set()
    gpath = ROOT / "docs" / GUIDE_02
    if gpath.exists():
        gtext = gpath.read_text(encoding="utf-8")
        problems.extend(editorial_problems(gtext))

        # Variables citadas en la tabla de inventario (celdas con `VAR` en backticks).
        for row in re.findall(r"^\|.*\|$", gtext, re.M):
            first = row.split("|")[1] if row.count("|") > 2 else ""
            inventory |= set(re.findall(r"`([A-Z][A-Z0-9_]{2,})`", first))

        # Variables que interpola el Compose de producción (§14.1/§14.2 y el override HTTPS).
        # `$${VAR}` es un escape de Compose: lo resuelve la shell del contenedor, no el .env.
        for start, lang, body in iter_code_blocks(gtext, gpath)[0]:
            if lang != "yaml" or "services:" not in body:
                continue
            # Sin los comentarios: ahí un ${VAR} suele ser la explicación de la regla,
            # no una interpolación real que el .env deba satisfacer.
            live = "\n".join(l for l in body.split("\n") if not l.lstrip().startswith("#"))
            compose |= set(re.findall(r"(?<!\$)\$\{([A-Z_][A-Z0-9_]*)", live))

    # Build args y el selector local del archivo de validación no viven en el .env del host.
    compose -= {"AIRFLOW_VERSION", "PYTHON_VERSION", "PROD_ENV_FILE"}
    for name in sorted(compose - inventory):
        problems.append(
            f"guía 02: el Compose interpola ${name} pero no está en el inventario "
            f"del .env (§13.4): nadie dice qué sección lo publica"
        )

    # 7 — prod-env.sh se enlaza, no se copia. Cuando la guía traía el script entero, las dos
    # versiones divergían y hacía falta comparar export por export; ahora el invariante es más
    # fuerte y más simple: que la copia no vuelva.
    if gpath.exists() and "__pe_load_terraform" in gpath.read_text(encoding="utf-8"):
        problems.append(
            f"{GUIDE_02}: volvió a embeberse el cuerpo de prod-env.sh. Es un archivo versionado: "
            f"enlazá scripts/prod-env.sh en vez de pegar una copia que va a divergir"
        )

    # 8 — el Taskfile se pega, no se reescribe: sin este chequeo el archivo del repo se
    # quedó sin `prod:wait`, `prod:smoke` ni `release:*`, y la guía las manda a correr igual.
    # La guía lo entrega por partes (§3.0b, §5.5, §6.4, §8, §10.1, §13.4, §15): se revisan
    # TODOS los bloques que declaran tasks, no uno solo.
    taskfile = ROOT / "Taskfile.yml"
    real: dict[str, str] = {}
    if gpath.exists() and taskfile.exists():
        real = yaml_tasks(taskfile.read_text(encoding="utf-8"))
        for _, lang, body in iter_code_blocks(gtext, gpath)[0]:
            # Un bloque de tasks es YAML con `desc:` y `cmds:`; descarta los Compose y el `vars:`.
            if lang != "yaml" or "    desc:" not in body or "    cmds:" not in body:
                continue
            # Igualdad exacta: un `-auto-approve` agregado al final es justo la deriva a cazar.
            for name, cuerpo in yaml_tasks(body).items():
                if name not in real:
                    problems.append(
                        f"guía 02: la task `{name}` está en la guía pero no en Taskfile.yml — "
                        f"quien siga la guía va a correr `task {name}` y no existe"
                    )
                elif real[name] != cuerpo:
                    problems.append(
                        f"guía 02: la task `{name}` difiere de la de Taskfile.yml. El bloque de la "
                        f"guía es la fuente de verdad: pegalo tal cual (o corregí la guía si el "
                        f"cambio va al revés)"
                    )

    # 9 — orden de las tasks. La guía entrega el Taskfile por partes: una `task X` que se manda a
    # correr antes del bloque que la pega es el mismo error que usar una variable antes de su
    # output. Solo se miran bloques bash (en prosa, "la task corre..." daría falsos positivos) y
    # solo nombres con `:` — los locales del repo (`test`) no los documenta la guía.
    if gpath.exists():
        gblocks = iter_code_blocks(gtext, gpath)[0]
        pasted: dict[str, int] = {}
        for start, lang, body in gblocks:
            if lang != "yaml" or "    desc:" not in body or "    cmds:" not in body:
                continue
            for name in yaml_tasks(body):
                pasted.setdefault(name, start)
        for start, lang, body in gblocks:
            if lang not in SHELL_LANGS:
                continue
            for offset, raw in enumerate(body.split("\n")):
                if raw.lstrip().startswith("#"):
                    continue
                for match in re.finditer(r"\btask\s+([a-z][a-z0-9._-]*:[a-z0-9:._-]+)", raw):
                    name = match.group(1)
                    line = start + 1 + offset
                    if name not in pasted:
                        # Las locales (`doc:check`) ya vienen en el repo: la guía las usa sin
                        # pegarlas, y eso es correcto. Lo que no puede pasar es que no exista.
                        if name not in real:
                            problems.append(
                                f"{GUIDE_02}:{line}: `task {name}` no existe: ni la pega un bloque "
                                f"de la guía, ni está en Taskfile.yml"
                            )
                    elif line < pasted[name]:
                        problems.append(
                            f"{GUIDE_02}:{line}: `task {name}` se manda a correr ANTES del bloque "
                            f"que la pega (línea {pasted[name]}) — todavía no existe en su archivo"
                        )

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
