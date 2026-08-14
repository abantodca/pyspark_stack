#!/usr/bin/env python3
"""Comprueba enlaces y referencias internas de la documentación, sin red.

Tres controles:

1.  ENLACES RELATIVOS: `[texto](otro.md)` apunta a un archivo que existe.
2.  ANCLAS: `[texto](otro.md#seccion)` apunta a un encabezado que existe en ese
    archivo, con las mismas reglas de slug que usa GitHub.
3.  REFERENCIAS §: `§7.3` existe en el documento actual, y `guía 02 §13.4` existe
    en la guía 02. Las guías son incrementales y se renumeran al editarlas: una
    referencia a una subsección inexistente manda al lector a buscar algo que no
    está, y es invisible en una lectura lineal.
"""

from __future__ import annotations

import re
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

LINK = re.compile(r"\[[^\]]*]\((?!https?://|mailto:)([^)#]*)(?:#([^)]*))?\)")
HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*$", re.M)
SECTION_NUM = re.compile(r"^#{2,3}\s+(\d+(?:\.\d+[a-z]?)?)[.\s]", re.M)

# Referencias a otro documento. Se aceptan las tres formas que usa el repo:
# "guía 02 §5.6", "docs/02 §6.4", "01 §8.5". El identificador que antecede al §
# decide contra qué documento se valida. El último grupo admite cadenas:
# "guía 02 §11.2/§11.3", "guía 02 §12.1–§12.3", "Guía 02 §13.4 y §14.1".
CROSS_REF = re.compile(
    r"(?:gu[ií]a\s+|docs/)?\b(02b|02|0[13-6])\b\s+(?:v\d\s+)?§(\d+(?:\.\d+[a-z]?)?)"
    r"((?:\s*(?:[/,–—-]|y|and)\s*§\d+(?:\.\d+[a-z]?)?)*)",
    re.I,
)
BARE_REF = re.compile(r"§(\d+\.\d+[a-z]?)")

# Cada guía es una familia de archivos: hoy todas tienen uno solo, pero el índice de secciones
# se arma con la unión de la familia, así que partir un documento en varios no rompe una sola
# referencia `§X.Y` — solo hay que sumar el archivo nuevo acá.
#
# Las rutas son relativas a `docs/`: en el árbol solo quedan arriba los cuatro documentos
# centrales, y el material de consulta vive en `referencia/`. Los nombres de archivo son únicos
# en todo el árbol, así que el resto del script indexa por nombre y no por ruta.
GUIDES = {
    "01": ["01-stack-local.md"],
    "02": ["02-produccion-aws-terraform.md"],
    "02b": ["referencia/02b-produccion-aws-consola.md"],
    "03": ["03-arquitectura.md"],
    "04": ["04-ejemplos-locales.md"],
    "05": ["referencia/05-production-readiness.md"],
    "06": ["referencia/06-historial-de-incidentes.md"],
}

# Nombre de archivo -> guía a la que pertenece, para resolver un `§X.Y` suelto contra toda su
# familia. Se indexa por nombre porque los documentos no comparten carpeta.
FAMILY_OF = {
    Path(rel).name: guide for guide, rels in GUIDES.items() for rel in rels
}
MEMBERS = {guide: {Path(rel).name for rel in rels} for guide, rels in GUIDES.items()}

def strip_code(text: str) -> str:
    """Quita los bloques ``` conservando la numeración de líneas.

    No sirve una regex: las guías embeben fences dentro de citas (`> ```bash`), y
    un escaneo por regex empareja el cierre citado con una apertura sin citar,
    desincronizándose y tragándose encabezados reales. Se recorre línea a línea,
    normalizando el prefijo de cita antes de decidir.
    """
    out: list[str] = []
    inside = False
    for line in text.split("\n"):
        bare = line.lstrip()
        while bare.startswith(">"):
            bare = bare[1:].lstrip()
        if bare.startswith("```"):
            inside = not inside
            out.append("")
            continue
        out.append("" if inside else line)
    return "\n".join(out)


def slug(text: str) -> str:
    """Replica el slug de anclas de GitHub.

    GitHub CONSERVA los caracteres acentuados (`#1-visión-general` es válido), así
    que no hay que normalizar a ASCII: hacerlo genera falsos positivos en todos los
    encabezados en español.
    """
    text = re.sub(r"`([^`]*)`", r"\1", text)             # quita backticks
    text = re.sub(r"\[([^\]]*)]\([^)]*\)", r"\1", text)  # deja el texto del enlace
    text = unicodedata.normalize("NFC", text).lower()
    text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    # Cada espacio da UN guion: GitHub no colapsa los espacios consecutivos que
    # dejó la puntuación borrada. Por eso "S3 + cómputo" ancla como "s3--cómputo".
    return re.sub(r"\s", "-", text.strip())


def anchors_of(text: str) -> set[str]:
    return {slug(m.group(2)) for m in HEADING.finditer(strip_code(text))}


def sections_of(text: str) -> set[str]:
    return {m.group(1) for m in SECTION_NUM.finditer(strip_code(text))}


def main() -> int:
    broken: list[str] = []
    docs = [ROOT / "README.md", *sorted((ROOT / "docs").rglob("*.md"))]

    anchors: dict[str, set[str]] = {}
    sections: dict[str, set[str]] = {}
    for doc in docs:
        text = doc.read_text(encoding="utf-8")
        anchors[doc.name] = anchors_of(text)
        sections[doc.name] = sections_of(text)

    # Secciones por guía: la unión de todos los archivos de la familia.
    guide_sections: dict[str, set[str]] = {
        guide: set().union(*(sections.get(n, set()) for n in names))
        for guide, names in MEMBERS.items()
    }

    for doc in docs:
        text = doc.read_text(encoding="utf-8")

        def at(pos: int) -> str:
            return f"{doc.relative_to(ROOT)}:{text.count(chr(10), 0, pos) + 1}"

        # 1 y 2 — enlaces relativos y sus anclas
        for match in LINK.finditer(text):
            target = (match.group(1) or "").strip().replace("%20", " ")
            anchor = match.group(2)
            resolved = (doc.parent / target).resolve() if target else doc
            if target and not resolved.exists():
                broken.append(f"{at(match.start())}: archivo inexistente -> {target}")
                continue
            if anchor and resolved.name in anchors:
                if anchor.lower() not in anchors[resolved.name]:
                    broken.append(
                        f"{at(match.start())}: ancla inexistente -> {resolved.name}#{anchor}"
                    )

        # 3 — referencias §. Primero las cruzadas, para no contarlas como locales.
        cross_spans: list[tuple[int, int]] = []
        for match in CROSS_REF.finditer(text):
            cross_spans.append(match.span())
            guide = match.group(1).lower()
            if doc.name in MEMBERS[guide]:
                continue  # se autorreferencia por nombre: se valida como local
            nums = [match.group(2), *re.findall(r"§(\d+(?:\.\d+[a-z]?)?)", match.group(3) or "")]
            for num in nums:
                if num not in guide_sections[guide]:
                    broken.append(
                        f"{at(match.start())}: §{num} no existe en la guía {guide}"
                    )

        # Un `§X.Y` suelto se resuelve contra la guía entera, no contra el archivo suelto.
        own = guide_sections.get(FAMILY_OF.get(doc.name, ""), sections[doc.name])
        for match in BARE_REF.finditer(text):
            if any(s <= match.start() < e for s, e in cross_spans):
                continue
            if match.group(1) not in own:
                broken.append(
                    f"{at(match.start())}: §{match.group(1)} no existe en esta guía"
                )

    if broken:
        print("Referencias rotas en la documentación:\n")
        print("\n".join(f"- {item}" for item in broken))
        return 1

    total = sum(len(v) for v in sections.values())
    print(f"Enlaces, anclas y referencias §: OK ({total} secciones indexadas)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
