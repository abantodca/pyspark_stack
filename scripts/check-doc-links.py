#!/usr/bin/env python3
"""Comprueba enlaces relativos Markdown sin depender de servicios externos."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LINK = re.compile(r"\[[^\]]*]\((?!https?://|mailto:)([^)#]+)(?:#[^)]*)?\)")


def main() -> int:
    broken: list[str] = []
    for document in [ROOT / "README.md", *sorted((ROOT / "docs").glob("*.md"))]:
        text = document.read_text(encoding="utf-8")
        for match in LINK.finditer(text):
            target = match.group(1).strip().replace("%20", " ")
            if not (document.parent / target).resolve().exists():
                line = text.count("\n", 0, match.start()) + 1
                broken.append(f"{document.relative_to(ROOT)}:{line}: {target}")
    if broken:
        print("Enlaces relativos rotos:")
        print("\n".join(f"- {item}" for item in broken))
        return 1
    print("Enlaces relativos Markdown: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
