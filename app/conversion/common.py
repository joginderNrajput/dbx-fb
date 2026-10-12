"""Shared helpers and outcome vocabulary of the migration engine. Spec §3.

Ported from dbx2fabric_migrate_v4.py (the reference tool); behaviour is kept identical and is
checked by tests/unit/conversion/test_parity.py.
"""

from __future__ import annotations

import json
import posixpath
import re
from pathlib import Path
from typing import Any

__all__ = [
    "ENGINE_VERSION",
    "LABEL",
    "MIG",
    "NOT",
    "OUTCOMES",
    "PART",
    "TEST",
    "ident",
    "load_json",
    "nb_name",
    "norm_path",
    "safe",
    "save_json",
]

# Version of the conversion rules (matches the reference tool, stamped into generated notebooks).
ENGINE_VERSION = "0.3.0"
MIG, TEST, PART, NOT = "migrated", "needs_testing", "partial", "not_migrated"
OUTCOMES = (MIG, TEST, PART, NOT)
LABEL = {
    MIG: "Migrated",
    TEST: "Migrated - needs testing",
    PART: "Partially migrated - manual rework",
    NOT: "Not migrated",
}


def safe(n: str | None) -> str:
    return re.sub(r"[^A-Za-z0-9_\-]+", "_", n or "").strip("_")[:100] or "item"


def ident(n: str | None) -> str:
    n = re.sub(r"\W+", "_", n or "")
    return n if re.match(r"[A-Za-z_]", n) else "_" + n


def load_json(p: str | Path) -> Any:
    return json.loads(Path(p).read_text("utf-8"))


def save_json(p: str | Path, o: Any) -> None:
    Path(p).parent.mkdir(parents=True, exist_ok=True)
    Path(p).write_text(json.dumps(o, indent=2, ensure_ascii=False), "utf-8")


def norm_path(p: str | None, base: str = "") -> str:
    p = (p or "").strip().strip("\"'")
    if p.startswith("/Workspace/"):
        p = p[len("/Workspace") :]
    if p and not p.startswith("/") and base.startswith("/"):
        p = posixpath.normpath(posixpath.join(posixpath.dirname(base), p))
    return p


def nb_name(p: str | None) -> str:
    return safe(norm_path(p).rstrip("/").split("/")[-1])
