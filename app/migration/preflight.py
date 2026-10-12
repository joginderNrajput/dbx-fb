"""Static checks on generated artifacts before anything is sent to Fabric. Spec §3.11."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from app.migration.ledger import load_ledger, resolve

__all__ = ["preflight"]


def preflight(out: str | Path, cfg: dict[str, Any]) -> list[tuple[str, str, str]]:
    """-> [(level, where, message)]; any "error" blocks deploy unless forced."""
    out = Path(out)
    led = load_ledger(out)
    issues: list[tuple[str, str, str]] = []

    def add(lvl: str, where: str, msg: str) -> None:
        issues.append((lvl, where, msg))

    for k in ("lakehouse_default", "warehouse_name"):
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", cfg[k]):
            add(
                "error",
                "mapping.json",
                f"{k} '{cfg[k]}' must start with a letter and use letters, digits, underscores",
            )
    for c, lh in cfg["catalog_to_lakehouse"].items():
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", lh):
            add(
                "error",
                "mapping.json",
                f"lakehouse name '{lh}' (catalog {c}) must use letters, digits, underscores",
            )
    names: dict[tuple[str, str], int] = {}
    nbs: set[str] = set()
    for e in led.get("artifacts", []):
        if e["kind"] in ("notebook", "pipeline"):
            names.setdefault((e["kind"], e["name"]), 0)
            names[(e["kind"], e["name"])] += 1
        if e["kind"] == "notebook":
            nbs.add(e["name"])
    for (k, n), c in names.items():
        if c > 1:
            add("error", n, f"duplicate {k} name (two source assets map to the same Fabric name)")
    for e in led.get("artifacts", []):
        p = resolve(out, e["path"])
        if e["kind"] in ("notebook", "pipeline", "shortcuts") and not p.exists():
            add("error", e["name"], f"artifact file missing: {p}")
            continue
        if e["kind"] == "notebook":
            try:
                nb = json.loads(p.read_text("utf-8"))
                code = "\n".join(
                    "".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code"
                )
            except Exception as ex:
                add("error", e["name"], f"not a valid notebook: {ex}")
                continue
            if re.search(r"^\s*(#|--|//)\s?MAGIC\b", code, re.M):
                add("error", e["name"], "unconverted MAGIC lines left in a code cell")
            if re.search(r"Databricks notebook source", code):
                add("warn", e["name"], "Databricks header comment left in a cell")
            todo_n = code.count("TODO[dbx2fabric:")
            if todo_n:
                add(
                    "warn",
                    e["name"],
                    f"{todo_n} unresolved TODO marker(s): deploys, but those cells need rework",
                )
            if re.search(r"\bdbutils\.", code):
                add(
                    "warn",
                    e["name"],
                    "dbutils calls remain (see TODO markers): they fail in Fabric",
                )
            if "<your-vault>" in code:
                add(
                    "warn",
                    e["name"],
                    "Key Vault URL placeholder: set keyvault_url_default / keyvault_urls in mapping.json and re-run convert",
                )
        elif e["kind"] == "pipeline":
            try:
                acts = json.loads(p.read_text("utf-8"))["properties"]["activities"]
            except Exception as ex:
                add("error", e["name"], f"not a valid pipeline definition: {ex}")
                continue
            an = {a["name"] for a in acts}
            if len(an) != len(acts):
                add("error", e["name"], "duplicate activity names")
            for a in acts:
                for d in a.get("dependsOn", []):
                    if d["activity"] not in an:
                        add(
                            "error",
                            e["name"],
                            f"activity {a['name']} depends on unknown activity {d['activity']}",
                        )
                for x in re.findall(r"@@NB:([^@]+)@@", json.dumps(a)):
                    if x not in nbs:
                        add(
                            "warn",
                            e["name"],
                            f"activity {a['name']} calls notebook '{x}' which is not part of this migration: it must already exist in the workspace",
                        )
                for x in re.findall(r"@@PL:([^@]+)@@", json.dumps(a)):
                    if x == e["name"]:
                        add("error", e["name"], f"activity {a['name']} invokes its own pipeline")
                    elif ("pipeline", x) not in names:
                        add(
                            "warn",
                            e["name"],
                            f"activity {a['name']} invokes pipeline '{x}' which is not part of this migration: it must already exist in the workspace",
                        )
        elif e["kind"] == "shortcuts" and not cfg["adls_connection_id"]:
            add("warn", e["name"], "adls_connection_id is empty: shortcuts will not be created")
    blocked = sum(
        1 for it in led["items"] if any("staging_storage" in r for r in it.get("reasons", []))
    )
    if blocked:
        add("warn", "mapping.json", f"{blocked} item(s) are blocked until staging_storage is set")
    return issues
