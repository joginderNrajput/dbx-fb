"""ledger.json: the per-migration record of every item, its outcome and artifacts. Spec §3.5."""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any

from app.conversion.common import (
    ENGINE_VERSION,
    MIG,
    NOT,
    OUTCOMES,
    PART,
    TEST,
    load_json,
    save_json,
)
from app.core.exceptions import NotFound, ValidationFailed
from app.migration.planner import plan

__all__ = ["do_plan", "load_ledger", "record_result", "resolve", "summarize_ledger"]


def resolve(out: str | Path, p: str) -> Path:
    """Artifact paths in ledger.json may come from another machine: re-anchor them under the output folder."""
    q = Path(p)
    if q.exists():
        return q
    parts = re.split(r"[\\/]", str(p))
    for key in ("artifacts", "ai_tasks"):
        if key in parts:
            return Path(out).joinpath(*parts[parts.index(key) :])
    return q


def do_plan(
    assess: dict[str, Any], cfg: dict[str, Any], out: str | Path, source: str = ""
) -> dict[str, Any]:
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    ledger = {
        "version": ENGINE_VERSION,
        "created": datetime.now().isoformat(timespec="seconds"),
        "source_assessment": source,
        "items": plan(assess, cfg),
    }
    save_json(out / "ledger.json", ledger)
    return ledger


def load_ledger(out: str | Path) -> dict[str, Any]:
    p = Path(out) / "ledger.json"
    if not p.exists():
        raise NotFound("ledger not found: the migration has not been planned yet")
    led: dict[str, Any] = load_json(p)
    return led


def summarize_ledger(ledger: dict[str, Any]) -> dict[str, Any]:
    its = ledger["items"]
    cnt = {k: sum(1 for i in its if i["outcome"] == k) for k in (MIG, TEST, PART, NOT)}
    by_type: dict[str, dict[str, int]] = {}
    for i in its:
        row = by_type.setdefault(i["type"], {MIG: 0, TEST: 0, PART: 0, NOT: 0})
        row[i["outcome"]] = row.get(i["outcome"], 0) + 1
    return {
        "total": len(its),
        **cnt,
        "deployed": sum(1 for i in its if i["deployment"] == "deployed"),
        "generated_only": sum(1 for i in its if i["deployment"] == "generated"),
        "failed": sum(1 for i in its if i["deployment"] == "failed"),
        "by_type": by_type,
    }


def record_result(
    out: str | Path,
    item_id: str,
    outcome: str,
    note: str = "",
    target_id: str | None = None,
    *,
    actor: str = "manual/AI",
) -> dict[str, Any]:
    """Record manual or AI work on an item so the report stays accurate."""
    if outcome not in OUTCOMES:
        raise ValidationFailed(f"outcome must be one of {', '.join(OUTCOMES)}", field="outcome")
    p = Path(out) / "ledger.json"
    led = load_ledger(out)
    for i in led["items"]:
        if i["id"] == item_id:
            i["outcome"] = outcome
            i["actions"].append(f"[{actor}] {note}")
            if target_id:
                i["target_id"] = target_id
            if outcome != NOT:
                i["deployment"] = "deployed"
            save_json(p, led)
            return {"updated": item_id, "outcome": outcome}
    raise NotFound(f"item not found: {item_id}", field="item_id")
