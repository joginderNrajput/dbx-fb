"""AI work orders: one Markdown brief per item that still needs manual or AI completion. Spec §3.9."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.conversion.common import LABEL, safe

__all__ = ["write_ai_task"]


def write_ai_task(
    it: dict[str, Any], a: dict[str, Any], tasks_dir: Path, cfg: dict[str, Any]
) -> None:
    tasks_dir.mkdir(parents=True, exist_ok=True)
    body = f"""# AI work order: {it["id"]}
Outcome so far: **{LABEL.get(it["outcome"])}**   Target: **{it["target_type"]} / {it["target_name"]}**

## Why this needs you
{chr(10).join("- " + r for r in it["reasons"]) or "- manual migration"}

## Source details
```json
{json.dumps(a.get("meta", {}), indent=2, default=str)[:6000]}
```
Converted artifacts: {", ".join(it["artifacts"]) or "none"}

## Fabric target rules (follow these)
- OLTP database -> Fabric SQL database; OLAP database -> Warehouse; all other analytics / files -> Lakehouse
- Code -> Fabric Notebooks (use notebookutils, never dbutils)
- Ingestion -> Data Pipeline Copy Data or Copy job; orchestration -> Data Pipeline
- dbt -> Fabric dbt job (adapters: dbt-fabric for Warehouse, dbt-fabricspark for Lakehouse); Arrow/pyarrow is available in Fabric Spark runtimes

## Suggested tools
- Fabric MCP (API specs, item definitions) to build the item definition; Power BI Modeling MCP for semantic models / dashboards
- This service: `GET /api/v1/migrations/{{id}}/items` for context, then `POST /api/v1/migrations/{{id}}/results` when you finish

## Done when
- The item exists in Fabric (or the artifact is complete), is tested against the Databricks source, and the result has been recorded with outcome `needs_testing` or `migrated` and a note.
"""
    (tasks_dir / f"{safe(it['id'])}.md").write_text(body, "utf-8")
