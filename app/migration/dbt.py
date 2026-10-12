"""dbt projects: copy, flag Databricks-specific config, retarget profiles.yml. Spec §3.6."""

from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Any

from app.conversion.common import safe

__all__ = ["convert_dbt"]

DBT_FLAGS = [
    (r"file_format\s*[=:]\s*['\"]?delta", "file_format=delta is Databricks-specific"),
    (
        r"liquid_clustered_by|databricks_compute|\+?location_root|\+?catalog\b",
        "Databricks-specific dbt config",
    ),
    (r"dbt[-_]databricks", "dbt-databricks adapter reference"),
    (r"\bZORDER\b|\bOPTIMIZE\b", "Databricks maintenance SQL"),
    (r"\b\w+\.\w+\.\w+\b(?=\s*(?:\n|$|\)))", "possible 3-part (Unity Catalog) name"),
]


def convert_dbt(
    job_name: str, src_dir: str | Path, cfg: dict[str, Any], outdir: str | Path
) -> tuple[str, list[str]]:
    src, dst = Path(src_dir), Path(outdir) / "artifacts" / "dbt" / safe(job_name)
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(
        src, dst, ignore=shutil.ignore_patterns("target", "dbt_packages", "logs", ".git")
    )
    issues = []
    for f in list(dst.rglob("*.sql")) + list(dst.rglob("*.yml")):
        if f.name == "profiles.yml":
            continue
        text = f.read_text("utf-8", "ignore")
        for rx, msg in DBT_FLAGS:
            if re.search(rx, text, re.I | re.M):
                issues.append(f"{f.relative_to(dst)}: {msg}")
    old = dst / "profiles.yml"
    if old.exists():
        old.rename(dst / "profiles.databricks.yml")
    prof = "default"
    pj = dst / "dbt_project.yml"
    if pj.exists():
        m = re.search(r"^profile:\s*['\"]?([\w\-]+)", pj.read_text("utf-8"), re.M)
        prof = m.group(1) if m else prof
    if cfg["dbt_target"] == "lakehouse":
        out = f"{prof}:\n  target: fabric\n  outputs:\n    fabric:\n      type: fabricspark   # dbt-fabricspark (Livy)\n      method: livy\n      authentication: CLI\n      endpoint: https://api.fabric.microsoft.com/v1\n      workspaceid: {cfg['workspace_id'] or '<workspace-id>'}\n      lakehouseid: <lakehouse-id>\n      lakehouse: {cfg['lakehouse_default']}\n      schema: dbt\n"
    else:
        out = f"{prof}:\n  target: fabric\n  outputs:\n    fabric:\n      type: fabric   # dbt-fabric (Fabric Data Warehouse)\n      driver: 'ODBC Driver 18 for SQL Server'\n      server: <warehouse-sql-endpoint>\n      database: {cfg['warehouse_name']}\n      schema: dbt\n      authentication: CLI\n"
    (dst / "profiles.yml").write_text(out, "utf-8")
    return str(dst), sorted(set(issues))
