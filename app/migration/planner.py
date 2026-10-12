"""Plan: every inventoried asset gets a Fabric target and a strategy. Spec §3.5.

Target rules: OLTP database -> Fabric SQL database | OLAP database -> Warehouse |
other analytics/files -> Lakehouse | code -> Notebooks | orchestration -> Data Pipeline.
"""

from __future__ import annotations

import re
from typing import Any

from app.conversion.common import safe

__all__ = ["ROUTES", "is_default_managed", "new_item", "parse_abfss", "plan", "route_table"]


def parse_abfss(loc: str | None) -> tuple[str, str] | None:
    m = re.match(r"abfss?://([^@/]+)@([^/]+)(/.*)?$", loc or "")
    if m:
        return f"https://{m.group(2)}", f"/{m.group(1)}{m.group(3) or ''}"
    m = re.match(r"https://([^/]+\.dfs\.core\.windows\.net)(/.*)$", loc or "")
    return (f"https://{m.group(1)}", m.group(2)) if m else None


def route_table(a: dict[str, Any], cfg: dict[str, Any]) -> tuple[str, str, str]:
    cat, sch, _ = (a["name"].split(".") + ["", "", ""])[:3]
    m = a.get("meta", {})
    tt, loc = m.get("table_type"), m.get("storage_location") or ""
    lh = cfg["catalog_to_lakehouse"].get(cat, cfg["lakehouse_default"])
    tgt = (
        ("Warehouse", cfg["warehouse_name"])
        if sch.lower() in [x.lower() for x in cfg["warehouse_schemas"]]
        else ("Lakehouse", lh)
    )
    if tt == "VIEW":
        return tgt + ("view",)
    if tt in ("MATERIALIZED_VIEW", "STREAMING_TABLE", "FOREIGN") or not tt:
        return tgt + ("manual",)
    if parse_abfss(loc):
        return tgt + ("shortcut" if tt == "EXTERNAL" else "copy",)
    return tgt + ("manual",)


def is_default_managed(loc: str | None) -> bool:
    """Databricks-managed (default) UC storage: not reachable from outside Databricks."""
    if "__unitystorage" in (loc or ""):
        return True
    ph = parse_abfss(loc)
    return bool(ph and ph[0].split("//")[1].startswith("dbstorage"))


def new_item(a: dict[str, Any], ttype: str, tname: str, strategy: str) -> dict[str, Any]:
    return {
        "id": f"{a['type']}:{a['name']}",
        "type": a["type"],
        "name": a["name"],
        "target_type": ttype,
        "target_name": tname,
        "strategy": strategy,
        "outcome": "planned",
        "deployment": "pending",
        "reasons": [],
        "actions": [],
        "tests": [],
        "artifacts": [],
        "target_id": None,
        "assessment_score": a.get("score"),
        "assessment_status": a.get("bucket"),
    }


ROUTES = {
    "DLT Pipeline": ("Materialized Lake Views / Notebooks", "manual"),
    "Cluster": ("Environment", "environment"),
    "Library": ("Environment", "environment"),
    "SQL Warehouse": ("Warehouse", "infra"),
    "Secret Scope": ("Azure Key Vault", "manual"),
    "Repo": ("Git integration", "manual"),
    "Dashboard": ("Power BI report", "manual"),
    "Alert": ("Activator", "manual"),
    "ML Model": ("Fabric ML model", "manual"),
    "Serving Endpoint": ("Azure ML / AI Foundry", "manual"),
    "Credential": ("Workspace identity / connection", "manual"),
    "Permission": ("Fabric roles / OneLake security", "manual"),
    "Function": ("Warehouse function / notebook", "function"),
    "SQL Query": ("Notebook", "queries"),
    "DBFS Mount": ("OneLake shortcut / Files path", "mount"),
    "External Location": ("ADLS connection", "extloc"),
    "Volume": ("Lakehouse", "volume"),
    "Connection": ("Fabric connection / SQL database", "connection"),
}


def plan(assess: dict[str, Any], cfg: dict[str, Any]) -> list[dict[str, Any]]:
    items = []
    for a in assess["assets"]:
        t = a["type"]
        if t == "Notebook":
            items.append(new_item(a, "Notebook", safe(a["name"]), "convert"))
        elif t == "Job":
            items.append(new_item(a, "Data Pipeline", "pl_" + safe(a["name"]), "convert"))
            if any("dbt_task" in x for x in a["meta"].get("settings", {}).get("tasks", [])):
                items.append(
                    new_item(
                        {"type": "dbt Project", "name": a["name"]},
                        "dbt job",
                        safe(a["name"]),
                        "dbt",
                    )
                )
        elif t == "Table/View":
            items.append(new_item(a, *route_table(a, cfg)))
        elif t in ROUTES:
            tt, st = ROUTES[t]
            tname = ""
            if t == "SQL Warehouse":
                tname = cfg["warehouse_name"]
            elif (
                t == "Connection"
                and (a["meta"].get("connection_type") or "").upper()
                in cfg["sqldb_connection_types"]
            ):
                tt, tname = "SQL database", cfg["sqldb_name"]
            items.append(new_item(a, tt, tname, st))
        else:
            items.append(new_item(a, "-", "-", "manual"))
    return items
