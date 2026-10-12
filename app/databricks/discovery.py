"""Live workspace inventory -> assessment dict (the engine's input). Spec §5.1.

Produces the same asset shape as the external assessor (dbx2fabric_assess.py v3):
``{"type", "name", "location", "meta"}``. Notebook names are their base name because pipeline
activities and ``%run`` references resolve notebooks by base name.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from app.conversion.common import nb_name
from app.core.exceptions import DatabricksError
from app.core.logging import get_logger
from app.databricks.client import DatabricksClient

__all__ = ["DiscoveryOptions", "discover", "summarize_assets"]

log = get_logger(__name__)
SYSTEM_CATALOGS = {"system", "__databricks_internal", "samples"}


@dataclass
class DiscoveryOptions:
    workspace_paths: list[str] = field(default_factory=lambda: ["/"])
    include_jobs: bool = True
    include_tables: bool = True
    catalogs: list[str] = field(default_factory=list)  # empty = every non-system catalog
    schemas: list[str] = field(default_factory=list)  # empty = every schema
    include_secret_scopes: bool = True
    include_clusters: bool = True
    max_notebooks: int = 5000


def summarize_assets(assets: list[dict[str, Any]]) -> dict[str, Any]:
    by_type: dict[str, int] = {}
    for a in assets:
        by_type[a["type"]] = by_type.get(a["type"], 0) + 1
    return {"total_assets": len(assets), "by_type": by_type}


def _notebooks(
    c: DatabricksClient, opts: DiscoveryOptions, warn: Callable[[str], None]
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    queue = list(dict.fromkeys(opts.workspace_paths or ["/"]))
    seen: set[str] = set()
    while queue and len(out) < opts.max_notebooks:
        path = queue.pop(0)
        if path in seen:
            continue
        seen.add(path)
        try:
            objs = c.list_workspace(path)
        except DatabricksError as e:
            warn(f"workspace path {path}: {e.message}")
            continue
        for o in objs:
            kind, p = o.get("object_type"), o.get("path", "")
            if kind in ("DIRECTORY", "REPO"):
                queue.append(p)
            elif kind == "NOTEBOOK":
                out.append(
                    {
                        "type": "Notebook",
                        "name": nb_name(p),
                        "location": p,
                        "meta": {
                            "source_path": p,
                            "language": o.get("language") or "PYTHON",
                            "object_id": o.get("object_id"),
                        },
                    }
                )
                if len(out) >= opts.max_notebooks:
                    warn(
                        f"notebook limit reached ({opts.max_notebooks}); remaining notebooks were not inventoried"
                    )
                    break
    return out


def _tables(
    c: DatabricksClient, opts: DiscoveryOptions, warn: Callable[[str], None]
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    cats = opts.catalogs or [
        x["name"] for x in c.list_catalogs() if x.get("name") not in SYSTEM_CATALOGS
    ]
    for cat in cats:
        try:
            schemas = c.list_schemas(cat)
        except DatabricksError as e:
            warn(f"catalog {cat}: {e.message}")
            continue
        for s in schemas:
            if s.get("name") == "information_schema" or (
                opts.schemas and s.get("name") not in opts.schemas
            ):
                continue
            try:
                for t in c.iter_tables(cat, s["name"]):
                    parts = sorted(
                        (
                            col
                            for col in t.get("columns", [])
                            if col.get("partition_index") is not None
                        ),
                        key=lambda col: col["partition_index"],
                    )
                    full = t.get("full_name") or f"{cat}.{s['name']}.{t['name']}"
                    out.append(
                        {
                            "type": "Table/View",
                            "name": full,
                            "location": t.get("storage_location") or full,
                            "meta": {
                                "table_type": t.get("table_type"),
                                "storage_location": t.get("storage_location"),
                                "format": t.get("data_source_format"),
                                "partition_columns": [p["name"] for p in parts],
                                "view_definition": t.get("view_definition"),
                            },
                        }
                    )
            except DatabricksError as e:
                warn(f"schema {cat}.{s['name']}: {e.message}")
    return out


def discover(c: DatabricksClient, opts: DiscoveryOptions) -> dict[str, Any]:
    """Inventory the workspace. Partial failures (one schema, one path) become warnings, not errors."""
    warnings: list[str] = []
    warn = warnings.append
    assets = _notebooks(c, opts, warn)
    if opts.include_jobs:
        try:
            for j in c.iter_jobs():
                st = j.get("settings", {})
                assets.append(
                    {
                        "type": "Job",
                        "name": st.get("name") or f"job_{j.get('job_id')}",
                        "location": f"job:{j.get('job_id')}",
                        "meta": {"job_id": j.get("job_id"), "settings": st},
                    }
                )
        except DatabricksError as e:
            warn(f"jobs: {e.message}")
    if opts.include_tables:
        try:
            assets += _tables(c, opts, warn)
        except DatabricksError as e:
            warn(f"unity catalog: {e.message}")
    if opts.include_secret_scopes:
        try:
            for sc in c.list_secret_scopes():
                keys = [k["key"] for k in c.list_secret_keys(sc["name"])]
                assets.append(
                    {
                        "type": "Secret Scope",
                        "name": sc["name"],
                        "location": f"scope:{sc['name']}",
                        "meta": {"scope": sc["name"], "keys": keys},
                    }
                )
        except DatabricksError as e:
            warn(f"secret scopes: {e.message}")
    if opts.include_clusters:
        try:
            for cl in c.list_clusters():
                if cl.get("cluster_source") in ("JOB", "PIPELINE"):
                    continue  # ephemeral job clusters are covered by their job
                assets.append(
                    {
                        "type": "Cluster",
                        "name": cl.get("cluster_name") or cl.get("cluster_id"),
                        "location": f"cluster:{cl.get('cluster_id')}",
                        "meta": {
                            "spark_version": cl.get("spark_version"),
                            "spark_conf": cl.get("spark_conf") or {},
                            "node_type_id": cl.get("node_type_id"),
                        },
                    }
                )
        except DatabricksError as e:
            warn(f"clusters: {e.message}")
    for w in warnings:
        log.warning("discovery_warning", summary=w)
    return {
        "source": {"kind": "databricks", "host": c.creds.host},
        "summary": summarize_assets(assets),
        "warnings": warnings,
        "assets": assets,
    }
