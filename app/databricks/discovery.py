"""Live workspace inventory -> assessment dict (the engine's input). Spec §5.1, §5.10.

Produces the same asset shape as the external assessor (dbx2fabric_assess.py v3):
``{"type", "name", "location", "meta"}``. Notebook names are their base name because pipeline
activities and ``%run`` references resolve notebooks by base name.

Two entry points:
- ``discover``: crawl (paths, catalogs/schemas, all jobs/clusters/...), for "migrate everything";
- ``discover_selected``: only the objects a caller picked from the browse API (spec §5.10), each
  fetched directly, so a migration does not crawl the workspace again.
The browse API reuses the builders below, so what it lists is exactly what a job will migrate.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from app.conversion.common import nb_name
from app.core.exceptions import DatabricksError
from app.core.logging import get_logger
from app.databricks.client import DatabricksClient

__all__ = [
    "SYSTEM_CATALOGS",
    "DiscoveryOptions",
    "Selection",
    "cluster_asset",
    "discover",
    "discover_selected",
    "job_asset",
    "list_notebooks",
    "notebook_asset",
    "pipeline_asset",
    "scope_asset",
    "summarize_assets",
    "table_asset",
    "warehouse_asset",
]

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
    include_pipelines: bool = True
    include_warehouses: bool = True
    max_notebooks: int = 5000


@dataclass
class Selection:
    """Objects picked from the browse API; only these are inventoried (spec §5.10)."""

    notebooks: list[str] = field(
        default_factory=list
    )  # workspace paths (a folder = every notebook under it)
    jobs: list[str] = field(default_factory=list)  # job ids
    clusters: list[str] = field(default_factory=list)  # cluster ids
    pipelines: list[str] = field(default_factory=list)  # DLT pipeline ids
    warehouses: list[str] = field(default_factory=list)  # SQL warehouse ids
    tables: list[str] = field(
        default_factory=list
    )  # catalog.schema.table, or catalog.schema for all its tables
    secret_scopes: list[str] = field(default_factory=list)  # scope names

    def is_empty(self) -> bool:
        return not any(
            (
                self.notebooks,
                self.jobs,
                self.clusters,
                self.pipelines,
                self.warehouses,
                self.tables,
                self.secret_scopes,
            )
        )


def summarize_assets(assets: list[dict[str, Any]]) -> dict[str, Any]:
    by_type: dict[str, int] = {}
    for a in assets:
        by_type[a["type"]] = by_type.get(a["type"], 0) + 1
    return {"total_assets": len(assets), "by_type": by_type}


# ── asset builders (one per Databricks object kind) ──
def notebook_asset(o: dict[str, Any]) -> dict[str, Any]:
    p = o.get("path", "")
    return {
        "type": "Notebook",
        "name": nb_name(p),
        "location": p,
        "meta": {
            "source_path": p,
            "language": o.get("language") or "PYTHON",
            "object_id": o.get("object_id"),
        },
    }


def job_asset(j: dict[str, Any]) -> dict[str, Any]:
    st = j.get("settings", {})
    return {
        "type": "Job",
        "name": st.get("name") or f"job_{j.get('job_id')}",
        "location": f"job:{j.get('job_id')}",
        "meta": {"job_id": j.get("job_id"), "settings": st},
    }


def table_asset(t: dict[str, Any]) -> dict[str, Any]:
    parts = sorted(
        (col for col in t.get("columns") or [] if col.get("partition_index") is not None),
        key=lambda col: col["partition_index"],
    )
    full = t.get("full_name") or f"{t.get('catalog_name')}.{t.get('schema_name')}.{t.get('name')}"
    return {
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


def cluster_asset(cl: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "Cluster",
        "name": cl.get("cluster_name") or cl.get("cluster_id"),
        "location": f"cluster:{cl.get('cluster_id')}",
        "meta": {
            "cluster_id": cl.get("cluster_id"),
            "spark_version": cl.get("spark_version"),
            "spark_conf": cl.get("spark_conf") or {},
            "node_type_id": cl.get("node_type_id"),
        },
    }


def scope_asset(name: str, keys: list[str]) -> dict[str, Any]:
    return {
        "type": "Secret Scope",
        "name": name,
        "location": f"scope:{name}",
        "meta": {"scope": name, "keys": keys},
    }


def pipeline_asset(p: dict[str, Any]) -> dict[str, Any]:
    spec = p.get("spec") or {}
    return {
        "type": "DLT Pipeline",
        "name": p.get("name") or spec.get("name") or p.get("pipeline_id"),
        "location": f"pipeline:{p.get('pipeline_id')}",
        "meta": {
            "pipeline_id": p.get("pipeline_id"),
            "state": p.get("state"),
            "catalog": spec.get("catalog"),
            "target": spec.get("target") or spec.get("schema"),
            "libraries": spec.get("libraries", []),
            "continuous": spec.get("continuous"),
        },
    }


def warehouse_asset(w: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "SQL Warehouse",
        "name": w.get("name") or w.get("id"),
        "location": f"warehouse:{w.get('id')}",
        "meta": {
            "warehouse_id": w.get("id"),
            "cluster_size": w.get("cluster_size"),
            "warehouse_type": w.get("warehouse_type"),
            "state": w.get("state"),
            "serverless": w.get("enable_serverless_compute"),
        },
    }


def list_notebooks(
    c: DatabricksClient, paths: list[str], max_notebooks: int, warn: Callable[[str], None]
) -> list[dict[str, Any]]:
    """Breadth-first walk of workspace folders/repos; at most ``max_notebooks`` notebooks."""
    out: list[dict[str, Any]] = []
    queue = list(dict.fromkeys(paths or ["/"]))
    seen: set[str] = set()
    while queue and len(out) < max_notebooks:
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
            kind = o.get("object_type")
            if kind in ("DIRECTORY", "REPO"):
                queue.append(o.get("path", ""))
            elif kind == "NOTEBOOK":
                out.append(notebook_asset(o))
                if len(out) >= max_notebooks:
                    warn(
                        f"notebook limit reached ({max_notebooks}); remaining notebooks were not inventoried"
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
                out += [table_asset(t) for t in c.iter_tables(cat, s["name"])]
            except DatabricksError as e:
                warn(f"schema {cat}.{s['name']}: {e.message}")
    return out


def _result(
    c: DatabricksClient, assets: list[dict[str, Any]], warnings: list[str], kind: str
) -> dict[str, Any]:
    for w in warnings:
        log.warning("discovery_warning", summary=w)
    return {
        "source": {"kind": "databricks", "host": c.creds.host, "inventory": kind},
        "summary": summarize_assets(assets),
        "warnings": warnings,
        "assets": assets,
    }


def discover(c: DatabricksClient, opts: DiscoveryOptions) -> dict[str, Any]:
    """Inventory the workspace. Partial failures (one schema, one path) become warnings, not errors."""
    warnings: list[str] = []
    warn = warnings.append
    assets = list_notebooks(c, opts.workspace_paths, opts.max_notebooks, warn)

    def section(enabled: bool, label: str, build: Callable[[], list[dict[str, Any]]]) -> None:
        if not enabled:
            return
        try:
            assets.extend(build())
        except DatabricksError as e:
            warn(f"{label}: {e.message}")

    section(opts.include_jobs, "jobs", lambda: [job_asset(j) for j in c.iter_jobs()])
    section(opts.include_tables, "unity catalog", lambda: _tables(c, opts, warn))
    section(
        opts.include_secret_scopes,
        "secret scopes",
        lambda: [
            scope_asset(sc["name"], [k["key"] for k in c.list_secret_keys(sc["name"])])
            for sc in c.list_secret_scopes()
        ],
    )
    # ephemeral job/pipeline clusters are covered by their job or pipeline
    section(
        opts.include_clusters,
        "clusters",
        lambda: [
            cluster_asset(cl)
            for cl in c.list_clusters()
            if cl.get("cluster_source") not in ("JOB", "PIPELINE")
        ],
    )
    section(
        opts.include_pipelines, "pipelines", lambda: [pipeline_asset(p) for p in c.iter_pipelines()]
    )
    section(
        opts.include_warehouses,
        "sql warehouses",
        lambda: [warehouse_asset(w) for w in c.list_warehouses()],
    )
    return _result(c, assets, warnings, "crawl")


def discover_selected(
    c: DatabricksClient, sel: Selection, max_notebooks: int = 5000
) -> dict[str, Any]:
    """Inventory exactly the selected objects (direct lookups, no crawl). Missing ones become warnings."""
    warnings: list[str] = []
    warn = warnings.append
    assets: list[dict[str, Any]] = []

    def each(label: str, keys: list[str], build: Callable[[str], list[dict[str, Any]]]) -> None:
        for k in dict.fromkeys(keys):
            try:
                assets.extend(build(k))
            except DatabricksError as e:
                warn(f"{label} {k}: {e.message}")

    def notebook_or_folder(path: str) -> list[dict[str, Any]]:
        st = c.get_status(path)
        if st.get("object_type") == "NOTEBOOK":
            return [notebook_asset(st)]
        if st.get("object_type") in ("DIRECTORY", "REPO"):
            return list_notebooks(c, [st.get("path") or path], max_notebooks, warn)
        warn(f"notebook {path}: is a {st.get('object_type')}, not a notebook or folder")
        return []

    def tables(name: str) -> list[dict[str, Any]]:
        parts = name.split(".")
        if len(parts) == 3:
            return [table_asset(c.get_table(name))]
        if len(parts) == 2:
            return [table_asset(t) for t in c.iter_tables(parts[0], parts[1])]
        warn(f"table {name}: use catalog.schema.table or catalog.schema")
        return []

    def pipeline(pid: str) -> list[dict[str, Any]]:
        p = c.get_pipeline(pid)
        return [
            pipeline_asset(
                {
                    "pipeline_id": p.get("pipeline_id", pid),
                    "name": p.get("name"),
                    "state": p.get("state"),
                    "spec": p.get("spec"),
                }
            )
        ]

    each("notebook", sel.notebooks, notebook_or_folder)
    each("job", sel.jobs, lambda j: [job_asset(c.get_job(j))])
    each("cluster", sel.clusters, lambda cid: [cluster_asset(c.get_cluster(cid))])
    each("pipeline", sel.pipelines, pipeline)
    each("warehouse", sel.warehouses, lambda wid: [warehouse_asset(c.get_warehouse(wid))])
    each("table", sel.tables, tables)
    each(
        "secret scope",
        sel.secret_scopes,
        lambda s: [scope_asset(s, [k["key"] for k in c.list_secret_keys(s)])],
    )
    seen: set[tuple[str, str]] = set()
    unique = []
    for a in assets:  # a folder and a notebook inside it may both be selected
        if (a["type"], a["location"]) not in seen:
            seen.add((a["type"], a["location"]))
            unique.append(a)
    return _result(c, unique, warnings, "selection")
