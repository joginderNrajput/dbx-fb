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
    "alert_asset",
    "cluster_asset",
    "connection_asset",
    "dashboard_asset",
    "discover",
    "discover_selected",
    "external_location_asset",
    "function_asset",
    "job_asset",
    "library_assets",
    "list_notebooks",
    "model_asset",
    "notebook_asset",
    "pipeline_asset",
    "query_asset",
    "repo_asset",
    "scope_asset",
    "serving_endpoint_asset",
    "summarize_assets",
    "table_asset",
    "volume_asset",
    "warehouse_asset",
    "workspace_file_asset",
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
    include_volumes: bool = True
    include_functions: bool = True
    include_models: bool = True
    include_sql_assets: bool = True  # SQL queries, alerts, AI/BI (Lakeview) dashboards
    include_external_locations: bool = True
    include_connections: bool = True
    include_serving_endpoints: bool = True
    include_repos: bool = True
    include_libraries: bool = True  # libraries installed on all-purpose clusters
    include_workspace_files: bool = True  # non-notebook workspace files under workspace_paths
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
    # catalog.schema.name, or catalog.schema for every one in the schema
    volumes: list[str] = field(default_factory=list)
    functions: list[str] = field(default_factory=list)
    models: list[str] = field(default_factory=list)
    queries: list[str] = field(default_factory=list)  # SQL query ids
    alerts: list[str] = field(default_factory=list)  # SQL alert ids
    dashboards: list[str] = field(default_factory=list)  # Lakeview dashboard ids
    serving_endpoints: list[str] = field(default_factory=list)  # endpoint names
    repos: list[str] = field(default_factory=list)  # repo ids
    external_locations: list[str] = field(default_factory=list)  # names
    connections: list[str] = field(default_factory=list)  # names
    workspace_files: list[str] = field(default_factory=list)  # workspace file paths

    def is_empty(self) -> bool:
        return not any(getattr(self, f) for f in self.__dataclass_fields__)


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


def volume_asset(v: dict[str, Any]) -> dict[str, Any]:
    full = v.get("full_name") or f"{v.get('catalog_name')}.{v.get('schema_name')}.{v.get('name')}"
    return {
        "type": "Volume",
        "name": full,
        "location": f"volume:{full}",
        "meta": {
            "volume_type": v.get("volume_type"),
            "storage_location": v.get("storage_location"),
        },
    }


def function_asset(f: dict[str, Any]) -> dict[str, Any]:
    """UC function -> the assessor's shape (language, input_params, return_type, definition)."""
    full = f.get("full_name") or f"{f.get('catalog_name')}.{f.get('schema_name')}.{f.get('name')}"
    body = f.get("routine_body")
    params = (f.get("input_params") or {}).get("parameters")
    return {
        "type": "Function",
        "name": full,
        "location": f"function:{full}",
        "meta": {
            "language": "SQL" if body == "SQL" else (f.get("external_language") or body),
            "input_params": [
                {"name": p.get("name"), "type": p.get("type_text") or p.get("type_name")}
                for p in params
            ]
            if params is not None
            else [],
            "return_type": f.get("full_data_type") or f.get("data_type"),
            "definition": f.get("routine_definition"),
            "is_table": f.get("data_type") == "TABLE_TYPE",
        },
    }


def model_asset(m: dict[str, Any]) -> dict[str, Any]:
    full = m.get("full_name") or f"{m.get('catalog_name')}.{m.get('schema_name')}.{m.get('name')}"
    return {
        "type": "ML Model",
        "name": full,
        "location": f"model:{full}",
        "meta": {"storage_location": m.get("storage_location"), "comment": m.get("comment")},
    }


def external_location_asset(e: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "External Location",
        "name": e.get("name"),
        "location": e.get("url") or f"extloc:{e.get('name')}",
        "meta": {
            "url": e.get("url"),
            "credential_name": e.get("credential_name"),
            "read_only": e.get("read_only"),
        },
    }


# Connection options that describe the endpoint; anything else may be a secret and is dropped.
_SAFE_CONNECTION_OPTIONS = ("host", "port", "database", "httppath", "warehouse")


def connection_asset(cn: dict[str, Any]) -> dict[str, Any]:
    opts = cn.get("options") or {}
    return {
        "type": "Connection",
        "name": cn.get("name"),
        "location": f"connection:{cn.get('name')}",
        "meta": {
            "connection_type": cn.get("connection_type"),
            "options": {k: opts[k] for k in _SAFE_CONNECTION_OPTIONS if k in opts},
        },
    }


def query_asset(q: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "SQL Query",
        "name": q.get("display_name") or q.get("name") or f"query_{q.get('id')}",
        "location": f"query:{q.get('id')}",
        "meta": {
            "query_id": q.get("id"),
            "query_text": q.get("query_text") or q.get("query") or "",
            "warehouse_id": q.get("warehouse_id"),
            "catalog": q.get("catalog"),
            "schema": q.get("schema"),
        },
    }


def alert_asset(a: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "Alert",
        "name": a.get("display_name") or a.get("name") or f"alert_{a.get('id')}",
        "location": f"alert:{a.get('id')}",
        "meta": {
            "alert_id": a.get("id"),
            "query_id": a.get("query_id"),
            "condition": a.get("condition"),
            "state": a.get("state"),
        },
    }


def dashboard_asset(d: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "Dashboard",
        "name": d.get("display_name") or f"dashboard_{d.get('dashboard_id')}",
        "location": f"dashboard:{d.get('dashboard_id')}",
        "meta": {
            "dashboard_id": d.get("dashboard_id"),
            "path": d.get("path"),
            "warehouse_id": d.get("warehouse_id"),
            "lifecycle_state": d.get("lifecycle_state"),
        },
    }


def serving_endpoint_asset(e: dict[str, Any]) -> dict[str, Any]:
    cfg = e.get("config") or {}
    entities = cfg.get("served_entities") or cfg.get("served_models") or []
    return {
        "type": "Serving Endpoint",
        "name": e.get("name"),
        "location": f"serving:{e.get('name')}",
        "meta": {
            "served_entities": [
                {
                    "name": x.get("entity_name") or x.get("model_name"),
                    "version": x.get("entity_version") or x.get("model_version"),
                }
                for x in entities
            ],
            "state": (e.get("state") or {}).get("ready"),
        },
    }


def repo_asset(r: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "Repo",
        "name": r.get("path") or f"repo_{r.get('id')}",
        "location": f"repo:{r.get('id')}",
        "meta": {
            "repo_id": r.get("id"),
            "url": r.get("url"),
            "provider": r.get("provider"),
            "branch": r.get("branch"),
        },
    }


def workspace_file_asset(o: dict[str, Any]) -> dict[str, Any]:
    p = o.get("path", "")
    return {
        "type": "Workspace File",
        "name": p,
        "location": p,
        "meta": {"source_path": p, "size": o.get("size")},
    }


def _library(lib: dict[str, Any]) -> tuple[str, str] | None:
    """{"pypi": {"package": "x==1"}} -> ("pypi", "x==1"); jar/whl/egg/requirements are paths."""
    for kind, v in lib.items():
        if isinstance(v, dict):
            name = v.get("package") or v.get("coordinates")
            if name:
                return kind, str(name)
        elif isinstance(v, str) and v:
            return kind, v
    return None


def library_assets(statuses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Cluster library statuses -> one Library asset per (kind, name), listing every cluster."""
    found: dict[tuple[str, str], dict[str, Any]] = {}
    for st in statuses:
        cid = st.get("cluster_id")
        for ls in st.get("library_statuses", [st]):
            parsed = _library(ls.get("library") or {})
            if not parsed:
                continue
            a = found.setdefault(
                parsed,
                {
                    "type": "Library",
                    "name": parsed[1],
                    "location": f"library:{parsed[0]}:{parsed[1]}",
                    "meta": {"kind": parsed[0], "name": parsed[1], "clusters": []},
                },
            )
            if cid and cid not in a["meta"]["clusters"]:
                a["meta"]["clusters"].append(cid)
    return list(found.values())


def list_notebooks(
    c: DatabricksClient,
    paths: list[str],
    max_notebooks: int,
    warn: Callable[[str], None],
    files: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Breadth-first walk of workspace folders/repos; at most ``max_notebooks`` notebooks.

    When ``files`` is given, non-notebook workspace files found on the way are appended to it.
    """
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
            elif kind == "FILE" and files is not None:
                files.append(workspace_file_asset(o))
            elif kind == "NOTEBOOK":
                out.append(notebook_asset(o))
                if len(out) >= max_notebooks:
                    warn(
                        f"notebook limit reached ({max_notebooks}); remaining notebooks were not inventoried"
                    )
                    break
    return out


def _catalog_objects(
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
            sn = s["name"]
            kinds: list[tuple[bool, str, Callable[[str, str], Any], Callable[..., Any]]] = [
                (opts.include_tables, "tables", c.iter_tables, table_asset),
                (opts.include_volumes, "volumes", c.iter_volumes, volume_asset),
                (opts.include_functions, "functions", c.iter_functions, function_asset),
                (opts.include_models, "models", c.iter_models, model_asset),
            ]
            for enabled, label, iterate, build in kinds:
                if not enabled:
                    continue
                try:
                    out += [build(x) for x in iterate(cat, sn)]
                except DatabricksError as e:
                    warn(f"{label} in {cat}.{sn}: {e.message}")
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
    files: list[dict[str, Any]] = []
    assets = list_notebooks(
        c,
        opts.workspace_paths,
        opts.max_notebooks,
        warn,
        files if opts.include_workspace_files else None,
    )
    assets += files

    def section(enabled: bool, label: str, build: Callable[[], list[dict[str, Any]]]) -> None:
        if not enabled:
            return
        try:
            assets.extend(build())
        except DatabricksError as e:
            warn(f"{label}: {e.message}")

    section(opts.include_jobs, "jobs", lambda: [job_asset(j) for j in c.iter_jobs()])
    section(
        opts.include_tables
        or opts.include_volumes
        or opts.include_functions
        or opts.include_models,
        "unity catalog",
        lambda: _catalog_objects(c, opts, warn),
    )
    section(
        opts.include_external_locations,
        "external locations",
        lambda: [external_location_asset(e) for e in c.iter_external_locations()],
    )
    section(
        opts.include_connections,
        "connections",
        lambda: [connection_asset(cn) for cn in c.iter_connections()],
    )
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
    section(
        opts.include_sql_assets, "sql queries", lambda: [query_asset(q) for q in c.iter_queries()]
    )
    section(
        opts.include_sql_assets, "sql alerts", lambda: [alert_asset(a) for a in c.iter_alerts()]
    )
    section(
        opts.include_sql_assets,
        "dashboards",
        lambda: [dashboard_asset(d) for d in c.iter_dashboards()],
    )
    section(
        opts.include_serving_endpoints,
        "serving endpoints",
        lambda: [serving_endpoint_asset(e) for e in c.list_serving_endpoints()],
    )
    section(opts.include_repos, "repos", lambda: [repo_asset(r) for r in c.iter_repos()])
    section(
        opts.include_libraries,
        "cluster libraries",
        lambda: library_assets(c.all_cluster_libraries()),
    )
    return _result(c, _unique(assets), warnings, "crawl")


def _unique(assets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop repeats of the same object (a folder and a notebook inside it may both be selected)."""
    seen: set[tuple[str, str]] = set()
    out = []
    for a in assets:
        if (a["type"], a["location"]) not in seen:
            seen.add((a["type"], a["location"]))
            out.append(a)
    return out


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
        if st.get("object_type") == "FILE":
            return [workspace_file_asset(st)]
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

    def in_schema(
        label: str,
        get: Callable[[str], dict[str, Any]],
        iterate: Callable[[str, str], Any],
        build: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> Callable[[str], list[dict[str, Any]]]:
        def run(name: str) -> list[dict[str, Any]]:
            parts = name.split(".")
            if len(parts) == 3:
                return [build(get(name))]
            if len(parts) == 2:
                return [build(x) for x in iterate(parts[0], parts[1])]
            warn(f"{label} {name}: use catalog.schema.name or catalog.schema")
            return []

        return run

    def workspace_file(path: str) -> list[dict[str, Any]]:
        st = c.get_status(path)
        if st.get("object_type") != "FILE":
            warn(f"workspace file {path}: is a {st.get('object_type')}, not a file")
            return []
        return [workspace_file_asset(st)]

    def cluster(cid: str) -> list[dict[str, Any]]:
        out = [cluster_asset(c.get_cluster(cid))]
        try:  # the cluster's libraries become Environment entries
            out += library_assets(
                [{"cluster_id": cid, "library_statuses": c.cluster_libraries(cid)}]
            )
        except DatabricksError as e:
            warn(f"cluster {cid} libraries: {e.message}")
        return out

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
    each("cluster", sel.clusters, cluster)
    each("pipeline", sel.pipelines, pipeline)
    each("warehouse", sel.warehouses, lambda wid: [warehouse_asset(c.get_warehouse(wid))])
    each("table", sel.tables, tables)
    each(
        "secret scope",
        sel.secret_scopes,
        lambda s: [scope_asset(s, [k["key"] for k in c.list_secret_keys(s)])],
    )
    each("volume", sel.volumes, in_schema("volume", c.get_volume, c.iter_volumes, volume_asset))
    each(
        "function",
        sel.functions,
        in_schema("function", c.get_function, c.iter_functions, function_asset),
    )
    each("model", sel.models, in_schema("model", c.get_model, c.iter_models, model_asset))
    each("query", sel.queries, lambda q: [query_asset(c.get_query(q))])
    each("alert", sel.alerts, lambda a: [alert_asset(c.get_alert(a))])
    each("dashboard", sel.dashboards, lambda d: [dashboard_asset(c.get_dashboard(d))])
    each(
        "serving endpoint",
        sel.serving_endpoints,
        lambda e: [serving_endpoint_asset(c.get_serving_endpoint(e))],
    )
    each("repo", sel.repos, lambda r: [repo_asset(c.get_repo(r))])
    each(
        "external location",
        sel.external_locations,
        lambda e: [external_location_asset(c.get_external_location(e))],
    )
    each("connection", sel.connections, lambda n: [connection_asset(c.get_connection(n))])
    each("workspace file", sel.workspace_files, workspace_file)
    return _result(c, _unique(assets), warnings, "selection")
