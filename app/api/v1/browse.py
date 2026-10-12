"""Browse API: list Databricks / Fabric objects to pick for a migration. Spec §5.10.

Read-only listings exposed as POST: the JSON body carries the credentials (ADR 0008: credentials
only ever travel in request bodies) plus filters; path parameters only identify a resource.
Databricks: service principal (client_id + client_secret) or personal_access_token, the service
principal winning when both are sent. Fabric: service principal only. With ALLOW_ENV_CREDENTIALS
(development only) ``credentials`` can be omitted. Every list says which ``source.selection``
key its ids go into.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Annotated, Any

import httpx
from fastapi import APIRouter, Depends

from app.api.deps import get_settings_dep
from app.core.config import Settings
from app.core.exceptions import ValidationFailed
from app.databricks.client import DatabricksClient
from app.databricks.discovery import (
    SYSTEM_CATALOGS,
    DiscoveryOptions,
    alert_asset,
    connection_asset,
    dashboard_asset,
    discover,
    external_location_asset,
    function_asset,
    library_assets,
    list_notebooks,
    model_asset,
    query_asset,
    repo_asset,
    serving_endpoint_asset,
    volume_asset,
)
from app.fabric.auth import acquire_token
from app.fabric.client import Fabric
from app.orchestrator.service import build_databricks_credentials, build_fabric_credentials
from app.schemas.browse import (
    BrowseList,
    CatalogsBrowseIn,
    ClustersBrowseIn,
    DatabricksBrowseIn,
    FabricBrowseIn,
    FabricItemsBrowseIn,
    FabricWorkspacesBrowseIn,
    InventoryBrowseIn,
    JobsBrowseIn,
    NameFilterBrowseIn,
    NotebooksBrowseIn,
    PipelinesBrowseIn,
    WorkspaceBrowseIn,
)

databricks_router = APIRouter(prefix="/databricks", tags=["browse: databricks"])
fabric_router = APIRouter(prefix="/fabric", tags=["browse: fabric"])

Cfg = Annotated[Settings, Depends(get_settings_dep)]
_GUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


@contextmanager
def _dbx(body: DatabricksBrowseIn, settings: Settings) -> Iterator[DatabricksClient]:
    creds = build_databricks_credentials(body.credentials, settings, field="credentials")
    if creds is None:
        raise ValidationFailed(
            "send credentials {host, client_id + client_secret} or {host, personal_access_token}",
            field="credentials",
        )
    client = DatabricksClient(creds, settings)
    try:
        yield client
    finally:
        client.close()


@contextmanager
def _fab(body: FabricBrowseIn, settings: Settings) -> Iterator[Fabric]:
    creds = build_fabric_credentials(body.credentials, settings, field="credentials")
    if creds is None:
        raise ValidationFailed(
            "send credentials {tenant_id, client_id, client_secret} (Fabric service principal)",
            field="credentials",
        )
    with httpx.Client(timeout=settings.FABRIC_TIMEOUT_S) as http:
        token = acquire_token(creds, settings, http)
    fab = Fabric(token, settings)
    try:
        yield fab
    finally:
        fab.close()


def _list(
    items: list[dict[str, Any]],
    key: str | None = None,
    id_field: str | None = None,
    *,
    limit: int | None = None,
    warnings: list[str] | None = None,
) -> BrowseList:
    truncated = limit is not None and len(items) > limit
    items = items[:limit] if limit is not None else items
    return BrowseList(
        count=len(items),
        items=items,
        selection_key=key,
        id_field=id_field,
        truncated=truncated,
        warnings=warnings or [],
    )


def _contains(value: Any, needle: str | None) -> bool:
    return not needle or needle.lower() in str(value or "").lower()


# ── Databricks ──────────────────────────────────────────────
@databricks_router.post("/workspace", response_model=BrowseList)
def browse_workspace(body: WorkspaceBrowseIn, settings: Cfg) -> BrowseList:
    """One folder level (folders, repos, notebooks, files): for tree pickers."""
    with _dbx(body, settings) as c:
        items = [
            {
                "path": o.get("path"),
                "object_type": o.get("object_type"),
                "language": o.get("language"),
                "object_id": o.get("object_id"),
            }
            for o in c.list_workspace(body.path)
        ]
    return _list(
        sorted(items, key=lambda o: (o["object_type"] != "DIRECTORY", str(o["path"]))),
        "notebooks",
        "path",
    )


@databricks_router.post("/notebooks", response_model=BrowseList)
def browse_notebooks(body: NotebooksBrowseIn, settings: Cfg) -> BrowseList:
    """Every notebook under the given folder(s), recursively."""
    warnings: list[str] = []
    with _dbx(body, settings) as c:
        found = list_notebooks(
            c, body.paths, min(body.limit, settings.DATABRICKS_MAX_NOTEBOOKS) + 1, warnings.append
        )
    items = [
        {
            "path": a["location"],
            "name": a["name"],
            "language": a["meta"]["language"],
            "object_id": a["meta"]["object_id"],
        }
        for a in found
    ]
    return _list(
        items,
        "notebooks",
        "path",
        limit=body.limit,
        warnings=[w for w in warnings if "limit reached" not in w],
    )


@databricks_router.post("/jobs", response_model=BrowseList)
def browse_jobs(body: JobsBrowseIn, settings: Cfg) -> BrowseList:
    items = []
    with _dbx(body, settings) as c:
        for j in c.iter_jobs():
            st = j.get("settings", {})
            if not _contains(st.get("name"), body.name):
                continue
            tasks = st.get("tasks", [])
            items.append(
                {
                    "job_id": j.get("job_id"),
                    "name": st.get("name"),
                    "creator": j.get("creator_user_name"),
                    "tasks": len(tasks),
                    "task_types": sorted({k for t in tasks for k in t if k.endswith("_task")}),
                    "notebook_paths": [
                        t["notebook_task"].get("notebook_path")
                        for t in tasks
                        if "notebook_task" in t
                    ],
                    "schedule": (st.get("schedule") or {}).get("quartz_cron_expression"),
                    "paused": (st.get("schedule") or {}).get("pause_status") == "PAUSED",
                }
            )
            if len(items) > body.limit:
                break
    return _list(items, "jobs", "job_id", limit=body.limit)


@databricks_router.post("/jobs/{job_id}")
def get_job(job_id: str, body: DatabricksBrowseIn, settings: Cfg) -> dict[str, Any]:
    """Full job definition (settings, tasks, schedule) exactly as the migration will read it."""
    with _dbx(body, settings) as c:
        return c.get_job(job_id)


@databricks_router.post("/clusters", response_model=BrowseList)
def browse_clusters(body: ClustersBrowseIn, settings: Cfg) -> BrowseList:
    with _dbx(body, settings) as c:
        items = [
            {
                "cluster_id": cl.get("cluster_id"),
                "cluster_name": cl.get("cluster_name"),
                "state": cl.get("state"),
                "spark_version": cl.get("spark_version"),
                "node_type_id": cl.get("node_type_id"),
                "num_workers": cl.get("num_workers"),
                "autoscale": cl.get("autoscale"),
                "source": cl.get("cluster_source"),
            }
            for cl in c.list_clusters()
            if body.include_job_clusters or cl.get("cluster_source") not in ("JOB", "PIPELINE")
        ]
    return _list(items, "clusters", "cluster_id")


@databricks_router.post("/pipelines", response_model=BrowseList)
def browse_pipelines(body: PipelinesBrowseIn, settings: Cfg) -> BrowseList:
    """DLT / Lakeflow declarative pipelines (migrated as manual redesign items with AI work orders)."""
    with _dbx(body, settings) as c:
        items = [
            {
                "pipeline_id": p.get("pipeline_id"),
                "name": p.get("name"),
                "state": p.get("state"),
                "creator": p.get("creator_user_name"),
            }
            for p in c.iter_pipelines()
            if _contains(p.get("name"), body.name)
        ]
    return _list(items, "pipelines", "pipeline_id")


@databricks_router.post("/warehouses", response_model=BrowseList)
def browse_warehouses(body: DatabricksBrowseIn, settings: Cfg) -> BrowseList:
    """SQL warehouses: migrate them as items, and/or pass one as source.warehouse_id for copy_data."""
    with _dbx(body, settings) as c:
        items = [
            {
                "id": w.get("id"),
                "name": w.get("name"),
                "state": w.get("state"),
                "cluster_size": w.get("cluster_size"),
                "warehouse_type": w.get("warehouse_type"),
                "serverless": w.get("enable_serverless_compute"),
            }
            for w in c.list_warehouses()
        ]
    return _list(items, "warehouses", "id")


@databricks_router.post("/catalogs", response_model=BrowseList)
def browse_catalogs(body: CatalogsBrowseIn, settings: Cfg) -> BrowseList:
    with _dbx(body, settings) as c:
        items = [
            {
                "name": x.get("name"),
                "catalog_type": x.get("catalog_type"),
                "comment": x.get("comment"),
                "owner": x.get("owner"),
            }
            for x in c.list_catalogs()
            if body.include_system or x.get("name") not in SYSTEM_CATALOGS
        ]
    return _list(items)


@databricks_router.post("/catalogs/{catalog}/schemas", response_model=BrowseList)
def browse_schemas(catalog: str, body: DatabricksBrowseIn, settings: Cfg) -> BrowseList:
    """Schemas of a catalog; put ``catalog.schema`` into selection.tables to migrate all of its tables."""
    with _dbx(body, settings) as c:
        items = [
            {
                "name": s.get("name"),
                "full_name": s.get("full_name") or f"{catalog}.{s.get('name')}",
                "comment": s.get("comment"),
            }
            for s in c.list_schemas(catalog)
            if s.get("name") != "information_schema"
        ]
    return _list(items, "tables", "full_name")


@databricks_router.post("/catalogs/{catalog}/schemas/{schema}/tables", response_model=BrowseList)
def browse_tables(catalog: str, schema: str, body: DatabricksBrowseIn, settings: Cfg) -> BrowseList:
    with _dbx(body, settings) as c:
        items = [
            {
                "full_name": t.get("full_name"),
                "name": t.get("name"),
                "table_type": t.get("table_type"),
                "format": t.get("data_source_format"),
                "storage_location": t.get("storage_location"),
                "columns": len(t.get("columns") or []),
                "owner": t.get("owner"),
            }
            for t in c.iter_tables(catalog, schema)
        ]
    return _list(items, "tables", "full_name")


@databricks_router.post("/secret-scopes", response_model=BrowseList)
def browse_secret_scopes(body: DatabricksBrowseIn, settings: Cfg) -> BrowseList:
    """Scope and key NAMES (Databricks never returns secret values; recreate them in Key Vault)."""
    with _dbx(body, settings) as c:
        items = [
            {
                "name": sc.get("name"),
                "backend_type": sc.get("backend_type"),
                "keys": [k.get("key") for k in c.list_secret_keys(sc["name"])],
            }
            for sc in c.list_secret_scopes()
        ]
    return _list(items, "secret_scopes", "name")


def _assets(
    assets: list[dict[str, Any]], key: str, id_field: str, name: str | None = None
) -> BrowseList:
    """Items built by the same asset builders discovery uses, plus an ``id`` (``meta[id_field]``,
    else the asset name) that goes into ``source.selection.<key>``."""
    items = [
        {"id": str(a["meta"].get(id_field) or a["name"]), **a}
        for a in assets
        if _contains(a["name"], name)
    ]
    return _list(items, key, "id")


def _schema_objects(
    kind: str, catalog: str, schema: str, body: DatabricksBrowseIn, settings: Settings
) -> BrowseList:
    with _dbx(body, settings) as c:
        iterate, build = {
            "volumes": (c.iter_volumes, volume_asset),
            "functions": (c.iter_functions, function_asset),
            "models": (c.iter_models, model_asset),
        }[kind]
        return _assets([build(x) for x in iterate(catalog, schema)], kind, "name")


@databricks_router.post("/catalogs/{catalog}/schemas/{schema}/volumes", response_model=BrowseList)
def browse_volumes(
    catalog: str, schema: str, body: DatabricksBrowseIn, settings: Cfg
) -> BrowseList:
    """Unity Catalog volumes (-> lakehouse Files: shortcut for external, copy for managed)."""
    return _schema_objects("volumes", catalog, schema, body, settings)


@databricks_router.post("/catalogs/{catalog}/schemas/{schema}/functions", response_model=BrowseList)
def browse_functions(
    catalog: str, schema: str, body: DatabricksBrowseIn, settings: Cfg
) -> BrowseList:
    """UC functions (scalar SQL -> Spark function or Warehouse T-SQL; others manual)."""
    return _schema_objects("functions", catalog, schema, body, settings)


@databricks_router.post("/catalogs/{catalog}/schemas/{schema}/models", response_model=BrowseList)
def browse_models(catalog: str, schema: str, body: DatabricksBrowseIn, settings: Cfg) -> BrowseList:
    """UC registered models (-> Fabric ML model; artifacts re-registered manually)."""
    return _schema_objects("models", catalog, schema, body, settings)


@databricks_router.post("/queries", response_model=BrowseList)
def browse_queries(body: NameFilterBrowseIn, settings: Cfg) -> BrowseList:
    """Saved SQL queries (-> cells of the Migrated_SQL_Queries notebook)."""
    with _dbx(body, settings) as c:
        return _assets([query_asset(q) for q in c.iter_queries()], "queries", "query_id", body.name)


@databricks_router.post("/alerts", response_model=BrowseList)
def browse_alerts(body: NameFilterBrowseIn, settings: Cfg) -> BrowseList:
    """SQL alerts (-> Fabric Activator; manual)."""
    with _dbx(body, settings) as c:
        return _assets([alert_asset(a) for a in c.iter_alerts()], "alerts", "alert_id", body.name)


@databricks_router.post("/dashboards", response_model=BrowseList)
def browse_dashboards(body: NameFilterBrowseIn, settings: Cfg) -> BrowseList:
    """AI/BI (Lakeview) dashboards (-> Power BI report; manual)."""
    with _dbx(body, settings) as c:
        return _assets(
            [dashboard_asset(d) for d in c.iter_dashboards()],
            "dashboards",
            "dashboard_id",
            body.name,
        )


@databricks_router.post("/serving-endpoints", response_model=BrowseList)
def browse_serving_endpoints(body: NameFilterBrowseIn, settings: Cfg) -> BrowseList:
    """Model serving endpoints (-> Azure ML / AI Foundry; manual)."""
    with _dbx(body, settings) as c:
        return _assets(
            [serving_endpoint_asset(e) for e in c.list_serving_endpoints()],
            "serving_endpoints",
            "name",
            body.name,
        )


@databricks_router.post("/repos", response_model=BrowseList)
def browse_repos(body: NameFilterBrowseIn, settings: Cfg) -> BrowseList:
    """Git folders (-> Fabric workspace Git integration; manual). Notebooks inside them are
    migrated through selection.notebooks with the repo path."""
    with _dbx(body, settings) as c:
        return _assets([repo_asset(r) for r in c.iter_repos()], "repos", "repo_id", body.name)


@databricks_router.post("/external-locations", response_model=BrowseList)
def browse_external_locations(body: NameFilterBrowseIn, settings: Cfg) -> BrowseList:
    """UC external locations (-> ADLS Gen2 connection used by OneLake shortcuts)."""
    with _dbx(body, settings) as c:
        return _assets(
            [external_location_asset(e) for e in c.iter_external_locations()],
            "external_locations",
            "name",
            body.name,
        )


@databricks_router.post("/connections", response_model=BrowseList)
def browse_connections(body: NameFilterBrowseIn, settings: Cfg) -> BrowseList:
    """Lakehouse Federation connections (endpoint options only; credentials are never returned)."""
    with _dbx(body, settings) as c:
        return _assets(
            [connection_asset(cn) for cn in c.iter_connections()], "connections", "name", body.name
        )


@databricks_router.post("/libraries", response_model=BrowseList)
def browse_libraries(body: DatabricksBrowseIn, settings: Cfg) -> BrowseList:
    """Libraries installed on clusters (-> Fabric Environment). They are migrated with the
    cluster they belong to: select the cluster under selection.clusters."""
    with _dbx(body, settings) as c:
        items = library_assets(c.all_cluster_libraries())
    for it in items:
        it["id"] = it["name"]
    return _list(items, "clusters", "meta.clusters")


@databricks_router.post("/workspace-files", response_model=BrowseList)
def browse_workspace_files(body: NotebooksBrowseIn, settings: Cfg) -> BrowseList:
    """Non-notebook workspace files (.py modules, configs, ...) under the given folders."""
    warnings: list[str] = []
    files: list[dict[str, Any]] = []
    with _dbx(body, settings) as c:
        list_notebooks(c, body.paths, settings.DATABRICKS_MAX_NOTEBOOKS, warnings.append, files)
    items = [{"id": f["location"], **f} for f in files]
    return _list(items, "workspace_files", "id", limit=body.limit, warnings=warnings)


@databricks_router.post("/inventory")
def browse_inventory(body: InventoryBrowseIn, settings: Cfg) -> dict[str, Any]:
    """Everything at once, in the engine's assessment format. Send the response (or a filtered
    copy of its ``assets``) as ``assessment`` in ``POST /migrations`` to skip discovery entirely."""
    with _dbx(body, settings) as c:
        return discover(
            c,
            DiscoveryOptions(
                workspace_paths=body.workspace_paths,
                catalogs=body.catalogs,
                schemas=body.schemas,
                **body.flags(),
                max_notebooks=settings.DATABRICKS_MAX_NOTEBOOKS,
            ),
        )


# ── Fabric (choose the target) ──────────────────────────────
def _ws(fab: Fabric, workspace: str) -> str:
    return workspace if _GUID.match(workspace) else fab.find_workspace(workspace)


@fabric_router.post("/workspaces", response_model=BrowseList)
def browse_fabric_workspaces(body: FabricWorkspacesBrowseIn, settings: Cfg) -> BrowseList:
    with _fab(body, settings) as fab:
        items = [
            {
                "id": w.get("id"),
                "name": w.get("displayName"),
                "type": w.get("type"),
                "capacity_id": w.get("capacityId"),
            }
            for w in fab.list_workspaces()
            if _contains(w.get("displayName"), body.name)
        ]
    return _list(items)


@fabric_router.post("/workspaces/{workspace}/lakehouses", response_model=BrowseList)
def browse_fabric_lakehouses(workspace: str, body: FabricBrowseIn, settings: Cfg) -> BrowseList:
    """Lakehouses of a workspace (id or display name); use one as ``target.lakehouse``."""
    items = []
    with _fab(body, settings) as fab:
        for lh in fab.list_lakehouses(_ws(fab, workspace)):
            props = lh.get("properties") or {}
            items.append(
                {
                    "id": lh.get("id"),
                    "name": lh.get("displayName"),
                    "schemas_enabled": bool(props.get("defaultSchema")),
                    "tables_path": props.get("oneLakeTablesPath"),
                    "files_path": props.get("oneLakeFilesPath"),
                    "sql_endpoint": (props.get("sqlEndpointProperties") or {}).get(
                        "connectionString"
                    ),
                }
            )
    return _list(items)


@fabric_router.post("/workspaces/{workspace}/items", response_model=BrowseList)
def browse_fabric_items(workspace: str, body: FabricItemsBrowseIn, settings: Cfg) -> BrowseList:
    """Items already in the workspace (e.g. type=Notebook, DataPipeline, Warehouse)."""
    with _fab(body, settings) as fab:
        items = [
            {"id": i.get("id"), "name": i.get("displayName"), "type": i.get("type")}
            for i in fab.list_items(_ws(fab, workspace), body.type)
        ]
    return _list(items)
