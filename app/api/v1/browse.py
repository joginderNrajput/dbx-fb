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
from app.databricks.discovery import SYSTEM_CATALOGS, DiscoveryOptions, discover, list_notebooks
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
                include_jobs=body.include_jobs,
                include_tables=body.include_tables,
                include_secret_scopes=body.include_secret_scopes,
                include_clusters=body.include_clusters,
                include_pipelines=body.include_pipelines,
                include_warehouses=body.include_warehouses,
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
