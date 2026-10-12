"""Browse API: list Databricks / Fabric objects to pick for a migration. Spec §5.10.

Read-only GETs. Credentials travel in headers (never in the query string, which access logs
record): ``X-Databricks-Host`` + ``X-Databricks-Token`` (or ``X-Databricks-Client-Id`` +
``X-Databricks-Client-Secret``), and ``X-Fabric-Tenant-Id`` / ``X-Fabric-Client-Id`` /
``X-Fabric-Client-Secret`` (or ``X-Fabric-Access-Token``). With ALLOW_ENV_CREDENTIALS (dev only)
they can be omitted. Every list says which ``source.selection`` key its ids go into.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from typing import Annotated, Any

import httpx
from fastapi import APIRouter, Depends, Header, Query
from pydantic import SecretStr

from app.api.deps import get_settings_dep
from app.core.config import Settings
from app.core.exceptions import ValidationFailed
from app.databricks.client import DatabricksClient
from app.databricks.discovery import SYSTEM_CATALOGS, DiscoveryOptions, discover, list_notebooks
from app.fabric.auth import acquire_token
from app.fabric.client import Fabric
from app.orchestrator.service import build_databricks_credentials, build_fabric_credentials
from app.schemas.browse import BrowseList
from app.schemas.credentials import DatabricksCredentialsIn, FabricCredentialsIn

databricks_router = APIRouter(prefix="/databricks", tags=["browse: databricks"])
fabric_router = APIRouter(prefix="/fabric", tags=["browse: fabric"])

Cfg = Annotated[Settings, Depends(get_settings_dep)]
_GUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def _secret(v: str | None) -> SecretStr | None:
    return SecretStr(v) if v else None


def databricks_client(
    settings: Cfg,
    x_databricks_host: Annotated[str | None, Header(description="Workspace URL")] = None,
    x_databricks_token: Annotated[str | None, Header(description="Personal access token")] = None,
    x_databricks_client_id: Annotated[
        str | None, Header(description="OAuth M2M service principal id")
    ] = None,
    x_databricks_client_secret: Annotated[
        str | None, Header(description="OAuth M2M secret")
    ] = None,
) -> Iterator[DatabricksClient]:
    creds = build_databricks_credentials(
        DatabricksCredentialsIn(
            host=x_databricks_host or "",
            token=_secret(x_databricks_token),
            client_id=x_databricks_client_id,
            client_secret=_secret(x_databricks_client_secret),
        ),
        settings,
        field="X-Databricks",
    )
    if creds is None:
        raise ValidationFailed(
            "send X-Databricks-Host and X-Databricks-Token (or X-Databricks-Client-Id / -Secret) headers",
            field="X-Databricks-Host",
        )
    client = DatabricksClient(creds, settings)
    try:
        yield client
    finally:
        client.close()


def fabric_client(
    settings: Cfg,
    x_fabric_tenant_id: Annotated[str | None, Header()] = None,
    x_fabric_client_id: Annotated[str | None, Header()] = None,
    x_fabric_client_secret: Annotated[str | None, Header()] = None,
    x_fabric_access_token: Annotated[str | None, Header()] = None,
) -> Iterator[Fabric]:
    creds = build_fabric_credentials(
        FabricCredentialsIn(
            access_token=_secret(x_fabric_access_token),
            tenant_id=x_fabric_tenant_id,
            client_id=x_fabric_client_id,
            client_secret=_secret(x_fabric_client_secret),
        ),
        settings,
        field="X-Fabric",
    )
    if creds is None:
        raise ValidationFailed(
            "send X-Fabric-Tenant-Id, X-Fabric-Client-Id and X-Fabric-Client-Secret (or X-Fabric-Access-Token) headers",
            field="X-Fabric-Client-Id",
        )
    with httpx.Client(timeout=settings.FABRIC_TIMEOUT_S) as http:
        token = acquire_token(creds, settings, http)
    fab = Fabric(token, settings)
    try:
        yield fab
    finally:
        fab.close()


Dbx = Annotated[DatabricksClient, Depends(databricks_client)]
Fab = Annotated[Fabric, Depends(fabric_client)]


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
@databricks_router.get("/workspace", response_model=BrowseList)
def browse_workspace(c: Dbx, path: str = "/") -> BrowseList:
    """One folder level (folders, repos, notebooks, files): for tree pickers."""
    items = [
        {
            "path": o.get("path"),
            "object_type": o.get("object_type"),
            "language": o.get("language"),
            "object_id": o.get("object_id"),
        }
        for o in c.list_workspace(path)
    ]
    return _list(
        sorted(items, key=lambda o: (o["object_type"] != "DIRECTORY", str(o["path"]))),
        "notebooks",
        "path",
    )


@databricks_router.get("/notebooks", response_model=BrowseList)
def browse_notebooks(
    c: Dbx,
    settings: Cfg,
    path: Annotated[list[str], Query()] = ["/"],  # noqa: B006
    limit: Annotated[int, Query(ge=1, le=20000)] = 1000,
) -> BrowseList:
    """Every notebook under the given folder(s), recursively."""
    warnings: list[str] = []
    found = list_notebooks(
        c, path, min(limit, settings.DATABRICKS_MAX_NOTEBOOKS) + 1, warnings.append
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
        limit=limit,
        warnings=[w for w in warnings if "limit reached" not in w],
    )


@databricks_router.get("/jobs", response_model=BrowseList)
def browse_jobs(
    c: Dbx, name: str | None = None, limit: Annotated[int, Query(ge=1, le=10000)] = 1000
) -> BrowseList:
    items = []
    for j in c.iter_jobs():
        st = j.get("settings", {})
        if not _contains(st.get("name"), name):
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
                    t["notebook_task"].get("notebook_path") for t in tasks if "notebook_task" in t
                ],
                "schedule": (st.get("schedule") or {}).get("quartz_cron_expression"),
                "paused": (st.get("schedule") or {}).get("pause_status") == "PAUSED",
            }
        )
        if len(items) > limit:
            break
    return _list(items, "jobs", "job_id", limit=limit)


@databricks_router.get("/jobs/{job_id}")
def get_job(c: Dbx, job_id: str) -> dict[str, Any]:
    """Full job definition (settings, tasks, schedule) exactly as the migration will read it."""
    return c.get_job(job_id)


@databricks_router.get("/clusters", response_model=BrowseList)
def browse_clusters(c: Dbx, include_job_clusters: bool = False) -> BrowseList:
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
        if include_job_clusters or cl.get("cluster_source") not in ("JOB", "PIPELINE")
    ]
    return _list(items, "clusters", "cluster_id")


@databricks_router.get("/pipelines", response_model=BrowseList)
def browse_pipelines(c: Dbx, name: str | None = None) -> BrowseList:
    """DLT / Lakeflow declarative pipelines (migrated as manual redesign items with AI work orders)."""
    items = [
        {
            "pipeline_id": p.get("pipeline_id"),
            "name": p.get("name"),
            "state": p.get("state"),
            "creator": p.get("creator_user_name"),
        }
        for p in c.iter_pipelines()
        if _contains(p.get("name"), name)
    ]
    return _list(items, "pipelines", "pipeline_id")


@databricks_router.get("/warehouses", response_model=BrowseList)
def browse_warehouses(c: Dbx) -> BrowseList:
    """SQL warehouses: migrate them as items, and/or pass one as source.warehouse_id for copy_data."""
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


@databricks_router.get("/catalogs", response_model=BrowseList)
def browse_catalogs(c: Dbx, include_system: bool = False) -> BrowseList:
    items = [
        {
            "name": x.get("name"),
            "catalog_type": x.get("catalog_type"),
            "comment": x.get("comment"),
            "owner": x.get("owner"),
        }
        for x in c.list_catalogs()
        if include_system or x.get("name") not in SYSTEM_CATALOGS
    ]
    return _list(items)


@databricks_router.get("/catalogs/{catalog}/schemas", response_model=BrowseList)
def browse_schemas(c: Dbx, catalog: str) -> BrowseList:
    """Schemas of a catalog; put ``catalog.schema`` into selection.tables to migrate all of its tables."""
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


@databricks_router.get("/catalogs/{catalog}/schemas/{schema}/tables", response_model=BrowseList)
def browse_tables(c: Dbx, catalog: str, schema: str) -> BrowseList:
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


@databricks_router.get("/secret-scopes", response_model=BrowseList)
def browse_secret_scopes(c: Dbx) -> BrowseList:
    """Scope and key NAMES (Databricks never returns secret values; recreate them in Key Vault)."""
    items = [
        {
            "name": sc.get("name"),
            "backend_type": sc.get("backend_type"),
            "keys": [k.get("key") for k in c.list_secret_keys(sc["name"])],
        }
        for sc in c.list_secret_scopes()
    ]
    return _list(items, "secret_scopes", "name")


@databricks_router.get("/inventory")
def browse_inventory(
    c: Dbx,
    settings: Cfg,
    path: Annotated[list[str], Query()] = ["/"],  # noqa: B006
    catalog: Annotated[list[str], Query()] = [],  # noqa: B006
    schema: Annotated[list[str], Query()] = [],  # noqa: B006
    include_jobs: bool = True,
    include_tables: bool = True,
    include_secret_scopes: bool = True,
    include_clusters: bool = True,
    include_pipelines: bool = True,
    include_warehouses: bool = True,
) -> dict[str, Any]:
    """Everything at once, in the engine's assessment format. Send the response (or a filtered
    copy of its ``assets``) as ``assessment`` in ``POST /migrations`` to skip discovery entirely."""
    return discover(
        c,
        DiscoveryOptions(
            workspace_paths=path,
            catalogs=catalog,
            schemas=schema,
            include_jobs=include_jobs,
            include_tables=include_tables,
            include_secret_scopes=include_secret_scopes,
            include_clusters=include_clusters,
            include_pipelines=include_pipelines,
            include_warehouses=include_warehouses,
            max_notebooks=settings.DATABRICKS_MAX_NOTEBOOKS,
        ),
    )


# ── Fabric (choose the target) ──────────────────────────────
@fabric_router.get("/workspaces", response_model=BrowseList)
def browse_fabric_workspaces(fab: Fab, name: str | None = None) -> BrowseList:
    items = [
        {
            "id": w.get("id"),
            "name": w.get("displayName"),
            "type": w.get("type"),
            "capacity_id": w.get("capacityId"),
        }
        for w in fab.list_workspaces()
        if _contains(w.get("displayName"), name)
    ]
    return _list(items)


def _ws(fab: Fabric, workspace: str) -> str:
    return workspace if _GUID.match(workspace) else fab.find_workspace(workspace)


@fabric_router.get("/workspaces/{workspace}/lakehouses", response_model=BrowseList)
def browse_fabric_lakehouses(fab: Fab, workspace: str) -> BrowseList:
    """Lakehouses of a workspace (id or display name); use one as ``target.lakehouse``."""
    items = []
    for lh in fab.list_lakehouses(_ws(fab, workspace)):
        props = lh.get("properties") or {}
        items.append(
            {
                "id": lh.get("id"),
                "name": lh.get("displayName"),
                "schemas_enabled": bool(props.get("defaultSchema")),
                "tables_path": props.get("oneLakeTablesPath"),
                "files_path": props.get("oneLakeFilesPath"),
                "sql_endpoint": (props.get("sqlEndpointProperties") or {}).get("connectionString"),
            }
        )
    return _list(items)


@fabric_router.get("/workspaces/{workspace}/items", response_model=BrowseList)
def browse_fabric_items(fab: Fab, workspace: str, type: str | None = None) -> BrowseList:
    """Items already in the workspace (e.g. type=Notebook, DataPipeline, Warehouse)."""
    items = [
        {"id": i.get("id"), "name": i.get("displayName"), "type": i.get("type")}
        for i in fab.list_items(_ws(fab, workspace), type)
    ]
    return _list(items)
