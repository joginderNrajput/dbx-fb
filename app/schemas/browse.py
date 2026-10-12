"""Browse API request bodies and responses. Spec §5.10.

Every browse call is a POST whose JSON body carries the credentials (ADR 0008) plus the filters;
path parameters only identify a resource (job id, catalog, schema, workspace).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from app.schemas.common import StrictModel
from app.schemas.credentials import DatabricksCredentialsIn, FabricCredentialsIn

__all__ = [
    "BrowseList",
    "CatalogsBrowseIn",
    "ClustersBrowseIn",
    "DatabricksBrowseIn",
    "DiscoveryFlags",
    "FabricBrowseIn",
    "FabricItemsBrowseIn",
    "FabricWorkspacesBrowseIn",
    "InventoryBrowseIn",
    "JobsBrowseIn",
    "NameFilterBrowseIn",
    "NotebooksBrowseIn",
    "PipelinesBrowseIn",
    "SelectionIn",
    "WorkspaceBrowseIn",
]


class BrowseList(BaseModel):
    """A list of objects. ``selection_key`` / ``id_field`` say how to pick items for a migration:
    put ``item[id_field]`` into ``source.selection.<selection_key>`` of ``POST /migrations``."""

    count: int
    items: list[dict[str, Any]]
    selection_key: str | None = None
    id_field: str | None = None
    truncated: bool = False
    warnings: list[str] = Field(default_factory=list)


# ── Databricks ──
class DatabricksBrowseIn(StrictModel):
    credentials: DatabricksCredentialsIn = DatabricksCredentialsIn()


class WorkspaceBrowseIn(DatabricksBrowseIn):
    path: str = "/"


class NotebooksBrowseIn(DatabricksBrowseIn):
    paths: list[str] = Field(
        default_factory=lambda: ["/"], description="Folders to walk recursively"
    )
    limit: int = Field(1000, ge=1, le=20000)


class JobsBrowseIn(DatabricksBrowseIn):
    name: str | None = Field(None, description="Case-insensitive substring filter")
    limit: int = Field(1000, ge=1, le=10000)


class ClustersBrowseIn(DatabricksBrowseIn):
    include_job_clusters: bool = False


class PipelinesBrowseIn(DatabricksBrowseIn):
    name: str | None = None


class CatalogsBrowseIn(DatabricksBrowseIn):
    include_system: bool = False


class DiscoveryFlags(StrictModel):
    """Which component kinds a crawl inventories (all on by default)."""

    include_jobs: bool = True
    include_tables: bool = True
    include_secret_scopes: bool = True
    include_clusters: bool = True
    include_pipelines: bool = True
    include_warehouses: bool = True
    include_volumes: bool = True
    include_functions: bool = True
    include_models: bool = True
    include_sql_assets: bool = Field(True, description="SQL queries, alerts, AI/BI dashboards")
    include_external_locations: bool = True
    include_connections: bool = True
    include_serving_endpoints: bool = True
    include_repos: bool = True
    include_libraries: bool = Field(True, description="Libraries installed on all-purpose clusters")
    include_workspace_files: bool = Field(
        True, description="Non-notebook workspace files under workspace_paths"
    )

    def flags(self) -> dict[str, bool]:
        return {k: getattr(self, k) for k in DiscoveryFlags.model_fields}


class InventoryBrowseIn(DatabricksBrowseIn, DiscoveryFlags):
    workspace_paths: list[str] = Field(default_factory=lambda: ["/"])
    catalogs: list[str] = Field(default_factory=list)
    schemas: list[str] = Field(default_factory=list)


class NameFilterBrowseIn(DatabricksBrowseIn):
    name: str | None = Field(None, description="Case-insensitive substring filter")


# ── Fabric ──
class FabricBrowseIn(StrictModel):
    credentials: FabricCredentialsIn | None = None


class FabricWorkspacesBrowseIn(FabricBrowseIn):
    name: str | None = None


class FabricItemsBrowseIn(FabricBrowseIn):
    type: str | None = Field(None, description="Notebook, DataPipeline, Lakehouse, Warehouse, ...")


class SelectionIn(StrictModel):
    """Objects picked from the browse API. When any list is non-empty, ONLY these are migrated and
    each is fetched directly (no workspace crawl); lists left empty mean "none of this kind"."""

    notebooks: list[str] = Field(
        default_factory=list,
        description="Workspace paths; a folder path selects every notebook under it",
    )
    jobs: list[str] = Field(
        default_factory=list, description="Job ids (POST /databricks/jobs -> job_id)"
    )
    clusters: list[str] = Field(default_factory=list, description="Cluster ids")
    pipelines: list[str] = Field(default_factory=list, description="DLT / Lakeflow pipeline ids")
    warehouses: list[str] = Field(default_factory=list, description="SQL warehouse ids")
    tables: list[str] = Field(
        default_factory=list,
        description="catalog.schema.table, or catalog.schema for all of its tables",
    )
    secret_scopes: list[str] = Field(
        default_factory=list,
        description="Secret scope names (key names only; values are never readable)",
    )
    volumes: list[str] = Field(
        default_factory=list, description="catalog.schema.volume, or catalog.schema for all"
    )
    functions: list[str] = Field(
        default_factory=list, description="catalog.schema.function, or catalog.schema for all"
    )
    models: list[str] = Field(
        default_factory=list, description="catalog.schema.model, or catalog.schema for all"
    )
    queries: list[str] = Field(default_factory=list, description="SQL query ids")
    alerts: list[str] = Field(default_factory=list, description="SQL alert ids")
    dashboards: list[str] = Field(
        default_factory=list, description="AI/BI (Lakeview) dashboard ids"
    )
    serving_endpoints: list[str] = Field(default_factory=list, description="Serving endpoint names")
    repos: list[str] = Field(default_factory=list, description="Git folder (repo) ids")
    external_locations: list[str] = Field(
        default_factory=list, description="External location names"
    )
    connections: list[str] = Field(
        default_factory=list, description="Lakehouse Federation connection names"
    )
    workspace_files: list[str] = Field(
        default_factory=list, description="Workspace file paths (non-notebook files)"
    )
