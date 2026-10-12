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
    "FabricBrowseIn",
    "FabricItemsBrowseIn",
    "FabricWorkspacesBrowseIn",
    "InventoryBrowseIn",
    "JobsBrowseIn",
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


class InventoryBrowseIn(DatabricksBrowseIn):
    workspace_paths: list[str] = Field(default_factory=lambda: ["/"])
    catalogs: list[str] = Field(default_factory=list)
    schemas: list[str] = Field(default_factory=list)
    include_jobs: bool = True
    include_tables: bool = True
    include_secret_scopes: bool = True
    include_clusters: bool = True
    include_pipelines: bool = True
    include_warehouses: bool = True


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
