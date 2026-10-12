"""Browse API responses: Databricks / Fabric objects to pick from before migrating. Spec §5.10."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from app.schemas.common import StrictModel

__all__ = ["BrowseList", "SelectionIn"]


class BrowseList(BaseModel):
    """A list of objects. ``selection_key`` / ``id_field`` say how to pick items for a migration:
    put ``item[id_field]`` into ``source.selection.<selection_key>`` of ``POST /migrations``."""

    count: int
    items: list[dict[str, Any]]
    selection_key: str | None = None
    id_field: str | None = None
    truncated: bool = False
    warnings: list[str] = Field(default_factory=list)


class SelectionIn(StrictModel):
    """Objects picked from the browse API. When any list is non-empty, ONLY these are migrated and
    each is fetched directly (no workspace crawl); lists left empty mean "none of this kind"."""

    notebooks: list[str] = Field(
        default_factory=list,
        description="Workspace paths; a folder path selects every notebook under it",
    )
    jobs: list[str] = Field(
        default_factory=list, description="Job ids (GET /databricks/jobs -> job_id)"
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
