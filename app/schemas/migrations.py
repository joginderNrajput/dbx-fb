"""Migration request/response models. Spec §5.2-§5.5."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from app.schemas.common import StrictModel
from app.schemas.credentials import DatabricksCredentialsIn, FabricCredentialsIn

__all__ = [
    "DatabricksSourceIn",
    "DeployIn",
    "FabricTargetIn",
    "ItemResultIn",
    "JobError",
    "JobStatus",
    "MigrationAccepted",
    "MigrationCreate",
    "MigrationOptionsIn",
    "MigrationOut",
    "UploadConfig",
]

JobStatus = Literal["queued", "running", "succeeded", "failed", "cancelled"]
DeployKind = Literal["notebook", "shortcuts", "pipeline"]


class DatabricksSourceIn(StrictModel):
    type: Literal["databricks"] = "databricks"
    credentials: DatabricksCredentialsIn = DatabricksCredentialsIn()
    workspace_paths: list[str] = Field(
        default_factory=lambda: ["/"], description="Folders to inventory recursively"
    )
    include_jobs: bool = True
    include_tables: bool = True
    catalogs: list[str] = Field(
        default_factory=list, description="Unity Catalog catalogs (empty = all non-system)"
    )
    schemas: list[str] = Field(default_factory=list, description="Only these schemas (empty = all)")
    warehouse_id: str | None = Field(
        None, description="SQL warehouse for options.copy_data (default: first available)"
    )
    include_secret_scopes: bool = True
    include_clusters: bool = True
    max_notebooks: int | None = Field(None, ge=1)


class FabricTargetIn(StrictModel):
    workspace_id: str | None = Field(None, description="Fabric workspace GUID")
    workspace_name: str | None = Field(
        None, description="Fabric workspace display name (alternative to workspace_id)"
    )
    lakehouse: str | None = Field(
        None,
        description="Destination lakehouse for all migrated data (existing or created); default: one per catalog",
    )
    credentials: FabricCredentialsIn | None = None


class MigrationOptionsIn(StrictModel):
    mode: Literal["pipeline", "agent"] = Field(
        "pipeline", description="pipeline = deterministic stages; agent = Claude drives the stages"
    )
    deploy: bool = Field(
        False,
        description="Deploy to Fabric after conversion (needs target credentials + workspace_id)",
    )
    deploy_only: list[DeployKind] = Field(default_factory=list)
    force_deploy: bool = Field(False, description="Deploy even if preflight reports errors")
    ai_fix: bool = Field(
        False, description="Let the LLM rewrite cells that still carry TODO markers"
    )
    copy_data: bool = Field(
        False,
        description="Copy table data Databricks -> Fabric (SQL warehouse export -> OneLake -> loader notebook run); "
        "needs Databricks and Fabric credentials",
    )
    agent_instructions: str | None = Field(None, max_length=4000)


class MigrationCreate(StrictModel):
    """JSON request: migrate a live Databricks workspace."""

    source: DatabricksSourceIn
    target: FabricTargetIn = FabricTargetIn()
    mapping: dict[str, Any] = Field(
        default_factory=dict, description="Overrides of mapping.json keys"
    )
    options: MigrationOptionsIn = MigrationOptionsIn()
    assessment: dict[str, Any] | None = Field(
        None, description="Optional precomputed inventory (skips discovery)"
    )


class UploadConfig(StrictModel):
    """The `config` form field of POST /migrations/upload (JSON string)."""

    target: FabricTargetIn = FabricTargetIn()
    mapping: dict[str, Any] = Field(default_factory=dict)
    options: MigrationOptionsIn = MigrationOptionsIn()
    databricks: DatabricksCredentialsIn | None = Field(
        None, description="Optional: export notebooks that were not uploaded"
    )


class DeployIn(StrictModel):
    target: FabricTargetIn
    only: list[DeployKind] = Field(default_factory=list)
    force: bool = False
    copy_data: bool = Field(
        False, description="Also copy table data (needs databricks credentials)"
    )
    databricks: DatabricksCredentialsIn | None = None
    warehouse_id: str | None = None


class ItemResultIn(StrictModel):
    item_id: str
    outcome: Literal["migrated", "needs_testing", "partial", "not_migrated"]
    note: str = Field("", max_length=2000)
    target_id: str | None = None


class JobError(BaseModel):
    code: str
    message: str


class MigrationOut(BaseModel):
    id: str
    status: JobStatus
    mode: str
    source_kind: str
    stage: str | None = None
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    options: dict[str, Any] = Field(default_factory=dict)
    inventory: dict[str, Any] | None = None
    summary: dict[str, Any] | None = None
    preflight: dict[str, int] | None = None
    upload: dict[str, Any] | None = None
    agent: dict[str, Any] | None = None
    data_copy: dict[str, Any] | None = None
    error: JobError | None = None
    links: dict[str, str] = Field(default_factory=dict)


class MigrationAccepted(BaseModel):
    id: str
    status: JobStatus
    links: dict[str, str]
