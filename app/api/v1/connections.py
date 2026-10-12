"""Credential checks for Databricks and Fabric before starting a migration. Spec §5.1, §5.4."""

from __future__ import annotations

from typing import Annotated, Any

import httpx
from fastapi import APIRouter, Depends
from starlette.concurrency import run_in_threadpool

from app.api.deps import get_settings_dep
from app.core.config import Settings
from app.core.exceptions import ValidationFailed
from app.databricks.client import DatabricksClient
from app.fabric.auth import acquire_token
from app.fabric.client import Fabric
from app.orchestrator.service import build_databricks_credentials, build_fabric_credentials
from app.schemas.credentials import DatabricksTestIn, FabricTestIn

router = APIRouter(prefix="/connections", tags=["connections"])


@router.post("/databricks/test")
async def test_databricks(
    body: DatabricksTestIn, settings: Annotated[Settings, Depends(get_settings_dep)]
) -> dict[str, Any]:
    creds = build_databricks_credentials(body.credentials, settings, field="credentials")
    if creds is None:
        raise ValidationFailed("Databricks host and token are required", field="credentials")

    def check() -> dict[str, Any]:
        with DatabricksClient(creds, settings) as c:
            me = c.current_user()
        return {"ok": True, "host": creds.host, "user": me.get("userName") or me.get("displayName")}

    return await run_in_threadpool(check)


@router.post("/fabric/test")
async def test_fabric(
    body: FabricTestIn, settings: Annotated[Settings, Depends(get_settings_dep)]
) -> dict[str, Any]:
    creds = build_fabric_credentials(body.credentials, settings, field="credentials")
    if creds is None:
        raise ValidationFailed("Fabric credentials are required", field="credentials")

    def check() -> dict[str, Any]:
        with httpx.Client(timeout=settings.FABRIC_TIMEOUT_S) as http:
            token = acquire_token(creds, settings, http)
        fab = Fabric(token, settings)
        try:
            if body.workspace_id:
                ws = fab.get_workspace(body.workspace_id)
                return {
                    "ok": True,
                    "workspace": {
                        "id": ws.get("id"),
                        "name": ws.get("displayName"),
                        "capacity_id": ws.get("capacityId"),
                    },
                }
            return {"ok": True, "workspaces_visible": len(fab.list_workspaces())}
        finally:
            fab.close()

    return await run_in_threadpool(check)
