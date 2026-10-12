"""Credential payloads. Secrets are SecretStr: never echoed, logged or persisted. Spec §8.2."""

from __future__ import annotations

from pydantic import Field, SecretStr

from app.schemas.common import StrictModel

__all__ = ["DatabricksCredentialsIn", "DatabricksTestIn", "FabricCredentialsIn", "FabricTestIn"]


class DatabricksCredentialsIn(StrictModel):
    host: str = Field("", description="Workspace URL, e.g. https://adb-123.4.azuredatabricks.net")
    token: SecretStr | None = Field(
        None, description="Personal access token (or env://NAME in development)"
    )
    client_id: str | None = Field(None, description="Service principal application id (OAuth M2M)")
    client_secret: SecretStr | None = Field(None, description="Service principal OAuth secret")


class FabricCredentialsIn(StrictModel):
    access_token: SecretStr | None = Field(
        None, description="Bearer token for https://api.fabric.microsoft.com"
    )
    tenant_id: str | None = Field(None, description="Entra ID tenant (service principal flow)")
    client_id: str | None = None
    client_secret: SecretStr | None = None
    use_az_cli: bool = Field(False, description="Development only: use the token of `az login`")


class DatabricksTestIn(StrictModel):
    credentials: DatabricksCredentialsIn = DatabricksCredentialsIn()


class FabricTestIn(StrictModel):
    credentials: FabricCredentialsIn = FabricCredentialsIn()
    workspace_id: str | None = None
