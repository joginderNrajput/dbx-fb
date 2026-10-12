"""Credential payloads, always in the request body. Secrets are SecretStr: never echoed, logged
or persisted. Spec §8.2, ADR 0008."""

from __future__ import annotations

from pydantic import AliasChoices, Field, SecretStr

from app.schemas.common import StrictModel

__all__ = ["DatabricksCredentialsIn", "DatabricksTestIn", "FabricCredentialsIn", "FabricTestIn"]


class DatabricksCredentialsIn(StrictModel):
    """Service principal OR personal access token.

    If ``client_id`` + ``client_secret`` are present the service principal (OAuth M2M) is used and
    nothing else; otherwise ``personal_access_token`` is used. Sending only one of client_id /
    client_secret is a 422.
    """

    host: str = Field("", description="Workspace URL, e.g. https://adb-123.4.azuredatabricks.net")
    client_id: str | None = Field(
        None, description="Service principal application (client) id - OAuth M2M"
    )
    client_secret: SecretStr | None = Field(None, description="Service principal OAuth secret")
    token: SecretStr | None = Field(
        None,
        validation_alias=AliasChoices("personal_access_token", "token"),
        description="Personal access token (dapi...); field name personal_access_token (alias: token). "
        "Ignored when client_id + client_secret are sent",
    )


class FabricCredentialsIn(StrictModel):
    """Fabric supports an Entra ID service principal only."""

    tenant_id: str = Field(..., description="Entra ID tenant (directory) id or domain")
    client_id: str = Field(..., description="Service principal application (client) id")
    client_secret: SecretStr = Field(..., description="Service principal client secret")


class DatabricksTestIn(StrictModel):
    credentials: DatabricksCredentialsIn = DatabricksCredentialsIn()


class FabricTestIn(StrictModel):
    credentials: FabricCredentialsIn | None = None
    workspace_id: str | None = None
    workspace_name: str | None = None
