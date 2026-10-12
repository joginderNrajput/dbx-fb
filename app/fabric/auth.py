"""Fabric credentials and token acquisition (bearer token, service principal, or dev az CLI). Spec §5.4."""

from __future__ import annotations

import shutil

# subprocess runs only the fixed `az account get-access-token` argv, behind FABRIC_ALLOW_AZ_CLI.
import subprocess  # nosec B404
from dataclasses import dataclass, field

import httpx

from app.core.config import Settings
from app.core.exceptions import FabricError, ValidationFailed

__all__ = ["FabricCredentials", "acquire_token"]


@dataclass(frozen=True)
class FabricCredentials:
    access_token: str | None = field(default=None, repr=False)
    tenant_id: str | None = None
    client_id: str | None = None
    client_secret: str | None = field(default=None, repr=False)
    use_az_cli: bool = False

    def __post_init__(self) -> None:
        sp = self.tenant_id and self.client_id and self.client_secret
        if not (self.access_token or sp or self.use_az_cli):
            raise ValidationFailed(
                "Fabric credentials need access_token, or tenant_id + client_id + client_secret",
                field="target.credentials",
            )


def acquire_token(creds: FabricCredentials, settings: Settings, http: httpx.Client) -> str:
    if creds.access_token:
        return creds.access_token
    if creds.tenant_id and creds.client_id and creds.client_secret:
        url = f"{settings.FABRIC_AUTHORITY.rstrip('/')}/{creds.tenant_id}/oauth2/v2.0/token"
        try:
            r = http.post(
                url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": creds.client_id,
                    "client_secret": creds.client_secret,
                    "scope": settings.FABRIC_SCOPE,
                },
            )
        except httpx.HTTPError as e:
            raise FabricError(f"Entra ID token request failed: {e}") from e
        if r.status_code != 200:
            raise FabricError(f"Entra ID token request failed ({r.status_code}): {r.text[:300]}")
        tok = r.json().get("access_token")
        if not tok:
            raise FabricError("Entra ID response had no access_token")
        return str(tok)
    if creds.use_az_cli:
        if not settings.FABRIC_ALLOW_AZ_CLI:
            raise ValidationFailed(
                "az CLI tokens are disabled (FABRIC_ALLOW_AZ_CLI)",
                field="target.credentials.use_az_cli",
            )
        az = shutil.which("az")
        if not az:
            raise FabricError("az CLI not found on PATH")
        try:
            return subprocess.check_output(  # nosec B603
                [
                    az,
                    "account",
                    "get-access-token",
                    "--resource",
                    "https://api.fabric.microsoft.com",
                    "--query",
                    "accessToken",
                    "-o",
                    "tsv",
                ],
                text=True,
                stderr=subprocess.DEVNULL,
                timeout=60,
            ).strip()
        except (subprocess.SubprocessError, OSError) as e:
            raise FabricError("No token from az CLI: run `az login`") from e
    raise ValidationFailed("no usable Fabric credentials", field="target.credentials")
