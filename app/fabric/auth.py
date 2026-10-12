"""Fabric credentials: Entra ID service principal ONLY (ADR 0008). Spec §5.4.

Tokens are minted per audience from the same principal: the Fabric REST API (FABRIC_SCOPE) and
OneLake (ONELAKE_SCOPE, used by data copy).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import httpx

from app.core.config import Settings
from app.core.exceptions import FabricError, ValidationFailed

__all__ = ["FabricCredentials", "acquire_token"]

_TENANT = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,253}[A-Za-z0-9])?$")


@dataclass(frozen=True)
class FabricCredentials:
    tenant_id: str
    client_id: str
    client_secret: str = field(repr=False)

    def __post_init__(self) -> None:
        missing = [k for k in ("tenant_id", "client_id", "client_secret") if not getattr(self, k)]
        if missing:
            raise ValidationFailed(
                f"Fabric needs a service principal: missing {', '.join(missing)}",
                field=f"target.credentials.{missing[0]}",
            )
        # The tenant goes into the authority URL path: a GUID or a domain, nothing that could add path segments.
        if not _TENANT.match(self.tenant_id):
            raise ValidationFailed(
                "tenant_id must be a directory GUID or a domain like contoso.onmicrosoft.com",
                field="target.credentials.tenant_id",
            )


def acquire_token(
    creds: FabricCredentials, settings: Settings, http: httpx.Client, *, scope: str | None = None
) -> str:
    """Client-credentials token for the Fabric API (default) or another audience, e.g. OneLake."""
    url = f"{settings.FABRIC_AUTHORITY.rstrip('/')}/{creds.tenant_id}/oauth2/v2.0/token"
    try:
        r = http.post(
            url,
            data={
                "grant_type": "client_credentials",
                "client_id": creds.client_id,
                "client_secret": creds.client_secret,
                "scope": scope or settings.FABRIC_SCOPE,
            },
        )
    except httpx.HTTPError as e:
        raise FabricError(f"Entra ID token request failed: {e}") from e
    if r.status_code != 200:
        raise FabricError(f"Entra ID token request failed ({r.status_code}): {r.text[:300]}")
    try:
        tok = r.json().get("access_token")
    except ValueError as e:
        raise FabricError(f"Entra ID response is not JSON (check tenant_id): {r.text[:200]}") from e
    if not tok:
        raise FabricError("Entra ID response had no access_token")
    return str(tok)
