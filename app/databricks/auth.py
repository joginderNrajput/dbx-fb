"""Databricks credentials (PAT or OAuth M2M service principal). Held in memory only. Spec §5.1."""

from __future__ import annotations

from dataclasses import dataclass, field

import httpx

from app.core.exceptions import DatabricksError, ValidationFailed

__all__ = ["DatabricksCredentials", "access_token"]


@dataclass(frozen=True)
class DatabricksCredentials:
    host: str
    token: str | None = field(default=None, repr=False)
    client_id: str | None = None
    client_secret: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not self.host:
            raise ValidationFailed("Databricks host is required", field="credentials.host")
        if not self.token and not (self.client_id and self.client_secret):
            raise ValidationFailed(
                "Databricks credentials need a token, or client_id + client_secret",
                field="credentials.token",
            )


def access_token(creds: DatabricksCredentials, http: httpx.Client) -> str:
    """PAT as-is; for a service principal run the OAuth client-credentials flow on the workspace."""
    if creds.token:
        return creds.token
    try:
        r = http.post(
            f"{creds.host}/oidc/v1/token",
            data={"grant_type": "client_credentials", "scope": "all-apis"},
            auth=(creds.client_id or "", creds.client_secret or ""),
        )
    except httpx.HTTPError as e:
        raise DatabricksError(f"Databricks OAuth token request failed: {e}") from e
    if r.status_code != 200:
        raise DatabricksError(
            f"Databricks OAuth token request failed ({r.status_code}): {r.text[:200]}"
        )
    tok = r.json().get("access_token")
    if not tok:
        raise DatabricksError("Databricks OAuth response had no access_token")
    return str(tok)
