"""Databricks credentials (PAT or OAuth M2M service principal). Held in memory only. Spec §5.1."""

from __future__ import annotations

from dataclasses import dataclass, field

import httpx

from app.core.exceptions import DatabricksError, ValidationFailed

__all__ = ["DatabricksCredentials", "access_token"]


@dataclass(frozen=True)
class DatabricksCredentials:
    """Exactly one method: a service principal (client_id + client_secret, OAuth M2M) OR a PAT.

    When both are supplied the service principal wins and the token is dropped
    (``build_databricks_credentials``), so ``auth_type`` always says which one is used.
    """

    host: str
    token: str | None = field(default=None, repr=False)
    client_id: str | None = None
    client_secret: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not self.host:
            raise ValidationFailed("Databricks host is required", field="credentials.host")
        if bool(self.client_id) != bool(self.client_secret):
            raise ValidationFailed(
                "a Databricks service principal needs both client_id and client_secret",
                field="credentials.client_secret" if self.client_id else "credentials.client_id",
            )
        if not self.token and not self.client_id:
            raise ValidationFailed(
                "Databricks credentials need client_id + client_secret (service principal) or personal_access_token",
                field="credentials.personal_access_token",
            )

    @property
    def auth_type(self) -> str:
        return (
            "service_principal"
            if self.client_id and self.client_secret
            else "personal_access_token"
        )


def access_token(creds: DatabricksCredentials, http: httpx.Client) -> str:
    """Service principal: OAuth client-credentials on the workspace. Otherwise the PAT as-is."""
    if creds.auth_type == "personal_access_token":
        return creds.token or ""
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
    try:
        tok = r.json().get("access_token")
    except ValueError as e:
        raise DatabricksError(f"Databricks OAuth response is not JSON: {r.text[:200]}") from e
    if not tok:
        raise DatabricksError("Databricks OAuth response had no access_token")
    return str(tok)
