"""One Databricks credentials body serves both methods (docs/api-curl-reference.md, Postman)."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.core.exceptions import ValidationFailed
from app.orchestrator.service import build_databricks_credentials
from app.schemas.credentials import DatabricksCredentialsIn
from tests.conftest import make_settings

HOST = "https://adb-1.2.azuredatabricks.net"


def creds(tmp_path: Path, **body: str) -> str:
    c = build_databricks_credentials(
        DatabricksCredentialsIn.model_validate({"host": HOST, **body}),
        make_settings(tmp_path),
        field="credentials",
    )
    assert c is not None
    return c.auth_type


def test_empty_fields_count_as_not_sent(tmp_path: Path) -> None:
    pat = {"client_id": "", "client_secret": "", "personal_access_token": "dapi-1"}
    assert creds(tmp_path, **pat) == "personal_access_token"
    sp = {"client_id": "app", "client_secret": "s", "personal_access_token": ""}
    assert creds(tmp_path, **sp) == "service_principal"
    both = {"client_id": "app", "client_secret": "s", "personal_access_token": "dapi-1"}
    assert creds(tmp_path, **both) == "service_principal"
    with pytest.raises(ValidationFailed):
        creds(tmp_path, client_id="app", client_secret="", personal_access_token="dapi-1")
