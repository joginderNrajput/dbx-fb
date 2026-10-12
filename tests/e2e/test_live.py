"""Live tests against real Databricks / Fabric / Anthropic. Opt in: `pytest -m slow` with the env vars set."""

from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from tests.conftest import make_settings, wait_for_job

pytestmark = pytest.mark.slow


def need(*names: str) -> None:
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        pytest.skip(f"set {', '.join(missing)} to run live tests")


def test_live_databricks_inventory_and_convert(tmp_path) -> None:  # type: ignore[no-untyped-def]
    need("E2E_DATABRICKS_HOST", "E2E_DATABRICKS_TOKEN")
    with TestClient(create_app(make_settings(tmp_path))) as c:
        body = {
            "source": {
                "type": "databricks",
                "credentials": {
                    "host": os.environ["E2E_DATABRICKS_HOST"],
                    "token": os.environ["E2E_DATABRICKS_TOKEN"],
                },
                "workspace_paths": [os.environ.get("E2E_DATABRICKS_PATH", "/Shared")],
                "include_tables": False,
            }
        }
        r = c.post("/api/v1/migrations", json=body)
        assert r.status_code == 202, r.text
        job = wait_for_job(c, r.json()["id"], timeout=900)
        assert job["status"] == "succeeded", job


def test_live_fabric_connection(tmp_path) -> None:  # type: ignore[no-untyped-def]
    need(
        "E2E_FABRIC_TENANT_ID",
        "E2E_FABRIC_CLIENT_ID",
        "E2E_FABRIC_CLIENT_SECRET",
        "E2E_FABRIC_WORKSPACE_ID",
    )
    with TestClient(create_app(make_settings(tmp_path))) as c:
        creds = {
            "tenant_id": os.environ["E2E_FABRIC_TENANT_ID"],
            "client_id": os.environ["E2E_FABRIC_CLIENT_ID"],
            "client_secret": os.environ["E2E_FABRIC_CLIENT_SECRET"],
        }
        r = c.post(
            "/api/v1/connections/fabric/test",
            json={"credentials": creds, "workspace_id": os.environ["E2E_FABRIC_WORKSPACE_ID"]},
        )
        assert r.status_code == 200, r.text
