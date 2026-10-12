"""Live tests against real Databricks / Fabric / LLM. Opt in: E2E_LIVE=1 (env or .env) and `make test-e2e`.

Credentials come from the server settings (`.env`, gitignored) through ALLOW_ENV_CREDENTIALS, so
requests carry none. Scope the run with E2E_DATABRICKS_CATALOG / _SCHEMA / _PATH.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
from dotenv import dotenv_values
from fastapi.testclient import TestClient

from app.core.config import Settings
from app.main import create_app
from tests.conftest import ROOT, wait_for_job

pytestmark = pytest.mark.slow
CONF: dict[str, Any] = {**dotenv_values(ROOT / ".env"), **os.environ}


@pytest.fixture
def live(tmp_path: Path) -> TestClient:
    if CONF.get("E2E_LIVE") != "1":
        pytest.skip("set E2E_LIVE=1 (and real credentials in .env) to run live tests")
    settings = Settings(
        _env_file=ROOT / ".env",
        OUTPUT_DIR=tmp_path / "output",
        ALLOW_ENV_CREDENTIALS=True,
        AUTH_DISABLED=True,
    )  # type: ignore[call-arg]
    with TestClient(create_app(settings)) as c:
        yield c  # type: ignore[misc]


def test_connections(live: TestClient) -> None:
    r = live.post("/api/v1/connections/databricks/test", json={})
    assert r.status_code == 200, r.text
    r = live.post(
        "/api/v1/connections/fabric/test",
        json={"workspace_name": CONF.get("FABRIC_WORKSPACE_NAME")},
    )
    assert r.status_code == 200, r.text
    r = live.post("/api/v1/connections/llm/test")
    assert r.status_code == 200 and r.json()["ok"], r.text


def _scope() -> dict[str, Any]:
    src: dict[str, Any] = {
        "type": "databricks",
        "workspace_paths": [CONF.get("E2E_DATABRICKS_PATH") or "/Shared"],
    }
    if CONF.get("E2E_DATABRICKS_CATALOG"):
        src["catalogs"] = [CONF["E2E_DATABRICKS_CATALOG"]]
    if CONF.get("E2E_DATABRICKS_SCHEMA"):
        src["schemas"] = [CONF["E2E_DATABRICKS_SCHEMA"]]
    return src


def test_full_migration_with_deploy_and_data(live: TestClient) -> None:
    body = {"source": _scope(), "options": {"deploy": True, "copy_data": True}}
    r = live.post("/api/v1/migrations", json=body)
    assert r.status_code == 202, r.text
    job = wait_for_job(live, r.json()["id"], timeout=3600)
    print("JOB", job)
    assert job["status"] == "succeeded", job
    assert job["summary"]["total"] > 0
    assert not job["data_copy"] or job["data_copy"]["failed"] == 0, job["data_copy"]


def test_agent_mode_live(live: TestClient) -> None:
    if CONF.get("E2E_AGENT") != "1":
        pytest.skip("set E2E_AGENT=1 to spend LLM tokens on a live agent run")
    r = live.post("/api/v1/migrations", json={"source": _scope(), "options": {"mode": "agent"}})
    job = wait_for_job(live, r.json()["id"], timeout=3600)
    assert job["status"] == "succeeded" and job["agent"]["stopped"] in ("finished", "end_turn"), job
