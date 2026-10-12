"""Shared fixtures. Unit tests need no network and no containers; external HTTP is mocked with respx."""

from __future__ import annotations

import importlib.util
import json
import shutil
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.core.config import Settings

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures"


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--snapshot-update",
        action="store_true",
        help="rewrite golden files under tests/fixtures/expected_*",
    )


@pytest.fixture
def snapshot_update(request: pytest.FixtureRequest) -> bool:
    return bool(request.config.getoption("--snapshot-update"))


@pytest.fixture(scope="session")
def reference() -> ModuleType:
    """The original single-file tool, imported from the repo root (the behavioural reference)."""
    spec = importlib.util.spec_from_file_location(
        "dbx2fabric_reference", ROOT / "dbx2fabric_migrate_v4.py"
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["dbx2fabric_reference"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def assessment() -> dict[str, Any]:
    data: dict[str, Any] = json.loads((FIXTURES / "assessment.json").read_text())
    return data


@pytest.fixture
def notebooks_dir(tmp_path: Path) -> Path:
    d = tmp_path / "sources"
    shutil.copytree(FIXTURES / "notebooks", d)
    return d


def make_settings(tmp_path: Path, **overrides: Any) -> Settings:
    base: dict[str, Any] = {
        "OUTPUT_DIR": tmp_path / "output",
        "AUTH_DISABLED": True,
        "APP_ENV": "development",
        "LOG_LEVEL": "WARNING",
        "ALLOW_PRIVATE_HOSTS": False,
        "ANTHROPIC_API_KEY": None,
        "FABRIC_LRO_POLL_S": 0.01,
        "FABRIC_JOB_POLL_S": 0.01,
        "DATA_COPY_POLL_S": 0.01,
        "DATABRICKS_MAX_RETRIES": 0,
    }
    base.update(overrides)
    return Settings(_env_file=None, **base)  # type: ignore[call-arg]


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return make_settings(tmp_path)


@pytest.fixture
def client(settings: Settings) -> Iterator[TestClient]:
    from app.main import create_app

    with TestClient(create_app(settings)) as c:
        yield c


def wait_for_job(client: TestClient, job_id: str, timeout: float = 30.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body: dict[str, Any] = client.get(f"/api/v1/migrations/{job_id}").json()
        if body["status"] in ("succeeded", "failed", "cancelled"):
            return body
        time.sleep(0.05)
    raise AssertionError(f"job {job_id} did not finish in {timeout}s")
