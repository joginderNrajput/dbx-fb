from __future__ import annotations

import base64
import json
import shutil
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from app.conversion.common import NOT, TEST
from app.core.config import Settings
from app.core.exceptions import FabricError, ValidationFailed
from app.fabric.auth import FabricCredentials, acquire_token
from app.fabric.client import Fabric
from app.fabric.deploy import deploy
from app.migration.engine import convert_all
from app.migration.ledger import do_plan, load_ledger
from app.migration.mapping import init_mapping
from app.sources.lookup import SourceResolver
from tests.conftest import FIXTURES, make_settings
from tests.unit.fabric.fake import FakeFabric


def converted(
    assessment: dict[str, Any], tmp_path: Path, **over: Any
) -> tuple[Path, dict[str, Any]]:
    cfg = init_mapping(assessment)
    cfg.update(over)
    out = tmp_path / "out"
    do_plan(assessment, cfg, out)
    src = tmp_path / "src"
    shutil.copytree(FIXTURES / "notebooks", src)
    convert_all(out, assessment, cfg, source_lookup=SourceResolver(src, None))
    return out, cfg


def test_service_principal_token(settings: Settings) -> None:
    with respx.mock() as router:
        route = router.post("https://login.microsoftonline.com/t1/oauth2/v2.0/token").mock(
            return_value=httpx.Response(200, json={"access_token": "abc"})
        )
        with httpx.Client() as http:
            assert (
                acquire_token(
                    FabricCredentials(tenant_id="t1", client_id="c", client_secret="s"),
                    settings,
                    http,
                )
                == "abc"
            )
        assert (
            b"scope=https%3A%2F%2Fapi.fabric.microsoft.com%2F.default"
            in route.calls[0].request.content
        )


def test_az_cli_refused_unless_enabled(settings: Settings) -> None:
    with httpx.Client() as http, pytest.raises(ValidationFailed):
        acquire_token(FabricCredentials(use_az_cli=True), settings, http)


def test_deploy_creates_items_and_is_idempotent(assessment: dict[str, Any], tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    out, cfg = converted(
        assessment,
        tmp_path,
        adls_connection_id="conn-1",
        staging_storage="abfss://s@me.dfs.core.windows.net/x",
    )
    with respx.mock(assert_all_called=False) as router:
        fake = FakeFabric(router)
        fake.items.append(
            {"id": "nb-notify", "type": "Notebook", "displayName": "notify"}
        )  # already in the workspace
        fab = Fabric("tok", settings)
        deploy(out, cfg, fab, "ws-1")
        assert (
            fake.item("Lakehouse", "lh_main")
            and fake.item("Warehouse", "wh_main")
            and fake.item("SQLDatabase", "sqldb_main")
        )
        nb = fake.item("Notebook", "load_sales")
        assert nb
        payload = json.loads(base64.b64decode(fake.definitions[nb["id"]]["parts"][0]["payload"]))
        assert (
            payload["metadata"]["dependencies"]["lakehouse"]["default_lakehouse_name"] == "lh_main"
        )
        pl = fake.item("DataPipeline", "pl_nightly_sales")
        assert pl
        pipe = base64.b64decode(fake.definitions[pl["id"]]["parts"][0]["payload"]).decode()
        assert nb["id"] in pipe and "nb-notify" in pipe and "@@" not in pipe
        assert fake.schedules and fake.shortcuts
        created = len(fake.items)
        deploy(out, cfg, Fabric("tok", settings), "ws-1")  # re-run: updates, never duplicates
        assert len(fake.items) == created and any(c.startswith("update") for c in fake.calls)
    led = load_ledger(out)
    items = {i["id"]: i for i in led["items"]}
    assert (
        items["Notebook:load_sales"]["deployment"] == "deployed"
        and items["Notebook:load_sales"]["target_id"] == nb["id"]
    )
    assert items["Table/View:main.bronze.sales_raw"]["outcome"] == TEST


def test_deploy_blocked_by_preflight_errors(assessment: dict[str, Any], tmp_path: Path) -> None:
    out, cfg = converted(assessment, tmp_path, lakehouse_default="9bad")
    with pytest.raises(ValidationFailed):
        deploy(out, cfg, Fabric("tok", make_settings(tmp_path)), "ws-1")


def test_throttling_retries_then_fails(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, FABRIC_MAX_THROTTLE_RETRIES=2)
    with respx.mock() as router:
        router.get("https://api.fabric.microsoft.com/v1/workspaces").mock(
            return_value=httpx.Response(429, headers={"Retry-After": "0"})
        )
        with pytest.raises(FabricError, match="throttled"):
            Fabric("tok", settings).list_workspaces()


def test_failed_pipeline_marks_item_not_migrated(
    assessment: dict[str, Any], tmp_path: Path
) -> None:
    settings = make_settings(tmp_path)
    out, cfg = converted(assessment, tmp_path)
    with respx.mock(assert_all_called=False) as router:
        FakeFabric(router)
        router.post("https://api.fabric.microsoft.com/v1/workspaces/ws-1/dataPipelines").mock(
            return_value=httpx.Response(400, text="bad definition")
        )
        deploy(out, cfg, Fabric("tok", settings), "ws-1", only=["notebook", "pipeline"])
    it = next(i for i in load_ledger(out)["items"] if i["id"] == "Job:nightly_sales")
    assert it["outcome"] == NOT and it["deployment"] == "failed"


def test_paging_stops_on_repeated_token(tmp_path: Path) -> None:
    with respx.mock() as router:
        router.get(url__startswith="https://api.fabric.microsoft.com/v1/workspaces").mock(
            return_value=httpx.Response(
                200, json={"value": [{"id": "w"}], "continuationToken": "same"}
            )
        )
        assert len(Fabric("tok", make_settings(tmp_path)).list_workspaces()) == 2
