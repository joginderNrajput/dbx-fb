from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import pytest
import respx

from app.conversion.common import MIG, PART
from app.core.config import Settings
from app.databricks.auth import DatabricksCredentials
from app.fabric.auth import FabricCredentials
from app.migration.data_copy import rows_to_jsonl
from app.migration.ledger import load_ledger
from app.migration.session import MigrationSession, SessionSpec
from tests.conftest import make_settings
from tests.unit.databricks.fake_sql import FakeSQL
from tests.unit.fabric.fake import FakeFabric

HOST = "https://adb-9.9.azuredatabricks.net"
SP = FabricCredentials(tenant_id="t", client_id="c", client_secret="s")
COLS = [
    ("id", "bigint"),
    ("name", "string"),
    ("amount", "decimal(10,2)"),
    ("tags", "array<string>"),
]
ROWS = [["1", "a", "1.50", '["x"]'], ["2", None, "2.00", "[]"], ["3", "c", None, None]]


def t(name: str, tt: str, loc: str = "") -> dict[str, Any]:
    return {
        "type": "Table/View",
        "name": name,
        "location": loc or name,
        "meta": {"table_type": tt, "storage_location": loc, "format": "DELTA"},
    }


def assessment() -> dict[str, Any]:
    return {
        "assets": [
            t(
                "cat1.sales.orders",
                "MANAGED",
                "abfss://uc@dbstorageabc.dfs.core.windows.net/__unitystorage/o",
            ),
            t("cat1.sales.empty", "EXTERNAL", "abfss://lake@acct.dfs.core.windows.net/empty"),
            t(
                "cat1.sales.missing",
                "MANAGED",
                "abfss://uc@dbstorageabc.dfs.core.windows.net/__unitystorage/m",
            ),
            {
                "type": "Table/View",
                "name": "cat1.sales.v_orders",
                "location": "v",
                "meta": {
                    "table_type": "VIEW",
                    "view_definition": "SELECT * FROM cat1.sales.orders",
                },
            },
        ]
    }


def session(tmp_path: Path, settings: Settings, **spec: Any) -> MigrationSession:
    src = tmp_path / "src"
    src.mkdir(exist_ok=True)
    s = MigrationSession(
        SessionSpec(
            source_kind="databricks",
            sources_dir=src,
            out_dir=tmp_path / "out",
            assessment=assessment(),
            databricks=DatabricksCredentials(HOST, "dapi"),
            fabric=SP,
            workspace_name="Alteryx_Fabric",
            target_lakehouse="Src_LK",
            copy_data=True,
            **spec,
        ),
        settings,
    )
    s.inventory()
    s.plan()
    s.convert()
    return s


def test_rows_to_jsonl() -> None:
    assert rows_to_jsonl(["a", "b"], [["1", None]]) == b'{"a": "1", "b": null}\n'


@pytest.mark.parametrize("disposition", ["EXTERNAL_LINKS", "INLINE"])
def test_copy_exports_loads_validates_and_creates_views(tmp_path: Path, disposition: str) -> None:
    settings = make_settings(tmp_path, DATA_COPY_DISPOSITION=disposition, FABRIC_JOB_POLL_S=0.01)
    s = session(tmp_path, settings)
    with respx.mock(assert_all_called=False) as router:
        fab = FakeFabric(router)
        fab.items.append({"id": "ws-1", "type": "Workspace", "displayName": "x"})
        router.get("https://api.fabric.microsoft.com/v1/workspaces").mock(
            return_value=httpx.Response(
                200, json={"value": [{"id": "ws-1", "displayName": "Alteryx_Fabric"}]}
            )
        )
        sql = FakeSQL(
            router, HOST, {"cat1.sales.orders": (COLS, ROWS), "cat1.sales.empty": (COLS, [])}
        )
        summary = s.copy_data()
    assert summary["copied"] == 2 and summary["failed"] == 1 and summary["warehouse_id"] == "wh-run"
    assert summary["views_created"] == ["create_views_Src_LK"]
    assert bool(sql.downloads) == (disposition == "EXTERNAL_LINKS")
    lh = fab.item("Lakehouse", "Src_LK")
    assert lh is not None
    parts = sorted(k for k in fab.files if "/Files/dbx_migration/cat1/sales/orders/" in k)
    assert len(parts) == 2  # 3 rows in chunks of 2
    first = json.loads(fab.files[parts[0]].splitlines()[0])
    assert first == {"id": "1", "name": "a", "amount": "1.50", "tags": '["x"]'}
    loader = fab.notebook(fab.item("Notebook", "load_migrated_data_Src_LK")["id"])  # type: ignore[index]
    manifest_src = next(
        "".join(c["source"]) for c in loader["cells"] if "".join(c["source"]).startswith("MANIFEST")
    )
    assert '"decimal(10,2)"' in manifest_src and '"sales.orders"' in manifest_src
    assert fab.runs == ["load_migrated_data_Src_LK", "create_views_Src_LK"]
    items = {i["id"]: i for i in load_ledger(tmp_path / "out")["items"]}
    orders = items["Table/View:cat1.sales.orders"]
    assert orders["outcome"] == MIG and orders["validation"]["rows_target"] == 3
    assert not any("staging_storage" in r for r in orders["reasons"])
    assert items["Table/View:cat1.sales.empty"]["outcome"] == MIG
    assert items["Table/View:cat1.sales.missing"]["outcome"] in (PART, "not_migrated")
    assert any("Data export failed" in r for r in items["Table/View:cat1.sales.missing"]["reasons"])
    assert any(
        "create_views_Src_LK run succeeded" in a
        for a in items["Table/View:cat1.sales.v_orders"]["actions"]
    )


def test_loader_failure_and_row_limit(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, DATA_COPY_MAX_ROWS=2, FABRIC_JOB_POLL_S=0.01)
    s = session(tmp_path, settings, workspace_id="ws-1")
    with respx.mock(assert_all_called=False) as router:
        fab = FakeFabric(router)
        fab.fail_runs.add("load_migrated_data_Src_LK")
        FakeSQL(router, HOST, {"cat1.sales.orders": (COLS, ROWS), "cat1.sales.empty": (COLS, [])})
        summary = s.copy_data()
    items = {i["id"]: i for i in load_ledger(tmp_path / "out")["items"]}
    assert summary["skipped"] == 1 and any(
        "DATA_COPY_MAX_ROWS" in r for r in items["Table/View:cat1.sales.orders"]["reasons"]
    )
    assert items["Table/View:cat1.sales.empty"]["outcome"] == PART
    assert any("Data load failed" in r for r in items["Table/View:cat1.sales.empty"]["reasons"])


def test_no_schema_lakehouse_flattens_names(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, FABRIC_JOB_POLL_S=0.01)
    s = session(tmp_path, settings, workspace_id="ws-1")
    with respx.mock(assert_all_called=False) as router:
        fab = FakeFabric(router)
        fab.items.append({"id": "lk-1", "type": "Lakehouse", "displayName": "Src_LK"})
        fab.no_schema_lakehouses.add("Src_LK")
        FakeSQL(router, HOST, {"cat1.sales.orders": (COLS, ROWS), "cat1.sales.empty": (COLS, [])})
        s.copy_data()
    loader = fab.notebook(fab.item("Notebook", "load_migrated_data_Src_LK")["id"])  # type: ignore[index]
    src = "".join("".join(c["source"]) for c in loader["cells"])
    assert '"sales_orders"' in src and "SCHEMAS_ENABLED = False" in src


def test_copy_requires_databricks(tmp_path: Path, settings: Settings) -> None:
    s = session(tmp_path, settings)
    s.spec.databricks = None
    s._dbx = None
    with (
        patch.object(s, "dbx", return_value=None),
        pytest.raises(Exception, match="Databricks credentials"),
    ):
        s.copy_data()
    shutil.rmtree(tmp_path / "out")


def test_identifier_quoting_blocks_injection() -> None:
    from app.migration.data_copy import _quote

    assert _quote("c.s.t") == "`c`.`s`.`t`"
    assert _quote("c.s.t` ; DROP TABLE x; --") == "`c`.`s`.`t`` ; DROP TABLE x; --`"
