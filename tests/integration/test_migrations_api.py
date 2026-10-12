"""End-to-end flows through the HTTP API; Databricks, Fabric and Claude are faked (no network)."""

from __future__ import annotations

import base64
import io
import json
import zipfile
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx
from fastapi.testclient import TestClient

from app.core.exceptions import AIUnavailable
from app.main import create_app
from tests.conftest import FIXTURES, make_settings, wait_for_job
from tests.fakes import FakeLLM, call, fix_first_todo_cell
from tests.unit.databricks.test_discovery import HOST, mock_workspace
from tests.unit.fabric.fake import FakeFabric

pytestmark = pytest.mark.integration
SP = {"tenant_id": "t1", "client_id": "c1", "client_secret": "s1"}


def upload_files() -> list[tuple[str, tuple[str, bytes]]]:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.write(FIXTURES / "notebooks/etl/publish.py", "etl/publish.py")
        zf.write(FIXTURES / "notebooks/etl/load_sales.py", "etl/load_sales.py")
    nb = FIXTURES / "notebooks"
    return [
        ("files", ("etl.zip", buf.getvalue())),
        ("files", ("explore.ipynb", (nb / "explore.ipynb").read_bytes())),
        ("files", ("report.sql", (nb / "report.sql").read_bytes())),
    ]


def config(**over: Any) -> dict[str, str]:
    base: dict[str, Any] = {
        "mapping": {
            "catalog_to_lakehouse": {"main": "lh_sales"},
            "mount_map": {"/mnt/raw": "Files/raw"},
        }
    }
    base.update(over)
    return {"config": json.dumps(base)}


def test_offline_upload_pipeline_end_to_end(client: TestClient) -> None:
    r = client.post("/api/v1/migrations/upload", files=upload_files(), data=config())
    assert r.status_code == 202, r.text
    job = wait_for_job(client, r.json()["id"])
    assert job["status"] == "succeeded", job
    assert job["source_kind"] == "upload" and job["upload"]["notebooks"] == 4
    assert job["inventory"]["by_type"] == {"Notebook": 4}
    assert job["summary"]["total"] == 4 and job["preflight"]["errors"] == 0
    assert job["options"]["fabric_credentials"] is False
    jid = job["id"]

    items = client.get(f"/api/v1/migrations/{jid}/items").json()["items"]
    assert {i["name"] for i in items} == {"load_sales", "publish", "explore", "report"}
    assert "Post-Migration Report" in client.get(f"/api/v1/migrations/{jid}/report").text
    assert client.get(f"/api/v1/migrations/{jid}/report.csv").text.startswith("type,name,outcome")
    assert "# Runbook" in client.get(f"/api/v1/migrations/{jid}/runbook").text
    files = client.get(f"/api/v1/migrations/{jid}/artifacts").json()["files"]
    assert "artifacts/notebooks/load_sales.ipynb" in files and "ledger.json" in files
    nb = client.get(
        f"/api/v1/migrations/{jid}/artifacts/artifacts/notebooks/load_sales.ipynb"
    ).json()
    assert "lh_sales.bronze.sales_raw" in json.dumps(nb)
    assert client.get(f"/api/v1/migrations/{jid}/artifacts/../job.json").status_code == 404
    z = zipfile.ZipFile(io.BytesIO(client.get(f"/api/v1/migrations/{jid}/download").content))
    assert "post_migration_report.html" in z.namelist()
    pre = client.get(f"/api/v1/migrations/{jid}/preflight").json()
    assert pre["errors"] == 0 and pre["warnings"] >= 1

    r = client.post(
        f"/api/v1/migrations/{jid}/results",
        json={
            "item_id": "Notebook:load_sales",
            "outcome": "needs_testing",
            "note": "fixed by hand",
        },
    )
    assert r.status_code == 200 and r.json()["outcome"] == "needs_testing"
    r = client.post(
        f"/api/v1/migrations/{jid}/results", json={"item_id": "Notebook:nope", "outcome": "partial"}
    )
    assert r.status_code == 404
    r = client.post(
        f"/api/v1/migrations/{jid}/validation",
        files={
            "file": ("results.json", json.dumps([{"id": "Notebook:publish", "status": "PASS"}]))
        },
    )
    assert r.json()["imported"]["PASS"] == 1
    assert client.post(f"/api/v1/migrations/{jid}/cancel").status_code == 409
    assert client.get("/api/v1/migrations").json()[0]["id"] == jid
    assert client.delete(f"/api/v1/migrations/{jid}").status_code == 204
    assert client.get(f"/api/v1/migrations/{jid}").status_code == 404


def test_upload_with_deploy_to_fabric(client: TestClient) -> None:
    with respx.mock(assert_all_called=False) as router:
        fake = FakeFabric(router)
        fake.items.append({"id": "nb-x", "type": "Notebook", "displayName": "notify"})
        r = client.post(
            "/api/v1/migrations/upload",
            files=upload_files(),
            data=config(
                target={"workspace_id": "ws-1", "credentials": SP}, options={"deploy": True}
            ),
        )
        assert r.status_code == 202, r.text
        job = wait_for_job(client, r.json()["id"])
    assert job["status"] == "succeeded", job
    assert job["summary"]["deployed"] == 4
    assert fake.item("Notebook", "load_sales") and fake.item("Lakehouse", "lh_sales")
    stored = (
        Path(client.app.state.settings.OUTPUT_DIR) / "jobs" / job["id"] / "job.json"
    ).read_text()  # type: ignore[attr-defined]
    assert "s1" not in stored and "client_secret" not in stored  # credentials are never persisted


def test_live_databricks_migration_and_redeploy(client: TestClient) -> None:
    with respx.mock(assert_all_called=False) as router:
        mock_workspace(router)
        router.get(f"{HOST}/api/2.0/workspace/export").mock(
            side_effect=lambda req: httpx.Response(
                200,
                json={
                    "content": base64.b64encode(
                        (FIXTURES / "notebooks/etl/load_sales.py").read_bytes()
                    ).decode()
                },
            )
        )
        body = {
            "source": {
                "type": "databricks",
                "credentials": {"host": HOST, "token": "dapi-123"},
                "include_secret_scopes": False,
            },
            "mapping": {"mount_map": {"/mnt/raw": "Files/raw"}},
        }
        r = client.post("/api/v1/migrations", json=body)
        assert r.status_code == 202, r.text
        job = wait_for_job(client, r.json()["id"])
        assert job["status"] == "succeeded", job
        assert (
            job["inventory"]["by_type"]["Notebook"] == 2 and job["inventory"]["by_type"]["Job"] == 1
        )
        assert job["options"]["databricks_host"] == HOST

        fake = FakeFabric(router)
        r = client.post(
            f"/api/v1/migrations/{job['id']}/deploy",
            json={"target": {"workspace_id": "ws-1", "credentials": SP}, "only": ["notebook"]},
        )
        assert r.status_code == 202, r.text
        job = wait_for_job(client, job["id"])
    assert job["status"] == "succeeded", job
    assert fake.item("Notebook", "load_sales") and fake.item("Lakehouse", "lh_main")


def test_agent_mode_end_to_end(tmp_path: Path) -> None:
    llm = FakeLLM(
        [
            call("get_job_context"),
            call("inventory_source"),
            call("plan_migration"),
            call("convert_assets"),
            call("read_notebook", name="explore"),
            fix_first_todo_cell("explore", "user = notebookutils.runtime.context['userName']"),
            call("run_preflight"),
            call("finish", summary="done: explore fixed"),
        ]
    )
    settings = make_settings(tmp_path, ANTHROPIC_API_KEY="sk-ant-test")
    with TestClient(create_app(settings, llm_factory=lambda s: llm)) as c:
        r = c.post(
            "/api/v1/migrations/upload",
            files=upload_files(),
            data=config(options={"mode": "agent", "agent_instructions": "be careful"}),
        )
        job = wait_for_job(c, r.json()["id"])
        assert job["status"] == "succeeded", job
        assert (
            job["agent"]["stopped"] == "finished"
            and job["agent"]["summary"] == "done: explore fixed"
        )
        assert "safety_net" not in job["agent"]
        trace = c.get(f"/api/v1/migrations/{job['id']}/agent-trace").json()
        assert [s["name"] for s in trace["steps"] if s["kind"] == "tool"][:2] == [
            "get_job_context",
            "inventory_source",
        ]
        items = {i["id"]: i for i in c.get(f"/api/v1/migrations/{job['id']}/items").json()["items"]}
        assert items["Notebook:explore"]["outcome"] == "needs_testing"
        assert "be careful" in llm.requests[0][0]["content"]


def test_agent_failure_falls_back_to_pipeline(tmp_path: Path) -> None:
    class Broken(FakeLLM):
        def create_message(self, **kw: Any) -> Any:
            raise AIUnavailable("Anthropic API unreachable")

    settings = make_settings(tmp_path, ANTHROPIC_API_KEY="sk-ant-test")
    with TestClient(create_app(settings, llm_factory=lambda s: Broken([]))) as c:
        r = c.post(
            "/api/v1/migrations/upload",
            files=upload_files(),
            data=config(options={"mode": "agent"}),
        )
        job = wait_for_job(c, r.json()["id"])
    assert job["status"] == "succeeded" and job["agent"]["stopped"] == "error"
    assert "safety_net" in job["agent"] and job["summary"]["total"] == 4


def test_ai_fix_option_rewrites_cells(tmp_path: Path) -> None:
    llm = FakeLLM([], completion="```python\nuser = notebookutils.runtime.context['userName']\n```")
    settings = make_settings(tmp_path, ANTHROPIC_API_KEY="sk-ant-test")
    with TestClient(create_app(settings, llm_factory=lambda s: llm)) as c:
        r = c.post(
            "/api/v1/migrations/upload",
            files=[upload_files()[1]],
            data=config(options={"ai_fix": True}),
        )
        job = wait_for_job(c, r.json()["id"])
        items = c.get(f"/api/v1/migrations/{job['id']}/items").json()["items"]
    assert job["status"] == "succeeded" and items[0]["outcome"] == "needs_testing"
    assert any("AI-converted" in a for a in items[0]["actions"])


def test_request_validation(client: TestClient) -> None:
    r = client.post(
        "/api/v1/migrations/upload", files=upload_files(), data=config(options={"deploy": True})
    )
    assert r.status_code == 422 and r.json()["error"]["field"] == "target.credentials"
    r = client.post(
        "/api/v1/migrations/upload", files=upload_files(), data=config(options={"mode": "agent"})
    )
    assert r.status_code == 503 and r.json()["error"]["code"] == "ai_unavailable"
    r = client.post(
        "/api/v1/migrations/upload", files=[("files", ("notes.txt", b"hello"))], data=config()
    )
    assert r.status_code == 422 and r.json()["error"]["field"] == "files"
    r = client.post("/api/v1/migrations/upload", files=upload_files(), data={"config": "{not json"})
    assert r.status_code == 422 and r.json()["error"]["field"] == "config"
    r = client.post(
        "/api/v1/migrations/upload",
        files=upload_files(),
        data={"config": json.dumps({"optons": {}})},
    )
    assert r.status_code == 422 and r.json()["error"]["field"] == "config.optons"
    r = client.post(
        "/api/v1/migrations",
        json={
            "source": {
                "type": "databricks",
                "credentials": {"host": "https://127.0.0.1", "token": "x"},
            }
        },
    )
    assert r.status_code == 422 and r.json()["error"]["field"] == "source.credentials.host"
    r = client.post("/api/v1/migrations", json={"source": {"type": "databricks"}})
    assert r.status_code == 422
    assert list((Path(client.app.state.settings.OUTPUT_DIR) / "jobs").glob("*")) == []  # type: ignore[attr-defined]


def test_connection_checks(client: TestClient) -> None:
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{HOST}/api/2.0/preview/scim/v2/Me").mock(
            return_value=httpx.Response(200, json={"userName": "ana@example.com"})
        )
        FakeFabric(router)
        r = client.post(
            "/api/v1/connections/databricks/test",
            json={"credentials": {"host": HOST, "token": "dapi"}},
        )
        assert r.json() == {
            "ok": True,
            "host": HOST,
            "auth_type": "personal_access_token",
            "user": "ana@example.com",
        }
        r = client.post(
            "/api/v1/connections/fabric/test", json={"credentials": SP, "workspace_id": "ws-1"}
        )
        assert r.json()["workspace"]["name"] == "Migration WS"
        router.get(f"{HOST}/api/2.0/preview/scim/v2/Me").mock(
            return_value=httpx.Response(403, text="Invalid access token")
        )
        r = client.post(
            "/api/v1/connections/databricks/test",
            json={"credentials": {"host": HOST, "token": "bad"}},
        )
        assert r.status_code == 502 and r.json()["error"]["code"] == "databricks_error"


def test_full_live_migration_with_data_copy_by_workspace_name(client: TestClient) -> None:
    """Databricks discovery -> convert -> deploy to a workspace/lakehouse by NAME -> copy + validate data."""
    from tests.unit.databricks.fake_sql import FakeSQL

    cols = [("dt", "date"), ("amount", "double")]
    with respx.mock(assert_all_called=False) as router:
        mock_workspace(router)
        router.get(f"{HOST}/api/2.0/workspace/export").mock(
            side_effect=lambda req: httpx.Response(
                200,
                json={
                    "content": base64.b64encode(
                        (FIXTURES / "notebooks/etl/load_sales.py").read_bytes()
                    ).decode()
                },
            )
        )
        fake = FakeFabric(router)
        router.get("https://api.fabric.microsoft.com/v1/workspaces").mock(
            return_value=httpx.Response(
                200, json={"value": [{"id": "ws-1", "displayName": "Alteryx_Fabric"}]}
            )
        )
        FakeSQL(
            router,
            HOST,
            {
                "main.gold.sales": (
                    cols,
                    [["2024-01-01", "1.5"], ["2024-01-02", "2.5"], ["2024-01-03", None]],
                )
            },
        )
        body = {
            "source": {
                "type": "databricks",
                "credentials": {"host": HOST, "client_id": "sp", "client_secret": "sec"},
                "include_secret_scopes": False,
                "catalogs": ["main"],
                "schemas": ["gold"],
            },
            "target": {
                "workspace_name": "Alteryx_Fabric",
                "lakehouse": "Src_LK",
                "credentials": SP,
            },
            "options": {"deploy": True, "copy_data": True},
        }
        router.post(f"{HOST}/oidc/v1/token").mock(
            return_value=httpx.Response(200, json={"access_token": "dbx-oauth"})
        )
        r = client.post("/api/v1/migrations", json=body)
        assert r.status_code == 202, r.text
        job = wait_for_job(client, r.json()["id"])
    assert job["status"] == "succeeded", job
    assert job["data_copy"]["copied"] == 1 and job["data_copy"]["failed"] == 0
    assert (
        job["options"]["workspace_name"] == "Alteryx_Fabric"
        and job["options"]["lakehouse"] == "Src_LK"
    )
    assert fake.item("Lakehouse", "Src_LK") and not fake.item("Lakehouse", "lh_main")
    nb = fake.notebook(fake.item("Notebook", "load_sales")["id"])  # type: ignore[index]
    assert "Src_LK.gold.daily_sales" in json.dumps(nb)  # code points at the destination lakehouse
    items = {
        i["id"]: i for i in client.get(f"/api/v1/migrations/{job['id']}/items").json()["items"]
    }
    sales = items["Table/View:main.gold.sales"]
    assert sales["outcome"] == "migrated" and sales["validation"]["rows_target"] == 3
    assert "load_migrated_data_Src_LK" in fake.runs


def test_copy_data_validation(client: TestClient) -> None:
    r = client.post(
        "/api/v1/migrations/upload",
        files=upload_files(),
        data=config(target={"workspace_name": "W", "credentials": SP}, options={"copy_data": True}),
    )
    assert r.status_code == 422 and r.json()["error"]["field"] == "config.databricks"
    r = client.post(
        "/api/v1/migrations/upload",
        files=upload_files(),
        data=config(options={"copy_data": True}, databricks={"host": HOST, "token": "dapi"}),
    )
    assert r.status_code == 422 and r.json()["error"]["field"] == "target.credentials"
