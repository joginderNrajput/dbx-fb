"""Browse API (GET /databricks/*, /fabric/*) and migrations driven by a selection."""

from __future__ import annotations

import base64
from pathlib import Path

import httpx
import pytest
import respx
from fastapi.testclient import TestClient

from app.main import create_app
from tests.conftest import FIXTURES, make_settings, wait_for_job
from tests.unit.databricks.test_discovery import HOST, mock_workspace
from tests.unit.fabric.fake import BASE, FakeFabric

pytestmark = pytest.mark.integration
DBX = {"X-Databricks-Host": HOST, "X-Databricks-Token": "dapi-123"}
FAB = {"X-Fabric-Tenant-Id": "t1", "X-Fabric-Client-Id": "c1", "X-Fabric-Client-Secret": "s1"}


def test_databricks_browse_endpoints(client: TestClient) -> None:
    with respx.mock(assert_all_called=False) as router:
        mock_workspace(router)
        nbs = client.get("/api/v1/databricks/notebooks", headers=DBX).json()
        assert nbs["selection_key"] == "notebooks" and nbs["id_field"] == "path"
        assert {i["path"] for i in nbs["items"]} == {"/top", "/Repos/team/load_sales"}
        scoped = client.get(
            "/api/v1/databricks/notebooks", params={"path": "/Repos/team"}, headers=DBX
        ).json()
        assert [i["path"] for i in scoped["items"]] == ["/Repos/team/load_sales"]
        limited = client.get(
            "/api/v1/databricks/notebooks", params={"limit": 1}, headers=DBX
        ).json()
        assert limited["count"] == 1 and limited["truncated"] is True
        ws = client.get("/api/v1/databricks/workspace", headers=DBX).json()
        assert ws["items"][0]["object_type"] == "DIRECTORY"

        jobs = client.get("/api/v1/databricks/jobs", headers=DBX).json()
        assert jobs["selection_key"] == "jobs" and jobs["items"][0]["job_id"] == 7
        assert (
            client.get("/api/v1/databricks/jobs", params={"name": "nomatch"}, headers=DBX).json()[
                "count"
            ]
            == 0
        )
        assert (
            client.get("/api/v1/databricks/jobs/7", headers=DBX).json()["settings"]["name"]
            == "nightly"
        )

        clusters = client.get("/api/v1/databricks/clusters", headers=DBX).json()
        assert [c["cluster_id"] for c in clusters["items"]] == [
            "c1"
        ]  # job clusters hidden by default
        assert (
            client.get(
                "/api/v1/databricks/clusters", params={"include_job_clusters": True}, headers=DBX
            ).json()["count"]
            == 2
        )
        assert (
            client.get("/api/v1/databricks/pipelines", headers=DBX).json()["items"][0][
                "pipeline_id"
            ]
            == "p1"
        )
        wh = client.get("/api/v1/databricks/warehouses", headers=DBX).json()
        assert wh["id_field"] == "id" and {w["state"] for w in wh["items"]} == {
            "RUNNING",
            "STOPPED",
        }
        cats = client.get("/api/v1/databricks/catalogs", headers=DBX).json()
        assert [c["name"] for c in cats["items"]] == ["main"]  # system catalog hidden
        schemas = client.get("/api/v1/databricks/catalogs/main/schemas", headers=DBX).json()
        assert [s["full_name"] for s in schemas["items"]] == ["main.gold"] and schemas[
            "selection_key"
        ] == "tables"
        tables = client.get(
            "/api/v1/databricks/catalogs/main/schemas/gold/tables", headers=DBX
        ).json()
        assert tables["items"][0] == {
            "full_name": "main.gold.sales",
            "name": "sales",
            "table_type": "EXTERNAL",
            "format": "DELTA",
            "storage_location": "abfss://c@a.dfs.core.windows.net/sales",
            "columns": 2,
            "owner": None,
        }
        r = client.get("/api/v1/databricks/secret-scopes", headers=DBX)
        assert r.status_code == 502  # the fake's scopes/list fails upstream
        inv = client.get(
            "/api/v1/databricks/inventory", params={"catalog": "main"}, headers=DBX
        ).json()
        assert inv["summary"]["by_type"]["Notebook"] == 2 and "assets" in inv


def test_browse_needs_credentials_and_safe_host(client: TestClient) -> None:
    r = client.get("/api/v1/databricks/jobs")
    assert r.status_code == 422 and r.json()["error"]["field"] == "X-Databricks-Host"
    r = client.get(
        "/api/v1/databricks/jobs",
        headers={"X-Databricks-Host": "https://10.0.0.5", "X-Databricks-Token": "x"},
    )
    assert r.status_code == 422 and "private" in r.json()["error"]["message"]
    r = client.get("/api/v1/fabric/workspaces")
    assert r.status_code == 422 and r.json()["error"]["field"] == "X-Fabric-Client-Id"


def test_env_credentials_fallback(tmp_path: Path) -> None:
    s = make_settings(
        tmp_path, ALLOW_ENV_CREDENTIALS=True, DATABRICKS_HOST=HOST, DATABRICKS_TOKEN="dapi-env"
    )
    with TestClient(create_app(s)) as c, respx.mock(assert_all_called=False) as router:
        mock_workspace(router)
        assert c.get("/api/v1/databricks/clusters").status_code == 200
        assert router.calls.last.request.headers["Authorization"] == "Bearer dapi-env"


def test_fabric_browse_endpoints(client: TestClient) -> None:
    with respx.mock(assert_all_called=False) as router:
        fake = FakeFabric(router)
        router.get(f"{BASE}/workspaces").mock(
            return_value=httpx.Response(
                200,
                json={
                    "value": [
                        {
                            "id": "11111111-2222-3333-4444-555555555555",
                            "displayName": "Alteryx_Fabric",
                            "type": "Workspace",
                            "capacityId": "cap",
                        }
                    ]
                },
            )
        )
        ws_id = "11111111-2222-3333-4444-555555555555"

        def lakehouses(request: httpx.Request) -> httpx.Response:
            if request.url.params.get("continuationToken") == "next":
                return httpx.Response(
                    200, json={"value": [{"id": "lk2", "displayName": "Old_LK", "properties": {}}]}
                )
            return httpx.Response(
                200,
                json={
                    "value": [
                        {
                            "id": "lk1",
                            "displayName": "Src_LK",
                            "properties": {"defaultSchema": "dbo", "oneLakeTablesPath": "t"},
                        }
                    ],
                    "continuationToken": "next",
                },
            )

        router.get(url__startswith=f"{BASE}/workspaces/{ws_id}/lakehouses").mock(
            side_effect=lakehouses
        )
        router.get(url__startswith=f"{BASE}/workspaces/{ws_id}/items").mock(
            return_value=httpx.Response(
                200, json={"value": [{"id": "n1", "displayName": "nb", "type": "Notebook"}]}
            )
        )
        wss = client.get("/api/v1/fabric/workspaces", headers=FAB).json()
        assert wss["items"] == [
            {"id": ws_id, "name": "Alteryx_Fabric", "type": "Workspace", "capacity_id": "cap"}
        ]
        lhs = client.get(
            "/api/v1/fabric/workspaces/Alteryx_Fabric/lakehouses", headers=FAB
        ).json()  # by name
        assert [(lh["name"], lh["schemas_enabled"]) for lh in lhs["items"]] == [
            ("Src_LK", True),
            ("Old_LK", False),
        ]
        items = client.get(
            f"/api/v1/fabric/workspaces/{ws_id}/items", params={"type": "Notebook"}, headers=FAB
        ).json()
        assert items["items"][0]["name"] == "nb" and "type=Notebook" in str(
            router.calls.last.request.url
        )
        assert (
            client.get("/api/v1/fabric/workspaces/Nope/lakehouses", headers=FAB).status_code == 502
        )
        assert fake  # token flow used the fake's Entra ID route


def test_migration_with_selection_does_not_crawl(client: TestClient) -> None:
    with respx.mock(assert_all_called=False) as router:
        mock_workspace(router)
        router.get(f"{HOST}/api/2.0/workspace/export").mock(
            side_effect=lambda req: httpx.Response(
                200,
                json={
                    "content": base64.b64encode(
                        (FIXTURES / "notebooks/etl/publish.py").read_bytes()
                    ).decode()
                },
            )
        )
        body = {
            "source": {
                "type": "databricks",
                "credentials": {"host": HOST, "token": "dapi"},
                "selection": {
                    "notebooks": ["/top"],
                    "jobs": ["7"],
                    "tables": ["main.gold.sales"],
                    "warehouses": ["wh-run"],
                    "pipelines": ["p1"],
                },
            }
        }
        r = client.post("/api/v1/migrations", json=body)
        assert r.status_code == 202, r.text
        job = wait_for_job(client, r.json()["id"])
        urls = [str(c.request.url) for c in router.calls]
    assert job["status"] == "succeeded", job
    assert job["inventory"]["by_type"] == {
        "Notebook": 1,
        "Job": 1,
        "DLT Pipeline": 1,
        "SQL Warehouse": 1,
        "Table/View": 1,
    }
    assert job["options"]["selection"] == {
        "notebooks": 1,
        "jobs": 1,
        "pipelines": 1,
        "warehouses": 1,
        "tables": 1,
    }
    assert not any(
        "workspace/list" in u or "jobs/list" in u or "unity-catalog/catalogs" in u for u in urls
    )
    items = {
        i["id"]: i for i in client.get(f"/api/v1/migrations/{job['id']}/items").json()["items"]
    }
    assert items["Job:nightly"]["outcome"] in ("needs_testing", "partial")
    assert items["SQL Warehouse:Shared"]["outcome"] == "migrated"
    assert (
        items["DLT Pipeline:bronze_dlt"]["outcome"] == "not_migrated"
    )  # manual redesign + AI work order
    r = client.post(
        "/api/v1/migrations",
        json={
            "source": {
                "type": "databricks",
                "credentials": {"host": HOST, "token": "d"},
                "selection": {"notebook": ["/x"]},
            }
        },
    )
    assert r.status_code == 400 and r.json()["error"]["field"] == "source.selection.notebook"


def test_placeholder_tenant_is_a_422_not_a_500(client: TestClient) -> None:
    r = client.post(
        "/api/v1/connections/fabric/test",
        json={
            "credentials": {
                "tenant_id": "<entra-tenant-id>",
                "client_id": "c",
                "client_secret": "s",
            }
        },
    )
    assert r.status_code == 422 and r.json()["error"]["field"] == "credentials"
