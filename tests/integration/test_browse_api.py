"""Browse API (POST /databricks/*, /fabric/*: credentials in the body) and selection-based migrations."""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx
from fastapi.testclient import TestClient

from app.main import create_app
from tests.conftest import FIXTURES, make_settings, wait_for_job
from tests.unit.databricks.test_discovery import HOST, mock_workspace
from tests.unit.fabric.fake import BASE, FakeFabric

pytestmark = pytest.mark.integration
PAT = {"host": HOST, "personal_access_token": "dapi-123"}
SPN = {"host": HOST, "client_id": "sp-id", "client_secret": "sp-secret"}
FAB = {"tenant_id": "t1", "client_id": "c1", "client_secret": "s1"}


def dbx(**extra: Any) -> dict[str, Any]:
    return {"credentials": PAT, **extra}


def test_databricks_browse_endpoints(client: TestClient) -> None:
    with respx.mock(assert_all_called=False) as router:
        mock_workspace(router)
        nbs = client.post("/api/v1/databricks/notebooks", json=dbx()).json()
        assert nbs["selection_key"] == "notebooks" and nbs["id_field"] == "path"
        assert {i["path"] for i in nbs["items"]} == {"/top", "/Repos/team/load_sales"}
        scoped = client.post("/api/v1/databricks/notebooks", json=dbx(paths=["/Repos/team"])).json()
        assert [i["path"] for i in scoped["items"]] == ["/Repos/team/load_sales"]
        limited = client.post("/api/v1/databricks/notebooks", json=dbx(limit=1)).json()
        assert limited["count"] == 1 and limited["truncated"] is True
        ws = client.post("/api/v1/databricks/workspace", json=dbx(path="/")).json()
        assert ws["items"][0]["object_type"] == "DIRECTORY"

        jobs = client.post("/api/v1/databricks/jobs", json=dbx()).json()
        assert jobs["selection_key"] == "jobs" and jobs["items"][0]["job_id"] == 7
        assert client.post("/api/v1/databricks/jobs", json=dbx(name="nomatch")).json()["count"] == 0
        assert (
            client.post("/api/v1/databricks/jobs/7", json=dbx()).json()["settings"]["name"]
            == "nightly"
        )

        clusters = client.post("/api/v1/databricks/clusters", json=dbx()).json()
        assert [c["cluster_id"] for c in clusters["items"]] == ["c1"]
        assert (
            client.post("/api/v1/databricks/clusters", json=dbx(include_job_clusters=True)).json()[
                "count"
            ]
            == 2
        )
        assert (
            client.post("/api/v1/databricks/pipelines", json=dbx()).json()["items"][0][
                "pipeline_id"
            ]
            == "p1"
        )
        wh = client.post("/api/v1/databricks/warehouses", json=dbx()).json()
        assert wh["id_field"] == "id" and {w["state"] for w in wh["items"]} == {
            "RUNNING",
            "STOPPED",
        }
        cats = client.post("/api/v1/databricks/catalogs", json=dbx()).json()
        assert [c["name"] for c in cats["items"]] == ["main"]
        schemas = client.post("/api/v1/databricks/catalogs/main/schemas", json=dbx()).json()
        assert [s["full_name"] for s in schemas["items"]] == ["main.gold"] and schemas[
            "selection_key"
        ] == "tables"
        tables = client.post(
            "/api/v1/databricks/catalogs/main/schemas/gold/tables", json=dbx()
        ).json()
        assert (
            tables["items"][0]["full_name"] == "main.gold.sales"
            and tables["items"][0]["columns"] == 2
        )
        assert (
            client.post("/api/v1/databricks/secret-scopes", json=dbx()).status_code == 502
        )  # fake's scopes/list fails
        inv = client.post("/api/v1/databricks/inventory", json=dbx(catalogs=["main"])).json()
        assert inv["summary"]["by_type"]["Notebook"] == 2 and "assets" in inv
        assert all("Bearer dapi-123" == c.request.headers["Authorization"] for c in router.calls)


def test_service_principal_wins_over_pat(client: TestClient) -> None:
    with respx.mock(assert_all_called=False) as router:
        mock_workspace(router)
        token = router.post(f"{HOST}/oidc/v1/token").mock(
            return_value=httpx.Response(200, json={"access_token": "oauth-tok"})
        )
        both = {**SPN, "personal_access_token": "dapi-ignored"}
        r = client.post("/api/v1/databricks/clusters", json={"credentials": both})
        assert r.status_code == 200
        assert (
            token.called
            and router.calls.last.request.headers["Authorization"] == "Bearer oauth-tok"
        )
        assert "dapi-ignored" not in str([c.request.headers for c in router.calls])
        # the alias "token" is accepted for the PAT
        r = client.post(
            "/api/v1/databricks/clusters",
            json={"credentials": {"host": HOST, "token": "dapi-alias"}},
        )
        assert (
            r.status_code == 200
            and router.calls.last.request.headers["Authorization"] == "Bearer dapi-alias"
        )
        # connection check reports which method was used
        router.get(f"{HOST}/api/2.0/preview/scim/v2/Me").mock(
            return_value=httpx.Response(200, json={"userName": "sp"})
        )
        assert (
            client.post("/api/v1/connections/databricks/test", json={"credentials": both}).json()[
                "auth_type"
            ]
            == "service_principal"
        )
        assert (
            client.post("/api/v1/connections/databricks/test", json={"credentials": PAT}).json()[
                "auth_type"
            ]
            == "personal_access_token"
        )


def test_credential_validation(client: TestClient) -> None:
    r = client.post("/api/v1/databricks/jobs", json={})
    assert r.status_code == 422 and r.json()["error"]["field"] == "credentials"
    r = client.post(
        "/api/v1/databricks/jobs",
        json={
            "credentials": {"host": HOST, "client_id": "only-id", "personal_access_token": "dapi"}
        },
    )
    assert (
        r.status_code == 422 and r.json()["error"]["field"] == "credentials.client_secret"
    )  # half a service principal
    r = client.post(
        "/api/v1/databricks/jobs",
        json={"credentials": {"host": HOST, "client_secret": "only-secret"}},
    )
    assert r.status_code == 422 and r.json()["error"]["field"] == "credentials.client_id"
    r = client.post(
        "/api/v1/databricks/jobs",
        json={"credentials": {"host": "https://10.0.0.5", "personal_access_token": "x"}},
    )
    assert r.status_code == 422 and "private" in r.json()["error"]["message"]
    r = client.post("/api/v1/databricks/jobs", json={"credentials": {**PAT, "password": "x"}})
    assert r.status_code == 400 and r.json()["error"]["field"] == "credentials.password"
    # Fabric: service principal only
    r = client.post("/api/v1/fabric/workspaces", json={})
    assert r.status_code == 422 and r.json()["error"]["field"] == "credentials"
    r = client.post("/api/v1/fabric/workspaces", json={"credentials": {"access_token": "bearer"}})
    assert r.status_code == 400
    r = client.post(
        "/api/v1/fabric/workspaces", json={"credentials": {"tenant_id": "t", "client_id": "c"}}
    )
    assert r.status_code == 400 and r.json()["error"]["field"] == "credentials.client_secret"
    r = client.post(
        "/api/v1/connections/fabric/test",
        json={"credentials": {**FAB, "tenant_id": "<entra-tenant-id>"}},
    )
    assert r.status_code == 422 and r.json()["error"]["field"] == "credentials.tenant_id"
    # GET is gone: credentials never travel outside a body
    assert client.get("/api/v1/databricks/jobs").status_code == 405


def test_env_credentials_fallback(tmp_path: Path) -> None:
    s = make_settings(
        tmp_path,
        ALLOW_ENV_CREDENTIALS=True,
        DATABRICKS_HOST=HOST,
        DATABRICKS_TOKEN="dapi-env",
        DATABRICKS_CLIENT_ID="env-sp",
        DATABRICKS_CLIENT_SECRET="env-secret",
    )
    with TestClient(create_app(s)) as c, respx.mock(assert_all_called=False) as router:
        mock_workspace(router)
        router.post(f"{HOST}/oidc/v1/token").mock(
            return_value=httpx.Response(200, json={"access_token": "env-oauth"})
        )
        assert c.post("/api/v1/databricks/clusters", json={}).status_code == 200
        assert (
            router.calls.last.request.headers["Authorization"] == "Bearer env-oauth"
        )  # SP wins in settings too


def test_fabric_browse_endpoints(client: TestClient) -> None:
    ws_id = "11111111-2222-3333-4444-555555555555"
    with respx.mock(assert_all_called=False) as router:
        FakeFabric(router)
        router.get(f"{BASE}/workspaces").mock(
            return_value=httpx.Response(
                200,
                json={
                    "value": [
                        {
                            "id": ws_id,
                            "displayName": "Alteryx_Fabric",
                            "type": "Workspace",
                            "capacityId": "cap",
                        }
                    ]
                },
            )
        )

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
                            "properties": {"defaultSchema": "dbo"},
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
        body = {"credentials": FAB}
        wss = client.post("/api/v1/fabric/workspaces", json=body).json()
        assert wss["items"] == [
            {"id": ws_id, "name": "Alteryx_Fabric", "type": "Workspace", "capacity_id": "cap"}
        ]
        lhs = client.post("/api/v1/fabric/workspaces/Alteryx_Fabric/lakehouses", json=body).json()
        assert [(lh["name"], lh["schemas_enabled"]) for lh in lhs["items"]] == [
            ("Src_LK", True),
            ("Old_LK", False),
        ]
        items = client.post(
            f"/api/v1/fabric/workspaces/{ws_id}/items", json={**body, "type": "Notebook"}
        ).json()
        assert items["items"][0]["name"] == "nb" and "type=Notebook" in str(
            router.calls.last.request.url
        )
        assert (
            client.post("/api/v1/fabric/workspaces/Nope/lakehouses", json=body).status_code == 502
        )


def test_migration_with_selection_and_service_principal(client: TestClient) -> None:
    with respx.mock(assert_all_called=False) as router:
        mock_workspace(router)
        router.post(f"{HOST}/oidc/v1/token").mock(
            return_value=httpx.Response(200, json={"access_token": "oauth-tok"})
        )
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
                "credentials": SPN,
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
        auths = {
            c.request.headers.get("Authorization")
            for c in router.calls
            if "/api/2." in str(c.request.url)
        }
    assert job["status"] == "succeeded", job
    assert job["options"]["databricks_auth"] == "service_principal" and auths == {
        "Bearer oauth-tok"
    }
    assert job["inventory"]["by_type"] == {
        "Notebook": 1,
        "Job": 1,
        "DLT Pipeline": 1,
        "SQL Warehouse": 1,
        "Table/View": 1,
    }
    assert not any(
        "workspace/list" in u or "jobs/list" in u or "unity-catalog/catalogs" in u for u in urls
    )
    r = client.post(
        "/api/v1/migrations",
        json={
            "source": {"type": "databricks", "credentials": PAT, "selection": {"notebook": ["/x"]}}
        },
    )
    assert r.status_code == 400 and r.json()["error"]["field"] == "source.selection.notebook"
