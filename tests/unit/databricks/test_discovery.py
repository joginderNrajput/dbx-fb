from __future__ import annotations

import base64

import httpx
import pytest
import respx

from app.core.config import Settings
from app.core.exceptions import ValidationFailed
from app.databricks.auth import DatabricksCredentials
from app.databricks.client import DatabricksClient
from app.databricks.discovery import DiscoveryOptions, discover

HOST = "https://adb-1.2.azuredatabricks.net"


def mock_workspace(router: respx.MockRouter) -> None:
    def ws_list(request: httpx.Request) -> httpx.Response:
        path = request.url.params["path"]
        tree = {
            "/": [
                {"object_type": "DIRECTORY", "path": "/Repos"},
                {"object_type": "NOTEBOOK", "path": "/top", "language": "SQL"},
            ],
            "/Repos": [{"object_type": "REPO", "path": "/Repos/team"}],
            "/Repos/team": [
                {"object_type": "NOTEBOOK", "path": "/Repos/team/load_sales", "language": "PYTHON"}
            ],
        }
        return httpx.Response(200, json={"objects": tree.get(path, [])})

    router.get(f"{HOST}/api/2.0/workspace/list").mock(side_effect=ws_list)
    router.get(f"{HOST}/api/2.1/jobs/list").mock(
        return_value=httpx.Response(
            200,
            json={
                "jobs": [{"job_id": 7, "settings": {"name": "nightly", "tasks": []}}],
                "has_more": False,
            },
        )
    )
    router.get(f"{HOST}/api/2.1/unity-catalog/catalogs").mock(
        return_value=httpx.Response(200, json={"catalogs": [{"name": "main"}, {"name": "system"}]})
    )
    router.get(f"{HOST}/api/2.1/unity-catalog/schemas").mock(
        return_value=httpx.Response(
            200, json={"schemas": [{"name": "gold"}, {"name": "information_schema"}]}
        )
    )
    router.get(f"{HOST}/api/2.1/unity-catalog/tables").mock(
        return_value=httpx.Response(
            200,
            json={
                "tables": [
                    {
                        "name": "sales",
                        "full_name": "main.gold.sales",
                        "table_type": "EXTERNAL",
                        "data_source_format": "DELTA",
                        "storage_location": "abfss://c@a.dfs.core.windows.net/sales",
                        "columns": [{"name": "dt", "partition_index": 0}, {"name": "x"}],
                    }
                ]
            },
        )
    )
    router.get(f"{HOST}/api/2.0/secrets/scopes/list").mock(
        return_value=httpx.Response(500, text="boom")
    )
    router.get(f"{HOST}/api/2.0/clusters/list").mock(
        return_value=httpx.Response(
            200,
            json={
                "clusters": [
                    {"cluster_name": "etl", "cluster_id": "c1", "spark_version": "13.3"},
                    {"cluster_name": "job-1", "cluster_source": "JOB"},
                ]
            },
        )
    )


def test_discover_builds_assessment(settings: Settings) -> None:
    with respx.mock(assert_all_called=False) as router:
        mock_workspace(router)
        with DatabricksClient(DatabricksCredentials(HOST, "dapi-x"), settings) as c:
            assess = discover(c, DiscoveryOptions())
    by = {(a["type"], a["name"]): a for a in assess["assets"]}
    assert ("Notebook", "load_sales") in by and by[("Notebook", "load_sales")][
        "location"
    ] == "/Repos/team/load_sales"
    assert by[("Notebook", "top")]["meta"]["language"] == "SQL"
    assert ("Job", "nightly") in by and ("Cluster", "etl") in by and ("Cluster", "job-1") not in by
    t = by[("Table/View", "main.gold.sales")]["meta"]
    assert t["partition_columns"] == ["dt"] and t["format"] == "DELTA"
    assert any(
        "secret scopes" in w for w in assess["warnings"]
    )  # partial failure -> warning, not error


def test_export_and_oauth(settings: Settings) -> None:
    with respx.mock() as router:
        router.post(f"{HOST}/oidc/v1/token").mock(
            return_value=httpx.Response(200, json={"access_token": "oauth-tok"})
        )
        route = router.get(f"{HOST}/api/2.0/workspace/export").mock(
            return_value=httpx.Response(200, json={"content": base64.b64encode(b"x = 1").decode()})
        )
        with DatabricksClient(
            DatabricksCredentials(HOST, client_id="id", client_secret="s"), settings
        ) as c:
            assert c.export_source("/a") == "x = 1"
        assert route.calls[0].request.headers["Authorization"] == "Bearer oauth-tok"


def test_credentials_require_secret() -> None:
    with pytest.raises(ValidationFailed):
        DatabricksCredentials(HOST)
    assert "dapi" not in repr(DatabricksCredentials(HOST, "dapi-secret"))
