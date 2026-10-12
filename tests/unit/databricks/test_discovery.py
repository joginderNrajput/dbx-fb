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
    router.get(f"{HOST}/api/2.0/pipelines").mock(
        return_value=httpx.Response(
            200, json={"statuses": [{"pipeline_id": "p1", "name": "bronze_dlt", "state": "IDLE"}]}
        )
    )
    router.get(f"{HOST}/api/2.0/pipelines/p1").mock(
        return_value=httpx.Response(
            200,
            json={
                "pipeline_id": "p1",
                "name": "bronze_dlt",
                "state": "IDLE",
                "spec": {
                    "catalog": "main",
                    "target": "bronze",
                    "libraries": [{"notebook": {"path": "/Repos/team/dlt"}}],
                },
            },
        )
    )
    router.get(f"{HOST}/api/2.0/sql/warehouses").mock(
        return_value=httpx.Response(
            200,
            json={
                "warehouses": [
                    {"id": "wh-stopped", "name": "Starter", "state": "STOPPED"},
                    {"id": "wh-run", "name": "Shared", "state": "RUNNING", "cluster_size": "Small"},
                ]
            },
        )
    )
    router.get(f"{HOST}/api/2.0/sql/warehouses/wh-run").mock(
        return_value=httpx.Response(
            200, json={"id": "wh-run", "name": "Shared", "state": "RUNNING"}
        )
    )
    router.get(f"{HOST}/api/2.0/workspace/get-status").mock(
        side_effect=lambda r: (
            httpx.Response(
                200,
                json={
                    "/Repos/team": {"object_type": "REPO", "path": "/Repos/team"},
                    "/top": {"object_type": "NOTEBOOK", "path": "/top", "language": "SQL"},
                }.get(r.url.params["path"], {}),
            )
            if r.url.params["path"] in ("/Repos/team", "/top")
            else httpx.Response(404, json={"error_code": "RESOURCE_DOES_NOT_EXIST"})
        )
    )
    router.get(f"{HOST}/api/2.1/jobs/get").mock(
        side_effect=lambda r: (
            httpx.Response(
                200,
                json={
                    "job_id": int(r.url.params["job_id"]),
                    "settings": {
                        "name": "nightly",
                        "tasks": [{"task_key": "t", "notebook_task": {"notebook_path": "/top"}}],
                    },
                },
            )
            if r.url.params["job_id"] == "7"
            else httpx.Response(
                400,
                json={"error_code": "INVALID_PARAMETER_VALUE", "message": "Job 99 does not exist."},
            )
        )
    )
    router.get(f"{HOST}/api/2.0/clusters/get").mock(
        return_value=httpx.Response(
            200, json={"cluster_name": "etl", "cluster_id": "c1", "spark_version": "13.3"}
        )
    )
    router.get(f"{HOST}/api/2.1/unity-catalog/tables/main.gold.sales").mock(
        return_value=httpx.Response(
            200,
            json={
                "name": "sales",
                "full_name": "main.gold.sales",
                "table_type": "EXTERNAL",
                "data_source_format": "DELTA",
                "storage_location": "abfss://c@a.dfs.core.windows.net/sales",
                "columns": [{"name": "dt", "partition_index": 0}],
            },
        )
    )
    router.get(f"{HOST}/api/2.0/secrets/list").mock(
        return_value=httpx.Response(200, json={"secrets": [{"key": "pw"}]})
    )
    mock_extra_objects(router)


VOLUME = {
    "name": "raw",
    "full_name": "main.gold.raw",
    "volume_type": "EXTERNAL",
    "storage_location": "abfss://c@a.dfs.core.windows.net/raw",
}
FUNCTION = {
    "name": "add_tax",
    "full_name": "main.gold.add_tax",
    "routine_body": "SQL",
    "routine_definition": "x * 1.2",
    "data_type": "DOUBLE",
    "full_data_type": "DOUBLE",
    "input_params": {"parameters": [{"name": "x", "type_text": "double"}]},
}
MODEL = {"name": "churn", "full_name": "main.gold.churn", "storage_location": "abfss://m"}


def mock_extra_objects(router: respx.MockRouter) -> None:
    """Every other component kind discovery inventories (volumes ... libraries, workspace files)."""

    def ok(**body: object) -> httpx.Response:
        return httpx.Response(200, json=body)

    uc = f"{HOST}/api/2.1/unity-catalog"
    router.get(f"{uc}/volumes").mock(return_value=ok(volumes=[VOLUME]))
    router.get(f"{uc}/volumes/main.gold.raw").mock(return_value=ok(**VOLUME))
    router.get(f"{uc}/functions").mock(return_value=ok(functions=[FUNCTION]))
    router.get(f"{uc}/functions/main.gold.add_tax").mock(return_value=ok(**FUNCTION))
    router.get(f"{uc}/models").mock(return_value=ok(registered_models=[MODEL]))
    router.get(f"{uc}/external-locations").mock(
        return_value=ok(
            external_locations=[{"name": "landing", "url": "abfss://l@a.dfs.core.windows.net/"}]
        )
    )
    router.get(f"{uc}/connections").mock(
        return_value=ok(
            connections=[
                {
                    "name": "pg",
                    "connection_type": "POSTGRESQL",
                    "options": {"host": "db", "port": "5432", "password": "never-copied"},
                }
            ]
        )
    )
    router.get(f"{HOST}/api/2.0/sql/queries").mock(
        return_value=ok(
            results=[{"id": "q1", "display_name": "top sales", "query_text": "SELECT 1"}]
        )
    )
    router.get(f"{HOST}/api/2.0/sql/queries/q1").mock(
        return_value=ok(id="q1", display_name="top sales", query_text="SELECT 1")
    )
    router.get(f"{HOST}/api/2.0/sql/alerts").mock(
        return_value=ok(results=[{"id": "a1", "display_name": "late load", "query_id": "q1"}])
    )
    router.get(f"{HOST}/api/2.0/lakeview/dashboards").mock(
        return_value=ok(dashboards=[{"dashboard_id": "d1", "display_name": "Sales"}])
    )
    router.get(f"{HOST}/api/2.0/serving-endpoints").mock(
        return_value=ok(
            endpoints=[
                {
                    "name": "churn-ep",
                    "config": {"served_entities": [{"entity_name": "main.gold.churn"}]},
                }
            ]
        )
    )
    router.get(f"{HOST}/api/2.0/repos").mock(
        return_value=ok(repos=[{"id": 5, "path": "/Repos/team", "url": "https://git/x"}])
    )
    router.get(f"{HOST}/api/2.0/libraries/all-cluster-statuses").mock(
        return_value=ok(
            statuses=[
                {
                    "cluster_id": "c1",
                    "library_statuses": [
                        {"library": {"pypi": {"package": "great-expectations==1.0"}}},
                        {"library": {"maven": {"coordinates": "com.x:y:1.0"}}},
                    ],
                }
            ]
        )
    )
    vol = "/Volumes/main/gold/raw"
    tree = {
        vol: [
            {"path": f"{vol}/a.csv", "is_directory": False, "file_size": 3},
            {"path": f"{vol}/sub/", "is_directory": True},
        ],
        f"{vol}/sub": [{"path": f"{vol}/sub/b.bin", "is_directory": False, "file_size": 2}],
    }
    router.get(url__startswith=f"{HOST}/api/2.0/fs/directories").mock(
        side_effect=lambda r: ok(
            contents=tree.get(r.url.path.removeprefix("/api/2.0/fs/directories"), [])
        )
    )
    router.get(url__startswith=f"{HOST}/api/2.0/fs/files").mock(
        side_effect=lambda r: httpx.Response(
            200, content=b"abc" if r.url.path.endswith(".csv") else b"\x00\x01"
        )
    )
    router.get(f"{HOST}/api/2.0/libraries/cluster-status").mock(
        return_value=ok(
            library_statuses=[{"library": {"pypi": {"package": "great-expectations==1.0"}}}]
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
    fn = by[("Function", "main.gold.add_tax")]["meta"]
    assert fn["language"] == "SQL" and fn["input_params"] == [{"name": "x", "type": "double"}]
    assert by[("Volume", "main.gold.raw")]["meta"]["storage_location"].startswith("abfss://")
    assert by[("Connection", "pg")]["meta"]["options"] == {"host": "db", "port": "5432"}
    assert by[("SQL Query", "top sales")]["meta"]["query_text"] == "SELECT 1"
    assert by[("Library", "great-expectations==1.0")]["meta"]["kind"] == "pypi"
    for kind in ("ML Model", "External Location", "Alert", "Dashboard", "Serving Endpoint", "Repo"):
        assert kind in assess["summary"]["by_type"], kind


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


def test_crawl_includes_pipelines_and_warehouses(settings: Settings) -> None:
    with respx.mock(assert_all_called=False) as router:
        mock_workspace(router)
        with DatabricksClient(DatabricksCredentials(HOST, "dapi-x"), settings) as c:
            assess = discover(c, DiscoveryOptions())
    types = assess["summary"]["by_type"]
    assert types["DLT Pipeline"] == 1 and types["SQL Warehouse"] == 2


def test_selection_fetches_only_picked_objects(settings: Settings) -> None:
    from app.databricks.discovery import Selection, discover_selected

    sel = Selection(
        notebooks=["/Repos/team", "/top", "/gone"],
        jobs=["7", "99"],
        clusters=["c1"],
        pipelines=["p1"],
        warehouses=["wh-run"],
        tables=["main.gold.sales", "main.gold", "bad"],
        secret_scopes=["kv"],
        volumes=["main.gold.raw"],
        functions=["main.gold"],
        queries=["q1"],
    )
    with respx.mock(assert_all_called=False) as router:
        mock_workspace(router)
        with DatabricksClient(DatabricksCredentials(HOST, "dapi-x"), settings) as c:
            assess = discover_selected(c, sel)
        catalogs_called = any(
            "unity-catalog/catalogs" in str(call.request.url) for call in router.calls
        )
        jobs_list_called = any("jobs/list" in str(call.request.url) for call in router.calls)
    by = {(a["type"], a["name"]) for a in assess["assets"]}
    assert ("Notebook", "load_sales") in by and (
        "Notebook",
        "top",
    ) in by  # folder expanded + direct notebook
    assert (
        ("Job", "nightly") in by
        and ("Cluster", "etl") in by
        and ("DLT Pipeline", "bronze_dlt") in by
    )
    assert (
        ("SQL Warehouse", "Shared") in by
        and ("Table/View", "main.gold.sales") in by
        and ("Secret Scope", "kv") in by
    )
    assert (
        sum(1 for a in assess["assets"] if a["name"] == "main.gold.sales") == 1
    )  # table + schema selection deduped
    assert ("Volume", "main.gold.raw") in by and ("Function", "main.gold.add_tax") in by
    assert ("SQL Query", "top sales") in by
    assert ("Library", "great-expectations==1.0") in by  # pulled in with cluster c1
    assert not catalogs_called and not jobs_list_called  # no crawl
    assert assess["source"]["inventory"] == "selection"
    w = " | ".join(assess["warnings"])
    assert "notebook /gone" in w and "job 99" in w and "table bad" in w


def test_non_json_responses_are_upstream_errors(settings: Settings) -> None:
    from app.core.exceptions import DatabricksError

    with respx.mock() as router:
        router.get(f"{HOST}/api/2.0/clusters/list").mock(
            return_value=httpx.Response(200, text="<html>SSO</html>")
        )
        router.post(f"{HOST}/oidc/v1/token").mock(
            return_value=httpx.Response(200, text="<html>SSO</html>")
        )
        with (
            DatabricksClient(DatabricksCredentials(HOST, "dapi"), settings) as c,
            pytest.raises(DatabricksError, match="not JSON"),
        ):
            c.list_clusters()
        with (
            DatabricksClient(
                DatabricksCredentials(HOST, client_id="i", client_secret="s"), settings
            ) as c,
            pytest.raises(DatabricksError, match="not JSON"),
        ):
            c.list_clusters()
