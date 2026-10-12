"""A stateful respx fake of the Fabric REST API (items, LROs, shortcuts, schedules)."""

from __future__ import annotations

import base64
import json
import uuid
from typing import Any

import httpx
import respx

BASE = "https://api.fabric.microsoft.com/v1"
ONELAKE = "https://onelake.dfs.fabric.microsoft.com"
ENDPOINT_TYPES = {
    "lakehouses": "Lakehouse",
    "warehouses": "Warehouse",
    "sqlDatabases": "SQLDatabase",
    "notebooks": "Notebook",
    "dataPipelines": "DataPipeline",
}


class FakeFabric:
    def __init__(self, router: respx.MockRouter, ws: str = "ws-1") -> None:
        self.ws = ws
        self.items: list[dict[str, Any]] = []
        self.definitions: dict[str, Any] = {}
        self.shortcuts: list[dict[str, Any]] = []
        self.schedules: list[dict[str, Any]] = []
        self.calls: list[str] = []
        router.post(url__regex=r"https://login\.microsoftonline\.com/.+/oauth2/v2\.0/token").mock(
            return_value=httpx.Response(200, json={"access_token": "fab-token"})
        )
        router.get(f"{BASE}/workspaces").mock(
            return_value=httpx.Response(200, json={"value": [{"id": ws}]})
        )
        router.get(f"{BASE}/workspaces/{ws}").mock(
            return_value=httpx.Response(200, json={"id": ws, "displayName": "Migration WS"})
        )
        router.get(f"{BASE}/workspaces/{ws}/items").mock(
            side_effect=lambda r: httpx.Response(200, json={"value": self.items})
        )
        router.post(
            url__regex=rf"{BASE}/workspaces/{ws}/(lakehouses|warehouses|sqlDatabases|notebooks|dataPipelines)$"
        ).mock(side_effect=self._create)
        router.post(url__regex=rf"{BASE}/workspaces/{ws}/items/[^/]+/updateDefinition").mock(
            side_effect=self._update
        )
        router.post(url__regex=rf"{BASE}/workspaces/{ws}/items/[^/]+/shortcuts").mock(
            side_effect=self._shortcut
        )
        router.post(url__regex=rf"{BASE}/workspaces/{ws}/items/[^/]+/jobs/Pipeline/schedules").mock(
            side_effect=self._schedule
        )
        router.get(url__regex=rf"{BASE}/operations/.+").mock(
            return_value=httpx.Response(200, json={"status": "Succeeded"})
        )
        router.get(url__regex=rf"{BASE}/workspaces/{ws}/lakehouses/[^/]+$").mock(
            side_effect=self._lakehouse
        )
        router.post(url__regex=rf"{BASE}/workspaces/{ws}/items/[^/]+/jobs/instances").mock(
            side_effect=self._run_job
        )
        router.get(url__regex=rf"{BASE}/jobs/.+").mock(side_effect=self._job_status)
        router.route(url__startswith=ONELAKE).mock(side_effect=self._onelake)
        self.no_schema_lakehouses: set[str] = set()
        self.files: dict[str, bytes] = {}
        self.runs: list[str] = []
        self.fail_runs: set[str] = set()

    def _create(self, request: httpx.Request) -> httpx.Response:
        endpoint = request.url.path.rsplit("/", 1)[-1]
        body = json.loads(request.content)
        iid = str(uuid.uuid4())
        self.items.append(
            {"id": iid, "type": ENDPOINT_TYPES[endpoint], "displayName": body["displayName"]}
        )
        self.definitions[iid] = body.get("definition")
        self.calls.append(f"create {ENDPOINT_TYPES[endpoint]} {body['displayName']}")
        if body.get("definition"):  # items with a definition are created asynchronously
            return httpx.Response(
                202, headers={"Location": f"{BASE}/operations/{iid}", "Retry-After": "0"}
            )
        return httpx.Response(201, json={"id": iid})

    def _update(self, request: httpx.Request) -> httpx.Response:
        iid = request.url.path.split("/")[-2]
        self.definitions[iid] = json.loads(request.content)["definition"]
        self.calls.append(f"update {iid}")
        return httpx.Response(200, json={})

    def _shortcut(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if any(s["name"] == body["name"] and s["path"] == body["path"] for s in self.shortcuts):
            return httpx.Response(409, json={"errorCode": "AlreadyExists"})
        self.shortcuts.append(body)
        return httpx.Response(201, json=body)

    def _schedule(self, request: httpx.Request) -> httpx.Response:
        self.schedules.append(json.loads(request.content))
        return httpx.Response(201, json={"id": "sched"})

    def item(self, type_: str, name: str) -> dict[str, Any] | None:
        return next(
            (i for i in self.items if i["type"] == type_ and i["displayName"] == name), None
        )

    # ── lakehouse properties, notebook jobs, OneLake ──
    def _lakehouse(self, request: httpx.Request) -> httpx.Response:
        iid = request.url.path.rsplit("/", 1)[-1]
        item = next((i for i in self.items if i["id"] == iid), None)
        props = (
            {}
            if item and item["displayName"] in self.no_schema_lakehouses
            else {"defaultSchema": "dbo"}
        )
        return httpx.Response(200, json={"id": iid, "properties": props})

    def notebook(self, iid: str) -> dict[str, Any]:
        d = self.definitions[iid]
        return json.loads(base64.b64decode(d["parts"][0]["payload"]))

    def _run_job(self, request: httpx.Request) -> httpx.Response:
        iid = request.url.path.split("/")[-3]
        name = next(i["displayName"] for i in self.items if i["id"] == iid)
        self.runs.append(name)
        status = "Failed" if name in self.fail_runs else "Completed"
        if name.startswith("load_migrated_data_") and status == "Completed":
            self._simulate_loader(iid)
        return httpx.Response(202, headers={"Location": f"{BASE}/jobs/{status}/{iid}"})

    def _job_status(self, request: httpx.Request) -> httpx.Response:
        status = request.url.path.split("/")[-2]
        return httpx.Response(
            200,
            json={
                "status": status,
                "failureReason": None if status == "Completed" else {"message": "boom"},
            },
        )

    def _simulate_loader(self, iid: str) -> None:
        """What the generated loader notebook does in Fabric: count rows per exported table."""
        nb = self.notebook(iid)
        lh_id = nb["metadata"]["dependencies"]["lakehouse"]["default_lakehouse"]
        scope: dict[str, Any] = {}
        for c in nb["cells"]:
            src = "".join(c["source"])
            if src.startswith(("RESULTS_PATH", "MANIFEST")):
                exec(src, scope)  # parameters + manifest cells are plain assignments
        results = []
        for t in scope["MANIFEST"]:
            prefix = f"{self.ws}/{lh_id}/{t['path']}/"
            rows = sum(len(v.splitlines()) for k, v in self.files.items() if k.startswith(prefix))
            ok = rows == t["rows"]
            results.append(
                {
                    "id": t["id"],
                    "status": "PASS" if ok else "FAIL",
                    "target": t["target"],
                    "rows_source": t["rows"],
                    "rows_target": rows,
                    "note": "" if ok else f"rows {t['rows']} != {rows}",
                }
            )
        self.files[f"{self.ws}/{lh_id}/{scope['RESULTS_PATH']}"] = json.dumps(results).encode()

    def _onelake(self, request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer fab-token"
        path = request.url.path.lstrip("/")
        q = dict(request.url.params)
        if request.method == "PUT":
            self.files[path] = b""
            return httpx.Response(201)
        if request.method == "PATCH" and q.get("action") == "append":
            self.files[path] = self.files.get(path, b"") + request.content
            return httpx.Response(202)
        if request.method == "PATCH":
            return httpx.Response(200)
        if request.method == "DELETE":
            for k in [k for k in self.files if k == path or k.startswith(path + "/")]:
                del self.files[k]
            return httpx.Response(200)
        if path in self.files:
            return httpx.Response(200, content=self.files[path])
        return httpx.Response(404)
