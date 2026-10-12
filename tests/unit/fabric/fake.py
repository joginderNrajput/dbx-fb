"""A stateful respx fake of the Fabric REST API (items, LROs, shortcuts, schedules)."""

from __future__ import annotations

import json
import uuid
from typing import Any

import httpx
import respx

BASE = "https://api.fabric.microsoft.com/v1"
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
