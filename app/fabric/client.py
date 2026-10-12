"""Fabric REST client: items, long-running operations, throttling. Spec §5.4."""

from __future__ import annotations

import base64
import json
import time
import urllib.parse
from typing import Any

import httpx

from app.core.config import Settings
from app.core.exceptions import FabricError
from app.core.metrics import FABRIC_REQUESTS

__all__ = ["Fabric", "b64"]


def b64(obj: Any) -> str:
    return base64.b64encode(
        json.dumps(obj).encode() if not isinstance(obj, str) else obj.encode()
    ).decode()


class Fabric:
    def __init__(
        self, token: str, settings: Settings, transport: httpx.BaseTransport | None = None
    ) -> None:
        self.settings = settings
        self.base = settings.FABRIC_API_BASE.rstrip("/")
        self.http = httpx.Client(
            timeout=settings.FABRIC_TIMEOUT_S,
            transport=transport,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        )
        self._items: dict[str, list[dict[str, Any]]] = {}

    def close(self) -> None:
        self.http.close()

    def req(
        self, method: str, path: str, body: Any = None, full: bool = False
    ) -> tuple[int, dict[str, str], dict[str, Any]]:
        url = path if full else self.base + path
        for _ in range(self.settings.FABRIC_MAX_THROTTLE_RETRIES):
            try:
                r = self.http.request(
                    method, url, content=json.dumps(body).encode() if body is not None else None
                )
            except httpx.HTTPError as e:
                FABRIC_REQUESTS.labels(method, "network").inc()
                raise FabricError(f"{method} {path}: {e}") from e
            FABRIC_REQUESTS.labels(method, str(r.status_code)).inc()
            if r.status_code == 429:
                time.sleep(min(int(r.headers.get("Retry-After", 5)), 60))
                continue
            if r.status_code >= 400:
                raise FabricError(
                    f"{method} {path} -> {r.status_code}: {r.text[:300]}",
                    details={"status": r.status_code},
                )
            js = r.json() if r.content else None
            return r.status_code, dict(r.headers), js or {}
        raise FabricError(f"{method} {path}: throttled")

    def wait(self, headers: dict[str, str]) -> dict[str, Any]:
        loc = headers.get("Location") or headers.get("location")
        if not loc:
            return {}
        for i in range(self.settings.FABRIC_LRO_MAX_POLLS):
            time.sleep(
                float(
                    headers.get("Retry-After")
                    or headers.get("retry-after")
                    or self.settings.FABRIC_LRO_POLL_S
                )
                if i == 0
                else self.settings.FABRIC_LRO_POLL_S
            )
            _, _, js = self.req("GET", loc, full=True)
            if js.get("status") in ("Succeeded", "Completed"):
                return js
            if js.get("status") == "Failed":
                raise FabricError(f"operation failed: {js}")
        raise FabricError("operation timed out")

    def list_workspaces(self) -> list[dict[str, Any]]:
        _, _, js = self.req("GET", "/workspaces")
        out: list[dict[str, Any]] = js.get("value", [])
        return out

    def get_workspace(self, ws: str) -> dict[str, Any]:
        _, _, js = self.req("GET", f"/workspaces/{urllib.parse.quote(ws)}")
        return js

    def find(self, ws: str, typ: str, name: str) -> str | None:
        if ws not in self._items:
            items: list[dict[str, Any]] = []
            tok = None
            while True:
                _, _, js = self.req(
                    "GET",
                    f"/workspaces/{ws}/items"
                    + (f"?continuationToken={urllib.parse.quote(tok)}" if tok else ""),
                )
                items += js.get("value", [])
                tok = js.get("continuationToken")
                if not tok:
                    break
            self._items[ws] = items
        return next(
            (i["id"] for i in self._items[ws] if i["type"] == typ and i["displayName"] == name),
            None,
        )

    def ensure(
        self,
        ws: str,
        endpoint: str,
        typ: str,
        name: str,
        extra: dict[str, Any] | None = None,
        definition: dict[str, Any] | None = None,
    ) -> tuple[str, str]:
        """Create the item or update its definition: deploys are idempotent and safe to re-run."""
        iid = self.find(ws, typ, name)
        if iid:
            if definition:
                st, h, _ = self.req(
                    "POST",
                    f"/workspaces/{ws}/items/{iid}/updateDefinition",
                    {"definition": definition},
                )
                if st == 202:
                    self.wait(h)
            return iid, "updated" if definition else "existing"
        body: dict[str, Any] = {"displayName": name, **(extra or {})}
        if definition:
            body["definition"] = definition
        st, h, js = self.req("POST", f"/workspaces/{ws}/{endpoint}", body)
        if st == 202:
            self.wait(h)
        self._items.pop(ws, None)
        iid = js.get("id") or self.find(ws, typ, name)
        if not iid:
            raise FabricError(f"{typ} '{name}' created but id not found")
        return iid, "created"
