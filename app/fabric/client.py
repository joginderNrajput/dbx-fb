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
            try:
                js = r.json() if r.content else None
            except (
                ValueError
            ) as e:  # an HTML error page from a proxy or gateway, not the Fabric API
                raise FabricError(
                    f"{method} {path} -> {r.status_code}: response is not JSON: {r.text[:200]}"
                ) from e
            return r.status_code, dict(r.headers), js if isinstance(js, dict) else {}
        raise FabricError(f"{method} {path}: throttled")

    def upload(self, path: str, data: bytes, filename: str) -> tuple[int, dict[str, str]]:
        """POST a file: raw bytes first, then multipart ``file`` (endpoints differ in which they take)."""
        url = self.base + path
        attempts: list[dict[str, Any]] = [
            {"content": data, "headers": {"Content-Type": "application/octet-stream"}},
            {"files": {"file": (filename, data, "application/octet-stream")}},
        ]
        last = ""
        for kw in attempts:
            if "files" in kw:  # let httpx set the multipart boundary
                headers = {
                    k: v for k, v in self.http.headers.items() if k.lower() != "content-type"
                }
                req = httpx.Request("POST", url, headers=headers, files=kw["files"])
            else:
                req = self.http.build_request(
                    "POST", url, content=kw["content"], headers=kw["headers"]
                )
            try:
                r = self.http.send(req)
            except httpx.HTTPError as e:
                raise FabricError(f"POST {path}: {e}") from e
            FABRIC_REQUESTS.labels("POST", str(r.status_code)).inc()
            if r.status_code < 400:
                return r.status_code, dict(r.headers)
            last = f"{r.status_code}: {r.text[:300]}"
            if r.status_code not in (400, 415):
                break
        raise FabricError(f"POST {path} -> {last}", details={"status": int(last.split(":")[0])})

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

    def list_paged(self, path: str) -> list[dict[str, Any]]:
        """GET a Fabric list endpoint, following continuationToken."""
        out: list[dict[str, Any]] = []
        tok: str | None = None
        seen: set[str] = set()
        while True:
            sep = "&" if "?" in path else "?"
            _, _, js = self.req(
                "GET", path + (f"{sep}continuationToken={urllib.parse.quote(tok)}" if tok else "")
            )
            out += js.get("value", [])
            tok = js.get("continuationToken")
            if not tok or tok in seen:  # a repeated token would loop forever
                return out
            seen.add(tok)

    def list_workspaces(self) -> list[dict[str, Any]]:
        return self.list_paged("/workspaces")

    def list_lakehouses(self, ws: str) -> list[dict[str, Any]]:
        return self.list_paged(f"/workspaces/{ws}/lakehouses")

    def list_items(self, ws: str, item_type: str | None = None) -> list[dict[str, Any]]:
        return self.list_paged(
            f"/workspaces/{ws}/items"
            + (f"?type={urllib.parse.quote(item_type)}" if item_type else "")
        )

    def get_workspace(self, ws: str) -> dict[str, Any]:
        _, _, js = self.req("GET", f"/workspaces/{urllib.parse.quote(ws)}")
        return js

    def find_workspace(self, name: str) -> str:
        """Workspace id by display name (case-insensitive); the token must see the workspace."""
        ws_list = self.list_workspaces()
        match = [w for w in ws_list if str(w.get("displayName", "")).lower() == name.lower()]
        if not match:
            raise FabricError(
                f"Fabric workspace {name!r} not found or not accessible to these credentials "
                f"({len(ws_list)} workspace(s) visible); add the principal as a workspace Contributor"
            )
        return str(match[0]["id"])

    def get_lakehouse(self, ws: str, lakehouse_id: str) -> dict[str, Any]:
        _, _, js = self.req("GET", f"/workspaces/{ws}/lakehouses/{lakehouse_id}")
        return js

    def lakehouse_has_schemas(self, ws: str, lakehouse_id: str) -> bool:
        """Schema-enabled lakehouses report properties.defaultSchema (e.g. "dbo")."""
        return bool(
            (self.get_lakehouse(ws, lakehouse_id).get("properties") or {}).get("defaultSchema")
        )

    def run_notebook(
        self, ws: str, notebook_id: str, *, poll_s: float, timeout_s: int
    ) -> dict[str, Any]:
        """Run a notebook on demand (job scheduler) and wait for a terminal job status."""
        _, h, _ = self.req(
            "POST", f"/workspaces/{ws}/items/{notebook_id}/jobs/instances?jobType=RunNotebook", {}
        )
        loc = h.get("Location") or h.get("location")
        if not loc:
            raise FabricError("notebook run accepted without a job instance Location")
        deadline = time.monotonic() + timeout_s
        while True:
            time.sleep(poll_s)
            _, _, js = self.req("GET", loc, full=True)
            status = js.get("status")
            if status in ("Completed", "Failed", "Cancelled", "Deduped"):
                return js
            if time.monotonic() > deadline:
                raise FabricError(
                    f"notebook run did not finish within {timeout_s}s (last status {status})"
                )

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
