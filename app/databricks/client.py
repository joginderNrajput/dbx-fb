"""Databricks REST client (workspace, jobs, Unity Catalog, secrets, clusters). Spec §5.1."""

from __future__ import annotations

import base64
import time
from collections.abc import Iterator
from typing import Any

import httpx

from app.core.config import Settings
from app.core.exceptions import DatabricksError
from app.databricks.auth import DatabricksCredentials, access_token

__all__ = ["DatabricksClient"]


class DatabricksClient:
    def __init__(
        self,
        creds: DatabricksCredentials,
        settings: Settings,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.creds = creds
        self.settings = settings
        self.http = httpx.Client(timeout=settings.DATABRICKS_TIMEOUT_S, transport=transport)
        self._token: str | None = None

    def close(self) -> None:
        self.http.close()

    def __enter__(self) -> DatabricksClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _headers(self) -> dict[str, str]:
        if self._token is None:
            self._token = access_token(self.creds, self.http)
        return {"Authorization": f"Bearer {self._token}"}

    def get(self, ep: str, **params: Any) -> dict[str, Any]:
        return self.request("GET", ep, params=params)

    def post(self, ep: str, body: dict[str, Any]) -> dict[str, Any]:
        return self.request("POST", ep, body=body)

    def request(
        self,
        method: str,
        ep: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Bounded retries on 429/5xx; any other failure is a DatabricksError."""
        url = self.creds.host + ep
        last = ""
        for attempt in range(self.settings.DATABRICKS_MAX_RETRIES + 1):
            try:
                r = self.http.request(
                    method,
                    url,
                    params={k: v for k, v in (params or {}).items() if v is not None},
                    json=body,
                    headers=self._headers(),
                )
            except httpx.HTTPError as e:
                last = str(e)
            else:
                if r.status_code == 200:
                    data: dict[str, Any] = r.json() if r.content else {}
                    return data
                last = f"{r.status_code}: {r.text[:300]}"
                if r.status_code not in (429, 500, 502, 503, 504):
                    break
            if attempt < self.settings.DATABRICKS_MAX_RETRIES:
                time.sleep(min(2**attempt, 10))
        raise DatabricksError(f"{method} {ep} failed -> {last}")

    # ── SQL warehouses / statement execution (data copy, spec §5.9) ──
    def list_warehouses(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = self.get("/api/2.0/sql/warehouses").get("warehouses", [])
        return out

    def execute_statement(
        self,
        warehouse_id: str,
        statement: str,
        *,
        disposition: str,
        timeout_s: int,
        row_limit: int | None = None,
    ) -> dict[str, Any]:
        """Submit and poll until a terminal state; raises DatabricksError unless SUCCEEDED."""
        body: dict[str, Any] = {
            "warehouse_id": warehouse_id,
            "statement": statement,
            "wait_timeout": "30s",
            "on_wait_timeout": "CONTINUE",
            "disposition": disposition,
            "format": "JSON_ARRAY",
        }
        if row_limit:
            body["row_limit"] = row_limit
        r = self.post("/api/2.0/sql/statements", body)
        deadline = time.monotonic() + timeout_s
        while r.get("status", {}).get("state") in ("PENDING", "RUNNING"):
            if time.monotonic() > deadline:
                self.request("POST", f"/api/2.0/sql/statements/{r['statement_id']}/cancel")
                raise DatabricksError(f"statement timed out after {timeout_s}s: {statement[:120]}")
            time.sleep(self.settings.DATA_COPY_POLL_S)
            r = self.get(f"/api/2.0/sql/statements/{r['statement_id']}")
        state = r.get("status", {}).get("state")
        if state != "SUCCEEDED":
            err = r.get("status", {}).get("error", {}).get("message", "")
            raise DatabricksError(f"statement {state}: {err[:300]}")
        return r

    def statement_chunk_rows(self, stmt: dict[str, Any], index: int) -> list[list[Any]]:
        """Rows of one result chunk (inline data_array, or downloaded from its external link)."""
        res = stmt.get("result") or {}
        if res.get("chunk_index", 0) != index or not (
            res.get("data_array") is not None or res.get("external_links")
        ):
            res = self.get(f"/api/2.0/sql/statements/{stmt['statement_id']}/result/chunks/{index}")
        if res.get("data_array") is not None:
            rows: list[list[Any]] = res["data_array"]
            return rows
        links = res.get("external_links") or []
        out: list[list[Any]] = []
        for link in links:
            # Presigned URL: credentials are embedded, so NO Authorization header may be sent.
            try:
                d = self.http.get(
                    link["external_link"], timeout=self.settings.DATABRICKS_TIMEOUT_S * 4
                )
            except httpx.HTTPError as e:
                raise DatabricksError(f"result download failed: {e}") from e
            if d.status_code != 200:
                raise DatabricksError(f"result download failed ({d.status_code}): {d.text[:200]}")
            out += d.json()
        return out

    # ── identity ──
    def current_user(self) -> dict[str, Any]:
        return self.get("/api/2.0/preview/scim/v2/Me")

    # ── workspace ──
    def list_workspace(self, path: str) -> list[dict[str, Any]]:
        objs: list[dict[str, Any]] = self.get("/api/2.0/workspace/list", path=path).get(
            "objects", []
        )
        return objs

    def export_source(self, path: str) -> str | None:
        r = self.get("/api/2.0/workspace/export", path=path, format="SOURCE")
        if "content" not in r:
            return None
        return base64.b64decode(r["content"]).decode("utf-8", "ignore")

    # ── jobs ──
    def iter_jobs(self) -> Iterator[dict[str, Any]]:
        token: str | None = None
        while True:
            r = self.get("/api/2.1/jobs/list", expand_tasks="true", limit=25, page_token=token)
            yield from r.get("jobs", [])
            token = r.get("next_page_token")
            if not token or not r.get("has_more", True):
                return

    # ── unity catalog ──
    def list_catalogs(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = self.get("/api/2.1/unity-catalog/catalogs").get("catalogs", [])
        return out

    def list_schemas(self, catalog: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = self.get(
            "/api/2.1/unity-catalog/schemas", catalog_name=catalog
        ).get("schemas", [])
        return out

    def iter_tables(self, catalog: str, schema: str) -> Iterator[dict[str, Any]]:
        token: str | None = None
        while True:
            r = self.get(
                "/api/2.1/unity-catalog/tables",
                catalog_name=catalog,
                schema_name=schema,
                page_token=token,
            )
            yield from r.get("tables", [])
            token = r.get("next_page_token")
            if not token:
                return

    # ── secrets / compute ──
    def list_secret_scopes(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = self.get("/api/2.0/secrets/scopes/list").get("scopes", [])
        return out

    def list_secret_keys(self, scope: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = self.get("/api/2.0/secrets/list", scope=scope).get(
            "secrets", []
        )
        return out

    def list_clusters(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = self.get("/api/2.0/clusters/list").get("clusters", [])
        return out
