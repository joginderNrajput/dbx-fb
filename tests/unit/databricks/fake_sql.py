"""respx fake of Databricks SQL warehouses + Statement Execution API (JSON_ARRAY results)."""

from __future__ import annotations

import json
import re
from typing import Any

import httpx
import respx

BLOB = "https://dbstorage123.blob.core.windows.net/results"


class FakeSQL:
    def __init__(
        self,
        router: respx.MockRouter,
        host: str,
        tables: dict[str, tuple[list[tuple[str, str]], list[list[Any]]]],
        chunk_rows: int = 2,
    ) -> None:
        self.tables = tables  # full name -> (columns [(name, type_text)], rows)
        self.chunk_rows = chunk_rows
        self.statements: dict[str, dict[str, Any]] = {}
        self.downloads: list[httpx.Request] = []
        router.get(f"{host}/api/2.0/sql/warehouses").mock(
            return_value=httpx.Response(
                200,
                json={
                    "warehouses": [
                        {"id": "wh-stopped", "state": "STOPPED"},
                        {"id": "wh-run", "state": "RUNNING"},
                    ]
                },
            )
        )
        router.post(f"{host}/api/2.0/sql/statements").mock(side_effect=self._execute)
        router.get(url__regex=rf"{re.escape(host)}/api/2.0/sql/statements/[^/]+$").mock(
            side_effect=self._get
        )
        router.get(
            url__regex=rf"{re.escape(host)}/api/2.0/sql/statements/[^/]+/result/chunks/\d+"
        ).mock(side_effect=self._chunk)
        router.get(url__startswith=BLOB).mock(side_effect=self._download)

    def _name(self, stmt: str) -> str:
        return re.search(r"FROM (.+)$", stmt).group(1).replace("`", "")  # type: ignore[union-attr]

    def _chunks(self, rows: list[list[Any]]) -> list[list[list[Any]]]:
        return [rows[i : i + self.chunk_rows] for i in range(0, len(rows), self.chunk_rows)] or [[]]

    def _result(self, sid: str, idx: int) -> dict[str, Any]:
        st = self.statements[sid]
        chunks = st["chunks"]
        res: dict[str, Any] = {"chunk_index": idx, "row_count": len(chunks[idx])}
        if st["disposition"] == "INLINE":
            res["data_array"] = chunks[idx]
        else:
            res["external_links"] = [
                {"chunk_index": idx, "external_link": f"{BLOB}/{sid}/{idx}?sig=x"}
            ]
        return res

    def _execute(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        sid = f"s{len(self.statements)}"
        name = self._name(body["statement"])
        if name not in self.tables:
            return httpx.Response(
                200,
                json={
                    "statement_id": sid,
                    "status": {
                        "state": "FAILED",
                        "error": {"message": f"TABLE_OR_VIEW_NOT_FOUND {name}"},
                    },
                },
            )
        cols, rows = self.tables[name]
        if body["statement"].startswith("SELECT COUNT(*)"):
            self.statements[sid] = {"disposition": "INLINE", "chunks": [[[str(len(rows))]]]}
            cols, rows = [("count(1)", "bigint")], [[str(len(rows))]]
        else:
            self.statements[sid] = {
                "disposition": body["disposition"],
                "chunks": self._chunks(rows),
            }
        chunks = self.statements[sid]["chunks"]
        manifest = {
            "format": "JSON_ARRAY",
            "schema": {
                "columns": [
                    {"name": c, "type_text": t, "position": i} for i, (c, t) in enumerate(cols)
                ]
            },
            "total_row_count": len(rows),
            "total_chunk_count": len(chunks) if rows else 0,
        }
        self.statements[sid]["manifest"] = manifest
        # first poll returns RUNNING to exercise the wait loop
        return httpx.Response(200, json={"statement_id": sid, "status": {"state": "RUNNING"}})

    def _get(self, request: httpx.Request) -> httpx.Response:
        sid = request.url.path.rsplit("/", 1)[-1]
        st = self.statements[sid]
        out = {"statement_id": sid, "status": {"state": "SUCCEEDED"}, "manifest": st["manifest"]}
        if st["manifest"]["total_row_count"]:
            out["result"] = self._result(sid, 0)
        return httpx.Response(200, json=out)

    def _chunk(self, request: httpx.Request) -> httpx.Response:
        parts = request.url.path.split("/")
        return httpx.Response(200, json=self._result(parts[-4], int(parts[-1])))

    def _download(self, request: httpx.Request) -> httpx.Response:
        assert "Authorization" not in request.headers  # presigned URL: no auth header allowed
        self.downloads.append(request)
        sid, idx = request.url.path.split("/")[-2:]
        return httpx.Response(200, json=self.statements[sid]["chunks"][int(idx)])
