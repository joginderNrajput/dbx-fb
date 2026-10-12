"""OneLake file access over the ADLS Gen2 DFS API (data copy staging). Spec §5.9.

Paths are ``{workspace_id}/{item_id}/{Files/...}``; GUIDs avoid name/extension ambiguity.
"""

from __future__ import annotations

import urllib.parse

import httpx

from app.core.config import Settings
from app.core.exceptions import FabricError

__all__ = ["OneLake"]


APPEND_BYTES = 8 * 1024 * 1024


class OneLake:
    def __init__(
        self, token: str, settings: Settings, transport: httpx.BaseTransport | None = None
    ) -> None:
        self.base = settings.ONELAKE_DFS_BASE.rstrip("/")
        self.http = httpx.Client(
            timeout=max(settings.FABRIC_TIMEOUT_S, 300),
            transport=transport,
            headers={"Authorization": f"Bearer {token}", "x-ms-version": "2023-11-03"},
        )

    def close(self) -> None:
        self.http.close()

    def _url(self, ws: str, item: str, path: str) -> str:
        return f"{self.base}/{ws}/{item}/{urllib.parse.quote(path.strip('/'))}"

    def _check(self, r: httpx.Response, what: str, ok: tuple[int, ...] = (200, 201, 202)) -> None:
        if r.status_code not in ok:
            raise FabricError(
                f"OneLake {what} -> {r.status_code}: {r.text[:300]}",
                details={"status": r.status_code},
            )

    def delete(self, ws: str, item: str, path: str) -> None:
        """Delete a file or folder (recursive); missing is fine (idempotent re-runs)."""
        try:
            r = self.http.delete(self._url(ws, item, path), params={"recursive": "true"})
        except httpx.HTTPError as e:
            raise FabricError(f"OneLake delete {path}: {e}") from e
        self._check(r, f"delete {path}", ok=(200, 202, 404))

    def upload(self, ws: str, item: str, path: str, data: bytes) -> None:
        """Create (overwrite), append, flush: the three-step DFS write."""
        url = self._url(ws, item, path)
        try:
            self._check(
                self.http.put(url, params={"resource": "file"}, headers={"Content-Length": "0"}),
                f"create {path}",
            )
            for pos in range(0, len(data), APPEND_BYTES):  # bounded appends for large files
                self._check(
                    self.http.patch(
                        url,
                        params={"action": "append", "position": str(pos)},
                        content=data[pos : pos + APPEND_BYTES],
                    ),
                    f"append {path}",
                )
            self._check(
                self.http.patch(
                    url,
                    params={"action": "flush", "position": str(len(data))},
                    headers={"Content-Length": "0"},
                ),
                f"flush {path}",
            )
        except httpx.HTTPError as e:
            raise FabricError(f"OneLake upload {path}: {e}") from e

    def read(self, ws: str, item: str, path: str) -> bytes:
        try:
            r = self.http.get(self._url(ws, item, path))
        except httpx.HTTPError as e:
            raise FabricError(f"OneLake read {path}: {e}") from e
        self._check(r, f"read {path}", ok=(200,))
        return r.content
