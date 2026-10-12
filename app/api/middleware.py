"""Request context (X-Request-ID) and request body size limit. Spec §5."""

from __future__ import annotations

import json
import uuid
from typing import Any

import structlog
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.core.exceptions import PayloadTooLarge
from app.core.logging import bind_request

__all__ = ["BodySizeLimitMiddleware", "RequestContextMiddleware"]


class RequestContextMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = dict(scope.get("headers") or [])
        rid = headers.get(b"x-request-id", b"").decode()[:64] or uuid.uuid4().hex
        structlog.contextvars.clear_contextvars()
        bind_request(rid)

        async def send_with_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                message.setdefault("headers", []).append((b"x-request-id", rid.encode()))
            await send(message)

        await self.app(scope, receive, send_with_id)


class BodySizeLimitMiddleware:
    """413 before reading a body above the limit; also counts chunked bodies without Content-Length."""

    def __init__(self, app: ASGIApp, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = dict(scope.get("headers") or [])
        declared = headers.get(b"content-length")
        if declared and declared.isdigit() and int(declared) > self.max_bytes:
            await self._reject(send)
            return
        seen = 0

        async def limited_receive() -> Message:
            nonlocal seen
            msg = await receive()
            if msg["type"] == "http.request":
                seen += len(msg.get("body", b""))
                if seen > self.max_bytes:
                    # Raised inside request parsing, so the AppError handler renders the 413 body.
                    raise PayloadTooLarge(
                        f"request body exceeds {self.max_bytes // (1024 * 1024)} MB"
                    )
            return msg

        await self.app(scope, limited_receive, send)

    async def _reject(self, send: Send) -> None:
        body: dict[str, Any] = {
            "error": {
                "code": "payload_too_large",
                "message": f"request body exceeds {self.max_bytes // (1024 * 1024)} MB",
                "field": None,
            }
        }
        raw = json.dumps(body).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 413,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(raw)).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": raw})
