"""One error body for every failure: {"error": {code, message, field}}. Spec §4.4."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.core.exceptions import AppError
from app.core.logging import get_logger

__all__ = ["install_error_handlers"]

log = get_logger(__name__)


def _body(code: str, message: str, field: str | None = None) -> dict[str, dict[str, str | None]]:
    return {"error": {"code": code, "message": message, "field": field}}


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def app_error(_: Request, exc: AppError) -> JSONResponse:
        if exc.http_status >= 500:
            log.warning("request_failed", code=exc.code, message=exc.message)
        return JSONResponse(_body(exc.code, exc.message, exc.field), status_code=exc.http_status)

    @app.exception_handler(RequestValidationError)
    async def validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        errs = exc.errors()
        first = errs[0] if errs else {}
        loc = ".".join(
            str(x) for x in first.get("loc", []) if x not in ("body", "query", "path", "form")
        )
        return JSONResponse(
            _body("bad_request", str(first.get("msg", "invalid request")), loc or None),
            status_code=400,
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        return JSONResponse(_body("http_error", str(exc.detail)), status_code=exc.status_code)

    @app.exception_handler(Exception)
    async def unhandled(_: Request, exc: Exception) -> JSONResponse:
        log.exception("unhandled_error")
        return JSONResponse(_body("internal_error", "internal server error"), status_code=500)
