"""The static migration form (spec §5.11): one HTML page that drives the browse and migration APIs.

Served without auth because the page holds no data: the API key and the Databricks / Fabric
credentials are typed into it and sent with each API call, never stored by the service.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse, RedirectResponse

__all__ = ["router"]

PAGE = Path(__file__).resolve().parent.parent / "static" / "migrate.html"
router = APIRouter(tags=["ui"])


@router.get("/ui", include_in_schema=False)
def migration_form() -> FileResponse:
    return FileResponse(
        PAGE,
        media_type="text/html",
        headers={
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
            "Referrer-Policy": "no-referrer",
        },
    )


@router.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse("/ui")
