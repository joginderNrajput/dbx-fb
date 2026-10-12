"""Aggregates the v1 routers under /api/v1 (auth applied here). Spec §5."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from app.api.deps import require_api_key
from app.api.v1 import connections, convert, migrations

__all__ = ["api_router"]

api_router = APIRouter(prefix="/api/v1", dependencies=[Depends(require_api_key)])
api_router.include_router(connections.router)
api_router.include_router(migrations.router)
api_router.include_router(convert.router)
