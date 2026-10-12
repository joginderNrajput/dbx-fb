"""Liveness, readiness and Prometheus metrics. Spec §5.7."""

from __future__ import annotations

import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.api.deps import get_settings_dep, get_store
from app.core.config import Settings
from app.storage.base import ObjectStore

router = APIRouter(tags=["health"])


@router.get("/healthz")
def healthz() -> dict[str, str]:
    """Liveness: the process answers. No dependency checks."""
    return {"status": "ok"}


@router.get("/readyz")
def readyz(
    settings: Annotated[Settings, Depends(get_settings_dep)],
    store: Annotated[ObjectStore, Depends(get_store)],
) -> Response:
    """Readiness: the output directory is writable. AI availability is reported, not required."""
    checks: dict[str, Any] = {}
    ok = True
    probe = f".readyz-{uuid.uuid4().hex}"
    try:
        store.write_bytes(probe, b"ok")
        store.delete_prefix(probe)
        store.path(probe).unlink(missing_ok=True)
        checks["storage"] = "ok"
    except Exception as e:  # report, never raise, from a probe
        ok = False
        checks["storage"] = f"error: {e}"
    checks["ai"] = (
        "configured"
        if settings.ai_configured
        else "not configured (agent mode and ai_fix disabled)"
    )
    return JSONResponse(
        {
            "status": "ok" if ok else "unavailable",
            "checks": checks,
            "version": settings.APP_VERSION,
        },
        status_code=200 if ok else 503,
    )


@router.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
