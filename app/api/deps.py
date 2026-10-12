"""FastAPI dependencies: settings, API-key auth, service objects from app.state. Spec §8.1."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Header, Request

from app.core.config import Settings
from app.core.exceptions import Unauthorized
from app.core.security import verify_api_key
from app.orchestrator.jobs import JobRepository
from app.orchestrator.service import MigrationService
from app.storage.base import ObjectStore

__all__ = ["get_repo", "get_service", "get_settings_dep", "get_store", "require_api_key"]


def get_settings_dep(request: Request) -> Settings:
    settings: Settings = request.app.state.settings
    return settings


def get_service(request: Request) -> MigrationService:
    service: MigrationService = request.app.state.service
    return service


def get_repo(request: Request) -> JobRepository:
    repo: JobRepository = request.app.state.repo
    return repo


def get_store(request: Request) -> ObjectStore:
    store: ObjectStore = request.app.state.store
    return store


def require_api_key(
    settings: Annotated[Settings, Depends(get_settings_dep)],
    x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> None:
    if settings.AUTH_DISABLED:
        return
    if not x_api_key or not verify_api_key(x_api_key, settings.api_key_hashes):
        raise Unauthorized("missing or invalid X-API-Key header")
