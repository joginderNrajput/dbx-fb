"""FastAPI factory + lifespan only. Spec §5."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.agents.llm import LLMClient, build_llm
from app.api import ui
from app.api.errors import install_error_handlers
from app.api.middleware import BodySizeLimitMiddleware, RequestContextMiddleware
from app.api.v1 import health
from app.api.v1.router import api_router
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging, get_logger
from app.orchestrator.jobs import JobRepository
from app.orchestrator.runner import JobRunner
from app.orchestrator.service import MigrationService
from app.storage.factory import build_store

__all__ = ["app", "create_app"]


def create_app(
    settings: Settings | None = None, *, llm_factory: Callable[[Settings], LLMClient] = build_llm
) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging(settings)
        log = get_logger("app")
        store = build_store(settings)  # fails fast if OUTPUT_DIR is not writable
        repo = JobRepository(store)
        orphans = repo.fail_orphans()
        runner = JobRunner(repo, settings)
        app.state.settings, app.state.store, app.state.repo = settings, store, repo
        app.state.service = MigrationService(settings, store, repo, runner, llm_factory=llm_factory)
        log.info(
            "service_started",
            env=settings.APP_ENV,
            output_dir=str(settings.OUTPUT_DIR),
            ai=settings.ai_configured,
            auth_disabled=settings.AUTH_DISABLED,
            orphaned_jobs_failed=orphans,
        )
        try:
            yield
        finally:
            await runner.shutdown()
            log.info("service_stopped")

    app = FastAPI(
        title="Databricks to Fabric Migration Service",
        version=settings.APP_VERSION,
        lifespan=lifespan,
        description="Agentic FastAPI service that migrates Databricks workspaces and offline notebooks to Microsoft Fabric.",
    )
    app.add_middleware(BodySizeLimitMiddleware, max_bytes=settings.MAX_UPLOAD_MB * 1024 * 1024)
    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    app.add_middleware(RequestContextMiddleware)
    install_error_handlers(app)
    app.include_router(health.router)
    app.include_router(api_router)
    app.include_router(ui.router)
    return app


app = create_app()
