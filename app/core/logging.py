"""structlog setup, contextvar binding helpers and secret redaction. Spec §10."""

from __future__ import annotations

import logging
import re
import sys
from collections.abc import MutableMapping
from typing import Any

import structlog

from app.core.config import Settings

__all__ = ["bind_job", "bind_request", "configure_logging", "get_logger", "redact"]

_SECRET_KEY = re.compile(r"(token|secret|password|api_key|apikey|authorization|credential)", re.I)


def redact(_: Any, __: str, event: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    """Scrub values whose KEY looks secret. Keyed on the name: never log a secret under another key."""

    def scrub(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: ("***" if _SECRET_KEY.search(str(k)) else scrub(v)) for k, v in obj.items()}
        if isinstance(obj, list | tuple):
            return [scrub(v) for v in obj]
        return obj

    for k in list(event):
        event[k] = "***" if _SECRET_KEY.search(k) else scrub(event[k])
    return event


def configure_logging(settings: Settings) -> None:
    as_json = (
        settings.LOG_JSON if settings.LOG_JSON is not None else settings.APP_ENV != "development"
    )
    level = logging.getLevelName(settings.LOG_LEVEL.upper())
    logging.basicConfig(stream=sys.stdout, level=level, format="%(message)s", force=True)
    renderer: Any = (
        structlog.processors.JSONRenderer() if as_json else structlog.dev.ConsoleRenderer()
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            redact,
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            level if isinstance(level, int) else logging.INFO
        ),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str | None = None) -> Any:
    return structlog.get_logger(name)


def bind_request(request_id: str) -> None:
    structlog.contextvars.bind_contextvars(request_id=request_id)


def bind_job(job_id: str, stage: str | None = None) -> None:
    structlog.contextvars.bind_contextvars(job_id=job_id, **({"stage": stage} if stage else {}))
