"""Job id generation and validation. A validated id is safe to use as a path segment. Spec §7."""

from __future__ import annotations

import re
import uuid

from app.core.exceptions import NotFound

__all__ = ["is_job_id", "new_job_id", "require_job_id"]

_JOB_ID = re.compile(r"^mig_[0-9a-f]{20}$")


def new_job_id() -> str:
    return "mig_" + uuid.uuid4().hex[:20]


def is_job_id(value: str) -> bool:
    return bool(_JOB_ID.match(value))


def require_job_id(value: str) -> str:
    """Reject anything that is not a job id before it reaches a filesystem path."""
    if not is_job_id(value):
        raise NotFound(f"migration {value!r} not found")
    return value
