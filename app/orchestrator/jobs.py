"""Job record and its repository (job.json per migration). Spec §4.2."""

from __future__ import annotations

import threading
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field

from app.core.exceptions import NotFound
from app.storage import keys
from app.storage.base import ObjectStore

__all__ = ["ACTIVE", "TERMINAL", "JobRecord", "JobRepository", "utcnow"]

ACTIVE = ("queued", "running")
TERMINAL = ("succeeded", "failed", "cancelled")


def utcnow() -> datetime:
    return datetime.now(UTC)


class JobRecord(BaseModel):
    """Persisted job state. Holds NO credentials: only whether they were supplied."""

    id: str
    status: str = "queued"
    mode: str = "pipeline"
    source_kind: str
    stage: str | None = None
    created_at: datetime = Field(default_factory=utcnow)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    options: dict[str, Any] = Field(default_factory=dict)
    inventory: dict[str, Any] | None = None
    summary: dict[str, Any] | None = None
    preflight: dict[str, int] | None = None
    upload: dict[str, Any] | None = None
    agent: dict[str, Any] | None = None
    error: dict[str, str] | None = None


class JobRepository:
    def __init__(self, store: ObjectStore) -> None:
        self.store = store
        self._lock = threading.Lock()  # API thread and job threads both write job.json

    def create(self, rec: JobRecord) -> JobRecord:
        with self._lock:
            self.store.write_json(keys.job_record(rec.id), rec.model_dump(mode="json"))
        return rec

    def get(self, job_id: str) -> JobRecord:
        key = keys.job_record(job_id)
        if not self.store.exists(key):
            raise NotFound(f"migration {job_id!r} not found")
        return JobRecord.model_validate(self.store.read_json(key))

    def update(
        self, job_id: str, *, only_if_active: bool = False, **fields: Any
    ) -> JobRecord | None:
        """Atomic read-modify-write. ``only_if_active`` drops late progress writes from a job thread
        that outlived its timeout, so a terminal state is never overwritten."""
        with self._lock:
            rec = self.get(job_id)
            if only_if_active and rec.status not in ACTIVE:
                return None
            data = rec.model_dump()
            data.update(fields)
            rec = JobRecord.model_validate(data)
            self.store.write_json(keys.job_record(job_id), rec.model_dump(mode="json"))
            return rec

    def list(self, limit: int = 100) -> list[JobRecord]:
        out = []
        for jid in self.store.list_prefixes(keys.JOBS_PREFIX):
            try:
                out.append(self.get(jid))
            except (NotFound, ValueError):
                continue
        out.sort(key=lambda r: r.created_at, reverse=True)
        return out[:limit]

    def delete(self, job_id: str) -> None:
        self.store.delete_prefix(keys.job_prefix(job_id))

    def fail_orphans(self) -> int:
        """Jobs left queued/running by a previous process can never finish: record that (spec §4.3)."""
        n = 0
        for rec in self.list(limit=100000):
            if rec.status in ACTIVE:
                self.update(
                    rec.id,
                    status="failed",
                    finished_at=utcnow(),
                    error={
                        "code": "interrupted",
                        "message": "the service restarted while this job was active; credentials are not persisted, so start a new migration",
                    },
                )
                n += 1
        return n
