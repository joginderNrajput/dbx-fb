from __future__ import annotations

import asyncio
import time
from pathlib import Path

from app.core.exceptions import ValidationFailed
from app.orchestrator.jobs import JobRecord, JobRepository
from app.orchestrator.runner import JobControl, JobRunner
from app.storage.local import LocalStore
from tests.conftest import make_settings

JID = "mig_0123456789abcdef0123"


def repo(tmp_path: Path) -> JobRepository:
    r = JobRepository(LocalStore(tmp_path / "out"))
    r.create(JobRecord(id=JID, source_kind="upload"))
    return r


async def run(tmp_path: Path, work, **settings_over) -> JobRecord:  # type: ignore[no-untyped-def]
    r = repo(tmp_path)
    runner = JobRunner(r, make_settings(tmp_path, **settings_over))
    runner.submit(JID, work)
    await asyncio.sleep(0.05)
    while r.get(JID).status in ("queued", "running"):
        await asyncio.sleep(0.02)
    await runner.shutdown()
    return r.get(JID)


async def test_success_failure_and_terminal_state(tmp_path: Path) -> None:
    assert (await run(tmp_path / "a", lambda c: None)).status == "succeeded"

    def bad(c: JobControl) -> None:
        raise ValidationFailed("nope")

    rec = await run(tmp_path / "b", bad)
    assert rec.status == "failed" and rec.error == {"code": "validation_failed", "message": "nope"}

    def crash(c: JobControl) -> None:
        raise RuntimeError("boom")

    assert (await run(tmp_path / "c", crash)).error["code"] == "internal_error"  # type: ignore[index]


async def test_timeout_stops_job_at_next_checkpoint(tmp_path: Path) -> None:
    seen: list[str] = []

    def slow(c: JobControl) -> None:
        for i in range(40):
            c.checkpoint(f"s{i}")
            seen.append(f"s{i}")
            time.sleep(0.05)

    rec = await run(tmp_path, slow, JOB_TIMEOUT_S=1)
    assert rec.status == "failed" and rec.error["code"] == "job_timeout"  # type: ignore[index]
    time.sleep(0.2)
    assert len(seen) < 40  # the thread observed the cancel flag


async def test_cancel(tmp_path: Path) -> None:
    r = repo(tmp_path)
    runner = JobRunner(r, make_settings(tmp_path))

    def loop(c: JobControl) -> None:
        for i in range(100):
            c.checkpoint(str(i))
            time.sleep(0.02)

    runner.submit(JID, loop)
    await asyncio.sleep(0.1)
    assert runner.cancel(JID)
    while r.get(JID).status in ("queued", "running"):
        await asyncio.sleep(0.02)
    assert r.get(JID).status == "cancelled"
    await runner.shutdown()


def test_orphans_failed_on_startup(tmp_path: Path) -> None:
    r = repo(tmp_path)
    r.update(JID, status="running")
    assert r.fail_orphans() == 1
    assert r.get(JID).error["code"] == "interrupted"  # type: ignore[index]
    assert r.update(JID, only_if_active=True, stage="x") is None
