"""Bounded in-process job runner: concurrency, wall-clock budget, cancellation. Spec §4.3.

Jobs run the synchronous engine in a worker thread (``asyncio.to_thread``). A thread cannot be
killed, so cancellation and timeouts set a flag that the job observes at its next stage
checkpoint; the job record reaches its terminal state immediately either way (S11).
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from typing import Any

from app.core.config import Settings
from app.core.exceptions import AppError, Cancelled
from app.core.logging import bind_job, get_logger
from app.core.metrics import JOB_DURATION, JOBS_FINISHED
from app.orchestrator.jobs import JobRepository, utcnow

__all__ = ["JobControl", "JobRunner"]

log = get_logger(__name__)


class JobControl:
    """Handed to the job thread: stage checkpoints double as cancellation points."""

    def __init__(self, job_id: str, repo: JobRepository) -> None:
        self.job_id = job_id
        self.repo = repo
        self.cancelled = threading.Event()
        self.reason = "cancelled"

    def cancel(self, reason: str = "cancelled") -> None:
        self.reason = reason
        self.cancelled.set()

    def checkpoint(self, stage: str) -> None:
        if self.cancelled.is_set():
            raise Cancelled(f"job stopped before stage '{stage}': {self.reason}")
        bind_job(self.job_id, stage)
        self.repo.update(self.job_id, only_if_active=True, stage=stage)


class JobRunner:
    def __init__(self, repo: JobRepository, settings: Settings) -> None:
        self.repo = repo
        self.settings = settings
        # Created inside the lifespan, so this is the server's event loop. Sync endpoints run in a
        # threadpool without a loop and hand jobs over with call_soon_threadsafe.
        self._loop = asyncio.get_running_loop()
        self._sem = asyncio.Semaphore(settings.MAX_CONCURRENT_JOBS)
        self._controls: dict[str, JobControl] = {}
        self._tasks: set[asyncio.Task[None]] = set()

    def is_active(self, job_id: str) -> bool:
        return job_id in self._controls

    def submit(self, job_id: str, work: Callable[[JobControl], Any]) -> None:
        control = JobControl(job_id, self.repo)
        self._controls[job_id] = control
        self._loop.call_soon_threadsafe(self._spawn, control, work)

    def _spawn(self, control: JobControl, work: Callable[[JobControl], Any]) -> None:
        task = self._loop.create_task(self._run(control, work))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def cancel(self, job_id: str) -> bool:
        c = self._controls.get(job_id)
        if not c:
            return False
        c.cancel("cancelled by request")
        return True

    async def _run(self, control: JobControl, work: Callable[[JobControl], Any]) -> None:
        job_id = control.job_id
        status, error = "failed", None
        t0 = time.monotonic()
        try:
            async with self._sem:
                if control.cancelled.is_set():
                    raise Cancelled("cancelled before start")
                self.repo.update(
                    job_id, status="running", started_at=utcnow(), finished_at=None, error=None
                )
                log.info("job_started", job_id=job_id)
                await asyncio.wait_for(
                    asyncio.to_thread(work, control), timeout=self.settings.JOB_TIMEOUT_S
                )
                status = "succeeded"
        except TimeoutError:
            control.cancel("job timeout")
            error = {
                "code": "job_timeout",
                "message": f"job exceeded JOB_TIMEOUT_S={self.settings.JOB_TIMEOUT_S}s",
            }
        except Cancelled as e:
            status, error = "cancelled", {"code": e.code, "message": e.message}
        except AppError as e:
            error = {"code": e.code, "message": e.message}
        except asyncio.CancelledError:
            control.cancel("service shutdown")
            error = {
                "code": "interrupted",
                "message": "service shut down while the job was running",
            }
            raise
        except Exception as e:  # S11: every job ends in a recorded terminal state
            log.exception("job_crashed", job_id=job_id)
            error = {"code": "internal_error", "message": f"{type(e).__name__}: {e}"}
        finally:
            self._controls.pop(job_id, None)
            try:
                self.repo.update(
                    job_id, status=status, finished_at=utcnow(), error=error, stage=None
                )
            except Exception:
                log.exception("job_record_write_failed", job_id=job_id)
            JOBS_FINISHED.labels(status).inc()
            JOB_DURATION.observe(time.monotonic() - t0)
            log.info("job_finished", job_id=job_id, status=status, error=error)

    async def shutdown(self, grace_s: float = 5.0) -> None:
        for c in self._controls.values():
            c.cancel("service shutdown")
        if self._tasks:
            await asyncio.wait(self._tasks, timeout=grace_s)
