"""The ONLY place storage keys are built. Spec §7.

Layout per job::

    jobs/<job_id>/job.json            job record (no credentials, ever)
    jobs/<job_id>/agent_trace.json    agent steps (mode=agent)
    jobs/<job_id>/sources/            uploaded notebooks / extracted zips / dbt projects
    jobs/<job_id>/out/                assessment.json, mapping.json, ledger.json, artifacts/, reports
"""

from __future__ import annotations

from app.core.ids import require_job_id

__all__ = ["JOBS_PREFIX", "agent_trace", "job_out", "job_prefix", "job_record", "job_sources"]

JOBS_PREFIX = "jobs"


def job_prefix(job_id: str) -> str:
    return f"{JOBS_PREFIX}/{require_job_id(job_id)}"


def job_record(job_id: str) -> str:
    return f"{job_prefix(job_id)}/job.json"


def agent_trace(job_id: str) -> str:
    return f"{job_prefix(job_id)}/agent_trace.json"


def job_sources(job_id: str) -> str:
    return f"{job_prefix(job_id)}/sources"


def job_out(job_id: str) -> str:
    return f"{job_prefix(job_id)}/out"
