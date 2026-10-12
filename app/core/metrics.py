"""Prometheus metrics, defined once. Spec §10."""

from __future__ import annotations

from prometheus_client import Counter, Histogram

__all__ = [
    "AI_CALLS",
    "FABRIC_REQUESTS",
    "ITEM_OUTCOMES",
    "JOB_DURATION",
    "JOBS_FINISHED",
    "JOBS_STARTED",
]

JOBS_STARTED = Counter(
    "dbx2fabric_jobs_started_total", "Migration jobs started", ["mode", "source"]
)
JOBS_FINISHED = Counter("dbx2fabric_jobs_finished_total", "Migration jobs finished", ["status"])
JOB_DURATION = Histogram(
    "dbx2fabric_job_duration_seconds",
    "Wall-clock duration of migration jobs",
    buckets=(1, 5, 15, 60, 300, 900, 1800, 3600, 7200),
)
ITEM_OUTCOMES = Counter(
    "dbx2fabric_item_outcomes_total", "Ledger item outcomes at job end", ["outcome"]
)
FABRIC_REQUESTS = Counter(
    "dbx2fabric_fabric_requests_total", "Fabric REST calls", ["method", "status"]
)
AI_CALLS = Counter("dbx2fabric_ai_calls_total", "Claude API calls", ["kind", "status"])
