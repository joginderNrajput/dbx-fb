# ADR 0001 — In-process job runner and filesystem job state (no database, no queue)

Status: accepted

Context: migrations are long-running (minutes), so the API returns 202 and runs them in the
background (spec §4.3). The blueprint's full shape adds PostgreSQL, Redis/arq and separate
workers. This accelerator runs a handful of migrations at a time. Its state is mostly files the
engine already writes: ledger, artifacts, reports.

Decision: run jobs in the API process (`asyncio.to_thread` behind a semaphore). Keep the job
record as `jobs/<id>/job.json` next to the job's files, through the `ObjectStore` protocol with a
local backend. Explicitly not chosen: a database, a broker, separate worker processes.

Consequences: one-command setup with or without Docker, and nothing else to operate. A restart
loses running jobs; they are marked `interrupted` at startup, which is acceptable because
credentials are never persisted, so they could not resume anyway. Horizontal scaling would need
a queue plus a shared store. Revisit if concurrent migrations or multi-instance deployment become
requirements.
