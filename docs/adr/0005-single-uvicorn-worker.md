# ADR 0005 — One uvicorn worker per container

Status: accepted

Context: jobs run in-process (ADR 0001). Cancellation flags, the concurrency semaphore and the
runner's task registry live in process memory.

Decision: run uvicorn with `--workers 1`. With several workers, a cancel request could land on a
worker that does not own the job, and the concurrency limit would multiply by the worker count.

Consequences: request throughput is that of one async worker, which is ample: request handlers
are thin, and the heavy work runs in job threads. Scale by running more containers on separate
`OUTPUT_DIR`s, or introduce a queue (revisit ADR 0001).
