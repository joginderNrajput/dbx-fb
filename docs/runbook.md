# Operations runbook

For the per-migration go-live steps, see the `RUNBOOK.md` generated with every migration
(`GET /api/v1/migrations/{id}/runbook`). This page covers operating the service itself.

## Health

| Probe | Meaning | Action when failing |
|---|---|---|
| `GET /healthz` | process alive | restart the container |
| `GET /readyz` 503 | `OUTPUT_DIR` not writable | check the volume, disk space and permissions (uid 10001) |
| `/readyz` `ai: not configured` | informational | set `ANTHROPIC_API_KEY` if agent mode is wanted |

## Metrics (`/metrics`)

| Metric | Watch for |
|---|---|
| `dbx2fabric_jobs_finished_total{status="failed"}` | rising failure rate: read `error.code` on recent jobs |
| `dbx2fabric_job_duration_seconds` | jobs close to `JOB_TIMEOUT_S` |
| `dbx2fabric_fabric_requests_total{status="429"}` | Fabric throttling: lower `MAX_CONCURRENT_JOBS` |
| `dbx2fabric_ai_calls_total{status!="end_turn"...}` | Claude errors / refusals / rate limits |
| `dbx2fabric_item_outcomes_total` | share of `partial` / `not_migrated` items |

## Job error codes

| `error.code` | Meaning | Recovery |
|---|---|---|
| `validation_failed` | bad input or preflight errors before deploy | fix the mapping / request; `force` only if accepted |
| `databricks_error` / `fabric_error` | upstream API failure (auth, permissions, 5xx) | test credentials via `/connections/*/test`; retry |
| `ai_unavailable` | Claude unreachable at submit | check key, network, `ai` extra |
| `job_timeout` | exceeded `JOB_TIMEOUT_S` | narrow `workspace_paths` / `catalogs`, or raise the budget |
| `cancelled` | cancelled by request or shutdown | resubmit |
| `interrupted` | the process restarted mid-job | resubmit (credentials are not persisted) |
| `internal_error` | bug | logs carry `job_id`; open an issue with the job's `out/` folder |

## Common operations

- **Rotate API keys.** Add the new hash to `API_KEY_HASHES` (whitespace-separated, single-quoted),
  restart, move clients over, then remove the old hash.
- **Free disk.** `DELETE /api/v1/migrations/{id}` for finished jobs, or delete
  `OUTPUT_DIR/jobs/<id>` while no job is running.
- **Logs.** JSON outside development. Every line of a job carries `job_id` and `stage`, every
  request `request_id` (also returned as `X-Request-ID`). Secret-named keys are redacted.
- **Upgrade.** Rebuild the image and restart. Running jobs are marked `interrupted` and must be
  resubmitted, so drain first: check that `GET /api/v1/migrations` shows no `running` jobs.
