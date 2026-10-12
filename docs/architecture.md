# Architecture

## Components

```text
                    ┌──────────────────────────── FastAPI process (1 uvicorn worker) ────────────────────────────┐
 client ──HTTP──▶   │ api/ (routes, auth, errors, size limit)                                                    │
 (curl, UI, CI)     │   │                                                                                        │
                    │   ▼                                                                                        │
                    │ orchestrator/service.py ── validates request, builds in-memory credentials, ingests uploads │
                    │   │  submit(job)                                                                           │
                    │   ▼                                                                                        │
                    │ orchestrator/runner.py ── semaphore(MAX_CONCURRENT_JOBS), JOB_TIMEOUT_S, cancel flags       │
                    │   │  asyncio.to_thread                                                                     │
                    │   ▼                                                                                        │
                    │ migration/session.py  ◀── pipeline mode: fixed stage order                                 │
                    │   │                   ◀── agent mode: agents/runner.py ⇄ Claude (tools.py)                  │
                    │   ├─ sources/ (offline ingest, source lookup) ── databricks/ (discovery, export) ──▶ Databricks REST
                    │   ├─ conversion/ (notebook + job rules)  migration/engine.py (convert_all)               │
                    │   └─ fabric/ (token, REST client, deploy) ───────────────────────────────────────────▶ Fabric REST / Entra ID
                    │ storage/ (LocalStore, keys) ──▶ OUTPUT_DIR/jobs/<id>/                                       │
                    └────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Data flow of one migration

1. **Request.** `POST /migrations` (live) or `/migrations/upload` (offline). The service:
   - validates mapping keys and AI availability;
   - builds `DatabricksCredentials` / `FabricCredentials` in memory;
   - for uploads, extracts files into `jobs/<id>/sources/` with traversal and size guards;
   - writes `job.json` (status `queued`, no secrets) and submits the job → `202`.
2. **Inventory.** Databricks discovery (workspace tree, jobs, UC tables, secret scopes, clusters),
   the uploaded `assessment.json`, or one Notebook asset per uploaded file → `out/assessment.json`.
3. **Plan.** `init_mapping(assessment)` + uploaded mapping + request overrides → `out/mapping.json`,
   then `plan()` → `out/ledger.json`.
4. **Convert.** `convert_all` with a `SourceResolver` (uploads first, then the Databricks export
   API) and an optional AI `CellFixer` → `out/artifacts/**`, `out/ai_tasks/*.md`,
   `out/RUNBOOK.md`, report.
5. **Preflight.** Static checks; counts go on the job record.
6. **Deploy** (optional). Entra ID token → Fabric items (lakehouses, warehouse, SQL DB, notebooks,
   shortcuts, pipelines, schedules); per-item results go into the ledger.
7. **Report.** `post_migration_report.html/.csv`; the summary goes on the job record → `succeeded`.

In agent mode, Claude calls steps 2–6 as tools, reads notebooks with TODO markers and rewrites
those cells. A deterministic safety net completes steps 2–4 if the agent stops early or the
Claude API fails. Step 7 always runs.

## Process topology

There is one process with one worker: jobs run in-process in threads (ADR 0001, ADR 0005). State
is the filesystem under `OUTPUT_DIR`, shared by the host flow and Docker through the `./output`
mount. Scaling out would mean a queue and a shared object store; neither is needed for an
accelerator that runs a handful of migrations at a time.

## Package dependency direction

`api → orchestrator → agents → migration → {conversion, sources, databricks, fabric} → {core, storage}`.
`tests/unit/architecture/test_imports.py` enforces three rules:
- `core` imports no other `app` package;
- only `agents/llm.py` imports `anthropic`;
- `app.main` does not load the Anthropic SDK.
