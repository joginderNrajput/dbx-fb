# dbx2fabric migration service

An agentic FastAPI service that migrates Databricks workspaces, or offline notebooks when no
Databricks credentials are shared, to Microsoft Fabric: inventory → plan → convert → preflight →
[deploy] → [copy data] → report. A Claude agent can drive those stages (`mode=agent`). The specification is
`dbx2fabric-migration-service.md`. Conversion behaviour comes from the reference tool
`dbx2fabric_migrate_v4.py` (kept verbatim; §1.2).

## Design principles

| # | Principle | Consequence |
|---|---|---|
| P1 | The reference tool defines conversion behaviour | `tests/unit/conversion/test_parity.py` must stay green; deviations only via ADR 0003 |
| P2 | Credentials are never persisted | job.json stores booleans only; re-deploy needs credentials again; restart fails active jobs |
| P3 | Deterministic first, AI second | the pipeline works without Claude; the agent calls the same session methods and has a safety net |
| P4 | Guards live in code, not prompts | deploy/record tools re-check preconditions; mapping keys are validated |
| P5 | Every job ends in a recorded terminal state | runner `finally`, orphan sweep at startup |

## Rules that override convenience

1. **Never change conversion output without updating the parity test or ADR 0003.** The original
   script is the contract with existing users.
2. **Never write a credential to disk or a log.** Use `SecretStr` in schemas, `repr=False` on
   credential dataclasses, and keep secret-named keys for the redact processor.
3. **Never read a user-controlled path outside the job's `sources/`.** This covers assessment
   `source_path`, dbt folders and zip members.

## Layer map

| Layer | Packages | Role |
|---|---|---|
| interface | `app/api`, `app/schemas`, `app/cli` | HTTP routes, wire contracts, operator CLI |
| orchestration | `app/orchestrator` | job record/repo, in-process runner, MigrationService |
| agent | `app/agents` | LLM clients (Claude / Azure OpenAI; only `llm.py` talks to an LLM), tools, loop, cell fixer |
| domain | `app/migration`, `app/conversion`, `app/sources`, `app/databricks`, `app/fabric` | engine, rules, offline ingest, integrations |
| platform | `app/core`, `app/storage` | settings, errors, logging, metrics, security; job storage |

Direction: `api → orchestrator → agents → migration → conversion/sources/databricks/fabric →
core/storage`. `core` imports nothing else from `app` (tested).

**Dropped from the blueprint, deliberately:** no database or Alembic (`db/`, `migrations/`), no
Redis/queue (`queue/`), no `multi` compose profile, no OpenTelemetry, no `deploy/` manifests.
Jobs run in-process and state is files under `OUTPUT_DIR` (ADR 0001); there is no worker process
to split out (ADR 0005).

## Concurrency / topology

One process, one uvicorn worker. `MAX_CONCURRENT_JOBS` jobs run in threads (`asyncio.to_thread`).
Each job has a `JOB_TIMEOUT_S` budget, observed at stage checkpoints. Within a job everything is
sequential. Claude calls are budgeted by `AGENT_MAX_TURNS` and `AI_MAX_CELLS_PER_JOB`.

## Local mode and dev-only flags

`make dev` (host) and `make up` (Docker) read the same `.env` and write the same `./output`.
Development-only flags, refused when `APP_ENV=production`: `AUTH_DISABLED`,
`ALLOW_ENV_CREDENTIALS` (settings fallbacks and `env://NAME` refs), `ALLOW_PRIVATE_HOSTS`.

## Read this section of the spec before touching this package

| Package | Spec |
|---|---|
| `app/conversion` | §3.1–§3.3 (and run the parity test) |
| `app/migration` | §3.4–§3.12, §4.1 |
| `app/sources` | §5.2–§5.3 |
| `app/databricks` | §5.1 |
| `app/fabric` | §5.4–§5.5 |
| `app/agents` | §6 |
| `app/orchestrator` | §4 |
| `app/api`, `app/schemas` | §5, §4.4 |
| `app/core`, `app/storage` | §7–§10 |

## Commands

| Command | What it does |
|---|---|
| `make install` | venv with runtime + ai + dev extras |
| `make fmt` / `make lint` | ruff format + fixes / check only |
| `make type` | mypy (strict for core, schemas, storage) |
| `make security` | bandit over `app/` |
| `make test-unit` / `make test-integration` / `make test` | unit / API flows / everything except live |
| `make test-e2e` | live tests (E2E_* env vars) |
| `make snapshots` | regenerate golden notebooks, then review the diff |
| `make dev` | API on the host with reload |
| `make cli-run SRC=...` | offline migration without the API |
| `make hash-key` | API key + Argon2id hash |
| `make up` / `make logs` / `make down` | Docker |

## Conventions

- **Port, don't reinvent.** Conversion code mirrors the reference tool line by line, so a
  behaviour diff shows up in the parity test instead of in a customer's notebook.
- **Ruff excludes `tests/fixtures`.** `ruff --fix` once deleted an "unused" import from a fixture
  notebook and silently changed what the tests cover.
- **Job threads write the job record with `only_if_active=True`.** A thread that outlives its
  timeout must not overwrite the terminal state the runner already recorded.
- **Sync endpoints submit jobs through `call_soon_threadsafe`.** They run in the threadpool
  without an event loop, so `asyncio.get_running_loop()` there raises.
- **Notebook asset names are base names.** Pipelines (`@@NB:name@@`) and `%run` resolve notebooks
  by base name. Two notebooks with the same base name are a preflight error, not a silent
  overwrite.
- **Agent tool results are bounded (60 KB) and errors are returned, not raised.** The model
  recovers from a tool error; an exception would end the job.
- **`API_KEY_HASHES` goes in single quotes in `.env`.** Argon2 hashes contain `$`, which compose
  and dotenv expand otherwise.
- **Data copy reads through a SQL warehouse and writes through OneLake.** OneLake tokens are minted
  from the same Fabric service principal with the storage audience. `EXTERNAL_LINKS` downloads
  also need `*.blob.core.windows.net` reachable.
- **Scripted multi-line edits must assert their anchor.** After `ruff format` reflows a block, a
  silent `str.replace` no-op once left a feature half-applied with green tests elsewhere.
- **Credentials only ever travel in request bodies (ADR 0008).** That is why browse calls are
  `POST`: GET bodies are dropped by clients and proxies, and URLs land in access logs. Databricks:
  `client_id` + `client_secret` win and the token is dropped (`build_databricks_credentials`), half
  a service principal is a 422. Fabric: service principal only. The browse lists and
  `discover_selected` share the asset builders in `app/databricks/discovery.py`.
- **Paginators stop on a repeated continuation token.** A fake that kept returning the same token
  once hung the test suite; a misbehaving API would hang a job the same way.
- **The Docker image has no apt layer.** `python:3.12-slim` ships CA certificates and every
  dependency has a wheel. If a new dependency needs a compiler, add `build-essential` to the
  builder stage only.
