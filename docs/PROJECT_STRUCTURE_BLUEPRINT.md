# Project Structure Blueprint — Python Service Accelerators

A reusable, project-agnostic standard for how an accelerator is laid out, built, configured,
containerised, tested and documented. It is distilled from a production service (an async
FastAPI API + background workers + PostgreSQL + Redis + object storage), but nothing below is
specific to that service's domain.

---

## 0. How to use this document (instructions for Claude Code)

Hand this file to Claude Code at the start of a new or existing accelerator, for example:

> "Follow `docs/PROJECT_STRUCTURE_BLUEPRINT.md`. Scaffold this repository to match it, adapting
> the domain packages to what this project does."

When you (Claude Code) apply it:

1. **Read the whole document first.** Section 13 is the scaffolding checklist; sections 1–12 are
   the rules behind each step.
2. **Substitute the placeholders** everywhere:
   | Placeholder | Meaning | Example |
   |---|---|---|
   | `<project>` | distribution / compose project name, kebab-case | `flip-invoice-extractor` |
   | `<pkg>` | the importable top-level package — **always `app`** unless the repo already uses another name | `app` |
   | `<svc>` | short name for images, users, volumes | `flip` |
   | `<domain_*>` | the project's own business packages (section 2.2) | `parsing`, `routing`, `extraction` |
   | `<spec>.md` | the authoritative specification at the repo root | `invoice-extractor-service.md` |
3. **Keep the invariants, adapt the rest.** Section 12 lists what must not change between
   projects (layout, tooling, config rules, safety rules) and what is expected to change
   (domain packages, optional services, provider extras).
4. **Drop what the project does not need, explicitly.** No queue? Remove `queue/`, the worker
   services and Redis — and say so in `CLAUDE.md`. Do not leave empty placeholder packages.
5. **On an existing repository, migrate incrementally**, one area per change (tooling →
   config → layout → Docker → tests), keeping the test suite green after each step. Never
   rename packages and change behaviour in the same change.
6. **Never invent infrastructure the project has not asked for.** This blueprint describes the
   full shape; a small accelerator legitimately uses a subset.

---

## 1. Principles behind the structure

These are why the layout looks the way it does. Each one is enforced by a file, a test or a
Make target — not only by convention.

| # | Principle | Where it is enforced |
|---|---|---|
| S1 | **Spec first.** One authoritative spec at the repo root; code cites its sections (`§4.1`). | `<spec>.md`, docstrings, `CLAUDE.md` |
| S2 | **One standing brief for agents and humans.** `CLAUDE.md` says what the service is, the rules that override convenience, and the commands. | `CLAUDE.md` |
| S3 | **Layered packages with a one-way dependency direction.** Platform code never imports domain code. | layer map in `CLAUDE.md`, import-isolation test |
| S4 | **All configuration through one typed `Settings`.** No `os.environ` in application modules. | `app/core/config.py`, review |
| S5 | **Reproducible dependencies.** `uv` + committed `uv.lock`; a new dependency needs an ADR. | `pyproject.toml`, `uv.lock`, `docs/adr/` |
| S6 | **Every task has a Make target.** If CI, a README or a teammate runs it, `make` names it. | `Makefile` |
| S7 | **Dev parity.** One-process local mode and the Docker stack read the same `.env` and the same database. | `make dev`, `docker-compose.yml` |
| S8 | **Dev-only conveniences are refused in production** by `Settings` validators, not by documentation. | `app/core/config.py` |
| S9 | **Secrets are never committed, logged or persisted.** References (`vault://…`) travel; values are resolved in-process. | `.gitignore`, log redaction, `app/core/secrets.py` |
| S10 | **Bounded everything.** Timeouts, retries, budgets, concurrency are explicit settings. | `Settings` |
| S11 | **Failures are reported, never silent.** A background job always ends in a recorded terminal state. | orchestration `finally` blocks, tests |
| S12 | **Decisions are recorded.** Non-obvious choices get a numbered ADR. | `docs/adr/NNNN-*.md` |

---

## 2. Repository layout

### 2.1 Top level

```text
<repo>/
├── <spec>.md                 # authoritative specification (S1); code cites §sections
├── CLAUDE.md                 # standing brief for agents & engineers (S2) — see §10.2
├── README.md                 # what it is + 5-command quickstart + doc links
├── pyproject.toml            # deps, extras, tool config (ruff, mypy, pytest, bandit)
├── uv.lock                   # COMMITTED — the resolved graph every machine & CI uses
├── Makefile                  # every routine task, self-documenting `make help`
├── Dockerfile                # multi-stage: base → builder → runtime (non-root)
├── .dockerignore
├── docker-compose.yml        # default single-process stack + `multi` / `tools` profiles
├── .env.example              # COMMITTED template, sectioned like Settings
├── .env                      # NOT committed — real values
├── .gitignore
├── alembic.ini               # only if the project has a relational database
├── azure-pipelines.yml       # or .github/workflows/*.yml — CI/CD
├── app/                      # the one importable package (section 2.2)
├── migrations/               # Alembic: env.py, script.py.mako, versions/NNNN_*.py
├── tests/                    # unit / integration / e2e / evals / fixtures (section 9)
├── docs/                     # architecture, runbook, guides, ADRs (section 10)
└── deploy/                   # k8s / terraform / observability / helper services (section 11)
```

Rules:

- **One importable package, `app/`.** No `src/` layout, no second top-level package. Tests
  import `app.*`; tooling lists `app` and `tests` as first-party.
- **Root holds only project-level files.** Scratch data, sample inputs, generated output and
  zips live in gitignored folders (`output/`, `samples/`), never in tracked paths.
- **Generated output has one home**, configurable through a setting (e.g. `ARTIFACT_OUTPUT_DIR`,
  default empty = disabled) and gitignored.

### 2.2 The application package

Platform packages are the same in every accelerator. Domain packages are the project's own.

```text
app/
├── __init__.py
├── main.py                # FastAPI factory + lifespan ONLY (section 7.1)
│
│  ── interface layer ──────────────────────────────────────────────
├── api/
│   ├── deps.py            # FastAPI dependencies (auth, session, settings)
│   ├── errors.py          # domain exception → HTTP error body mapping
│   ├── middleware.py      # request context, body-size limit
│   └── v1/
│       ├── router.py      # aggregates the v1 routers
│       ├── health.py      # /healthz, /readyz (and /metrics if exposed here)
│       └── <resource>.py  # one module per resource
├── schemas/               # pydantic request/response models (wire contracts)
├── cli/
│   └── main.py            # Typer operator CLI (also the [project.scripts] entry)
│
│  ── orchestration layer (only if there is background work) ───────
├── queue/
│   ├── broker.py          # the ONLY module that imports the queue library
│   ├── tasks.py           # worker classes / task entrypoints
│   ├── heartbeat.py, watchdog.py, redis_keys.py
│   └── embedded.py        # dev-only: run workers inside the API process
├── orchestrator/          # job state machine, stage runner, dependency wiring
│
│  ── domain layer (project-specific — rename freely) ─────────────
├── <domain_a>/            # e.g. parsing/  — deterministic, strictly typed
├── <domain_b>/            # e.g. routing/, agents/, emitters/, validation/
├── <integration_x>/       # e.g. databricks/ — one package per external system
│
│  ── platform layer (same in every project) ───────────────────────
├── core/
│   ├── config.py          # Settings (pydantic-settings) + get_settings()
│   ├── exceptions.py      # base domain error with `code` + `http_status`
│   ├── logging.py         # structlog setup, bind_* helpers, redact processor
│   ├── telemetry.py       # OpenTelemetry setup/shutdown
│   ├── metrics.py         # Prometheus metrics, defined once
│   ├── security.py        # API-key hashing (Argon2id), URL guards
│   ├── secrets.py         # secret-reference resolution (vault://…), TTL cache
│   └── ids.py             # id generation/validation
├── db/
│   ├── session.py         # one async engine per process, sessionmaker
│   ├── models.py          # SQLAlchemy models
│   └── repositories/      # one repository per aggregate; tenant-scoped reads
└── storage/
    ├── base.py            # object-store protocol
    ├── local.py, s3.py, azure_blob.py
    ├── factory.py         # backend chosen by a setting
    └── keys.py            # the ONLY place object keys are built
```

**Dependency direction (S3):** `api → schemas/orchestrator → domain → core/db/storage`.
`core/` imports nothing from `app` except other `core` modules. The API process must not import
heavy worker-only packages; enforce it with an import-isolation test (section 9.4).

**Package conventions**

- Every module starts with a docstring stating its responsibility and the spec section it
  implements (`"""Async engine and sessionmaker. Spec §13.1."""`).
- Public names are listed in `__all__`.
- One library is wrapped by one module (the queue library only in `queue/broker.py`, the LLM
  SDK only in one client module, object keys only in `storage/keys.py`). A test greps for
  violations.
- Domain exceptions subclass the base in `core/exceptions.py` and carry a stable `code` and an
  `http_status`; the API maps them in one place.

---

## 3. Dependency management

**Tooling:** [`uv`](https://docs.astral.sh/uv/) for environments and locking, `hatchling` as the
build backend, Python `>=3.12`.

### 3.1 `pyproject.toml` template

```toml
[project]
name = "<project>"
version = "1.0.0"
requires-python = ">=3.12"

# Grouped by concern, each group a comment; a one-line reason on anything non-obvious.
dependencies = [
  # --- web ---
  "fastapi>=0.115",
  "uvicorn[standard]>=0.32",
  "python-multipart>=0.0.12",   # multipart uploads (drop if unused)
  "httpx>=0.28",                # ALL outbound HTTP
  # --- data contracts ---
  "pydantic>=2.9",
  "pydantic-settings>=2.6",
  # --- persistence (if a database) ---
  "sqlalchemy[asyncio]>=2.0",
  "asyncpg>=0.30",
  "alembic>=1.14",
  # --- queue (if background work) ---
  "arq>=0.26",
  "redis>=5.2",
  # --- observability ---
  "structlog>=24.4",
  "opentelemetry-sdk>=1.28",
  "opentelemetry-instrumentation-fastapi>=0.49",
  "opentelemetry-instrumentation-httpx>=0.49",
  "prometheus-client>=0.21",
  # --- security ---
  "argon2-cffi>=23.1",          # API-key hashing; see docs/adr/NNNN
  # --- cli / misc ---
  "typer>=0.15",
  "tenacity>=9.0",
  # --- <domain> ---
]

[project.optional-dependencies]
# One extra per interchangeable provider; the adapter imports its SDK lazily, so a
# deployment installs only what it configured (record the choice in an ADR).
# provider-a = ["sdk-a>=1.0"]
dev = [
  "pytest>=8.3", "pytest-asyncio>=0.24", "pytest-cov>=6.0",
  "respx>=0.22",                          # httpx mocking
  "testcontainers[postgres,redis]>=4.9",  # real services in integration tests
  "mypy>=1.13", "ruff>=0.8",
]

[project.scripts]
<svc> = "app.cli.main:app"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["app"]
```

### 3.2 Rules

- **`uv.lock` is committed.** `.gitignore` must say so explicitly, so nobody "cleans it up".
- **Install with `make install` (`uv sync --extra dev`).** Run everything through `uv run`, so the
  project venv is always the one in use.
- **Docker installs with `uv sync --frozen --no-dev`.** `--frozen` fails the build if the lock is
  stale, instead of silently resolving something new.
- **A new runtime dependency needs an ADR** (`docs/adr/NNNN-<name>.md`), unless the spec already
  lists it. State why the stdlib and the existing dependencies are not enough.
- **Provider SDKs are extras, imported lazily** inside the adapter that needs them. Missing an
  extra should fail clearly at first use, never at import of an unrelated module.
- **Lower bounds only (`>=`) in `pyproject.toml`.** Exact versions live in `uv.lock`.

### 3.3 Tool configuration (in `pyproject.toml`)

```toml
[tool.ruff]
line-length = 100
target-version = "py312"
src = ["app", "tests"]
extend-exclude = ["tests/fixtures/expected_*"]   # golden output is compared byte-for-byte

[tool.ruff.lint]
select = ["E", "F", "W", "I", "UP", "B", "ASYNC"]

[tool.ruff.lint.isort]
known-first-party = ["app", "tests"]

[tool.ruff.lint.per-file-ignores]
"tests/**" = ["B011"]

[tool.mypy]
python_version = "3.12"
plugins = ["pydantic.mypy"]
ignore_missing_imports = true
warn_unused_configs = true
warn_redundant_casts = true

# Strict where correctness is load-bearing and the code is deterministic.
[[tool.mypy.overrides]]
module = ["app.core.*", "app.schemas.*"]   # + deterministic domain packages
disallow_untyped_defs = true
disallow_incomplete_defs = true
disallow_untyped_calls = true
disallow_untyped_decorators = true
disallow_any_generics = true
disallow_subclassing_any = true
check_untyped_defs = true
no_implicit_reexport = true
warn_return_any = true
warn_unused_ignores = true
strict_equality = true
extra_checks = true

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
markers = [
  "slow: requires live credentials or a real external system",
  "integration: needs postgres/redis containers",
]

[tool.bandit]
skips = ["B101"]   # assert guarding internal invariants; every other finding is a gate
```

---

## 4. Configuration

### 4.1 One `Settings` class

`app/core/config.py` holds a single `pydantic-settings` class, grouped by commented sections,
and a cached accessor:

```python
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=True)

    # ── service ────────────────────────────────────────────────
    APP_ENV: Literal["development", "staging", "production"] = "development"
    APP_VERSION: str = "1.0.0"
    LOG_LEVEL: str = "INFO"

    # ── single-process local mode (refused in production) ──────
    RUN_EMBEDDED_WORKERS: bool = False

    # ── datastores ─────────────────────────────────────────────
    DATABASE_URL: str
    DATABASE_SCHEMA: str = ""            # drives search_path AND alembic version table
    REDIS_URL: str = "redis://localhost:6379/0"
    OBJECT_STORE_BACKEND: Literal["local", "s3", "azure_blob"] = "local"

    # ── concurrency / budgets (S10) ────────────────────────────
    WORKER_CONCURRENCY: PositiveInt = 4
    JOB_TIMEOUT_S: PositiveInt = 2700

    # ── <integration> ──────────────────────────────────────────

    # ── validators ─────────────────────────────────────────────
    @model_validator(mode="after")
    def _dev_flags_refused_in_production(self) -> Settings:
        if self.APP_ENV == "production" and self.RUN_EMBEDDED_WORKERS:
            raise ValueError("RUN_EMBEDDED_WORKERS is development-only")
        return self


@lru_cache
def get_settings() -> Settings:
    """Process-wide settings singleton. Clear with ``get_settings.cache_clear()``."""
    return Settings()
```

**Rules**

- **Read config only via `get_settings()`** (or an injected `Settings`). Never `os.environ` in
  an `app/` module; tests are the only exception.
- **Upper-snake names, typed, with a default** when a safe one exists. Required values (a
  database URL, for example) have no default, so a misconfigured process fails at startup.
- **A comment on every non-obvious setting** says what it does and what goes wrong without it.
  A comment marked "Beyond §N" flags a setting the spec does not list.
- **Dev-only switches are refused in production** by a validator (S8): embedded workers, an
  insecure callback scheme, an env-var secrets backend, auth disabled.
- **Anything the spec calls configurable is configurable.** No hardcoded model names, timeouts,
  limits or URLs in application modules.

### 4.2 `.env.example`

- Committed. **Same section headers, same order as `Settings`** (`# ── datastores ──…`).
- Every variable is present, with a safe placeholder and a comment, including gotchas that cost
  time (e.g. `postgresql+asyncpg://` driver; asyncpg needs `?ssl=require`, not libpq's
  `?sslmode=require`).
- Credentials appear only as obvious placeholders or `vault://…` references.

---

## 5. The Makefile

Self-documenting: every public target carries a `## description`, and `make` with no target
prints the list.

```make
.DEFAULT_GOAL := help
COMPOSE := docker compose

.PHONY: help install fmt lint type test test-unit dev dev-migrate up up-multi logs down migrate

help:  ## List targets
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-14s %s\n", $$1, $$2}'

install:  ## Create the venv and install runtime + dev dependencies
	uv sync --extra dev

fmt:  ## Format and apply safe lint fixes
	uv run ruff format app tests
	uv run ruff check --fix app tests

lint:  ## Lint without modifying files
	uv run ruff format --check app tests
	uv run ruff check app tests

type:  ## Type-check (strict for app.core, app.schemas, deterministic domain packages)
	uv run mypy app

test:  ## Full suite, excluding the slow tier
	uv run pytest -m "not slow" --cov=app --cov-report=term-missing

test-unit:  ## Unit tests only — no containers, no network
	uv run pytest tests/unit -q -m "not integration"

dev:  ## ONE terminal: API + embedded workers (development only)
	@RUN_EMBEDDED_WORKERS=true uv run uvicorn app.main:app --host 0.0.0.0 --port 8000

dev-migrate:  ## Apply migrations against DATABASE_URL in .env (no containers)
	uv run alembic upgrade head

up:  ## Docker: the default single-process stack
	@mkdir -p output && chmod 777 output
	$(COMPOSE) up -d --build

up-multi:  ## Docker: production-shaped topology (separate api and workers)
	$(COMPOSE) --profile multi up -d --build

logs:  ## Follow the logs of whatever is running
	$(COMPOSE) --profile multi logs -f

down:  ## Stop the stack and remove its volumes
	$(COMPOSE) --profile multi down -v

migrate:  ## Apply migrations in a throwaway container
	$(COMPOSE) --profile tools run --rm --no-deps migrate alembic upgrade head

# ── project-specific checks (examples) ──
# <domain>-check:  ## Validate <domain> data files without touching the database
# snapshots:      ## Regenerate golden files — then REVIEW the diff
```

**Rules**

- **Every command runs through `uv run`** (or `$(COMPOSE)`). No bare `python` or `pytest`.
- **Standard names everywhere:** the target set above means the same thing in every accelerator.
- **Project-specific checks become targets too** (data validation, golden-file regeneration,
  doc generation, eval runs), each with a `##` line saying what it guards.
- **A target that regenerates a committed artifact** prints a reminder to review the diff.
- **Parameterised targets validate their input** (`@test -n "$(FIXTURE)" || (echo "usage: …"; exit 1)`).

---

## 6. Containers

### 6.1 `Dockerfile` — three stages, non-root, venv built in place

```dockerfile
# syntax=docker/dockerfile:1
FROM python:3.12-slim AS base
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
# ca-certificates: slim images ship no trust store, and every outbound call is TLS.
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl && rm -rf /var/lib/apt/lists/*

FROM base AS builder
# Only what compiling C extensions needs; none of it reaches the runtime image.
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential && rm -rf /var/lib/apt/lists/*
# Build the venv AT its final path: a virtualenv is not relocatable (console-script
# shebangs are absolute), so copying it elsewhere breaks `uvicorn`, `alembic`, workers.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ARG EXTRAS=""
COPY pyproject.toml uv.lock /src/
WORKDIR /src
RUN pip install uv && uv sync --frozen --no-dev --no-install-project \
      $(for extra in $EXTRAS; do echo "--extra $extra"; done)

FROM base AS runtime
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
WORKDIR /app
COPY app/ ./app/
COPY migrations/ ./migrations/
COPY alembic.ini ./
RUN mkdir -p /app/output && useradd -m -u 10001 <svc> && chown -R <svc>:<svc> /app
USER <svc>
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://localhost:8000/healthz')"
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
```

**Rules:** the lockfile is copied before the source (layer caching); runtime system libraries
go in `base`, header packages only in `builder`; the image runs as uid 10001; there is a
`HEALTHCHECK`; provider SDKs come in through a build-arg of extras.

### 6.2 `.dockerignore`

```text
.git
.gitignore
.idea
.venv
venv
__pycache__
*.py[cod]
*.egg-info
.pytest_cache
.mypy_cache
.ruff_cache
htmlcov
.coverage*
.env
.env.*
!.env.example
tests/
docs/
deploy/
*.md
!README.md
Makefile
docker-compose.yml
Dockerfile
output/
```

### 6.3 `docker-compose.yml` — one file, two topologies

- **Default (`docker compose up`): the dev-parity shape.** The cache/broker (Redis) runs as a
  container, plus **one `app` container** running the API with embedded workers. Migrations
  run first in the same container (`alembic upgrade head && … && uvicorn …`).
- **`--profile multi`: the production-shaped topology.** Separate `api`, worker services and a
  one-shot `migrate` service that the others wait on (`service_completed_successfully`).
- **`--profile tools`: one-shot utilities** (`migrate`, seeders) for `make migrate`.
- **No bundled primary database.** The stack reads `DATABASE_URL` from the same `.env` the host
  flow uses, so the host flow and Docker see one database. A bundled DB would be a second
  database that silently disagrees with the one you inspect. Offer `DOCKER_DATABASE_URL` as an
  override for a host-only (`localhost`) server.

```yaml
name: <project>

x-app: &app
  build: { context: ., dockerfile: Dockerfile }
  image: <svc>-app:local          # ONE shared tag: one build, no stale sibling images
  env_file: .env
  environment: &app-env
    APP_ENV: development
    DATABASE_URL: ${DOCKER_DATABASE_URL:-${DATABASE_URL:?set DATABASE_URL in .env}}
    REDIS_URL: redis://redis:6379/0   # service name, never localhost, inside the network
  volumes:
    - ./app:/app/app:ro
    - ./output:/app/output
  depends_on:
    redis: { condition: service_healthy }

services:
  redis:
    image: redis:7-alpine
    ports: ["${REDIS_PORT:-6380}:6379"]   # not 6379: a host Redis likely owns it
    healthcheck: { test: ["CMD", "redis-cli", "ping"], interval: 5s, timeout: 3s, retries: 20 }

  app:                                      # default: everything in one process
    <<: *app
    environment: { <<: *app-env, RUN_EMBEDDED_WORKERS: "true" }
    command: ["sh", "-c", "alembic upgrade head &&
              uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload --reload-dir /app/app"]
    ports: ["${API_PORT:-8000}:8000"]

  migrate:                                  # one-shot
    <<: *app
    profiles: ["multi", "tools"]
    command: ["sh", "-c", "alembic upgrade head"]
    healthcheck: { disable: true }
    restart: "no"

  api:
    <<: *app
    profiles: ["multi"]
    environment: { <<: *app-env, RUN_EMBEDDED_WORKERS: "false" }
    ports: ["${API_PORT:-8000}:8000"]
    depends_on:
      redis: { condition: service_healthy }
      migrate: { condition: service_completed_successfully }

  worker:
    <<: *app
    profiles: ["multi"]
    command: ["arq", "app.queue.tasks.Worker"]
    environment: { <<: *app-env, RUN_EMBEDDED_WORKERS: "false" }
    healthcheck: { disable: true }          # no HTTP server; the image healthcheck would lie
    depends_on:
      migrate: { condition: service_completed_successfully }
```

Notes: `--reload-dir /app/app` stops the reloader restarting on generated `.py` output; pin
`RUN_EMBEDDED_WORKERS=false` on the `multi` services so a shared `.env` cannot start duplicate
workers; publish host ports through `${VAR:-default}`.

---

## 7. Application conventions

### 7.1 `app/main.py`

- **A factory plus a lifespan, nothing else.** `create_app(settings)` builds the app, installs
  middleware and exception handlers, and mounts `health` and `api/v1`. The lifespan opens
  process-wide resources eagerly (engine, Redis pool, object store, telemetry), so
  misconfiguration fails at startup, and closes them in reverse order on shutdown.
- **Worker-only imports stay out of the API import graph.** They are imported inside functions,
  for example the dev-only embedded workers inside the lifespan.

### 7.2 API

- **Versioned routes:** `/api/v1/...`, one module per resource, aggregated in `v1/router.py`.
- **Health:** `/healthz` (liveness, no dependencies) and `/readyz` (each dependency checked with a
  per-dependency time budget). `/metrics` exposes Prometheus.
- **Long work returns `202 Accepted`** with an id and runs in a worker; a status endpoint and/or
  a signed callback report the result.
- **Inbound schemas use `extra="forbid"`**, so a typo is a `400` naming the field. Errors use one
  body shape, built in one module from domain exceptions (`code`, `message`, `field`).
- **Tenant/auth context** comes from a dependency in `api/deps.py`. Every repository read a
  tenant can reach filters on `tenant_id`.

### 7.3 Persistence and storage

- **One async engine per process** (`db/session.py`), pool sized from settings.
  `DATABASE_SCHEMA` sets the connection `search_path` **and** Alembic's `version_table_schema`.
- **Repositories** (`db/repositories/<aggregate>.py`) are the only place SQL lives; state
  changes go through one method that also writes the audit/event row in the same transaction.
- **Migrations:** `migrations/versions/NNNN_<description>.py`, zero-padded and sequential.
  Applied by `make dev-migrate` (host) or `make migrate` (container), and before the app starts
  in compose.
- **Object storage behind a protocol** with local/S3/Azure backends chosen by a setting. Keys
  come only from `storage/keys.py`, and every key is tenant-prefixed. Uploads stream; they are
  never read whole into memory.

### 7.4 Observability

- **structlog JSON logs** (pretty in development). Bind `request_id`, `tenant_id`, `job_id` and
  `stage` once at the top of a scope with `bind_*` helpers (contextvars, so the binding survives
  `await` and fan-out).
- **Event names are snake_case nouns-in-past** (`job_accepted`, `deploy_decision`). Add a human
  `summary=` for narrative lines.
- **A `redact` processor** scrubs keys matching `(token|secret|password|api_key|authorization)`.
  It keys on the *name*, so never log a secret under an innocuous key.
- **OpenTelemetry** for traces (FastAPI and httpx instrumentation); **Prometheus** metrics
  defined once in `core/metrics.py`.

### 7.5 Security baseline

- Secrets are referenced (`vault://path`) and resolved in-process (`core/secrets.py`). They are
  never logged, never persisted, and cached only in memory with a TTL.
- API keys are stored Argon2id-hashed; a key embeds its tenant id for a single indexed lookup.
- XML/YAML/HTML parsing uses one hardened configuration (no external entities, no network).
- Outbound callback URLs are https-only, with private-address guards (dev flags are refused in
  production).

---

## 8. Background work (only if the project has any)

- **The queue library is imported only in `queue/broker.py`**, so it can be swapped. A test greps
  for violations.
- **Worker classes live in `queue/tasks.py`**: one for the main jobs and one for delivery/retries,
  if needed.
- **Every job ends in a terminal state and a report** (S11). The job function's `finally`
  enqueues the result notification on every exit path: success, exception or cancellation.
- **Heartbeat and watchdog:** a running job heartbeats on its own DB session; a watchdog reclaims
  stale jobs and enforces wall-clock budgets.
- **Retry ladders re-enqueue with a delay** and never `sleep` inside a worker slot.
- **Concurrency has three explicit levels, each its own setting:** jobs per worker, parallel
  sub-tasks inside a job, and process-wide calls to rate-limited providers. Document all three.
- **`make dev` / `RUN_EMBEDDED_WORKERS`** runs the same worker classes inside the API process,
  for development only. It changes *where* the loop runs, never *what* runs.

---

## 9. Tests

### 9.1 Layout

```text
tests/
├── conftest.py            # shared fixtures: container DB/Redis (session-scoped), flags
├── unit/                  # MIRRORS app/: tests/unit/<package>/test_<module>.py
│   ├── api/  core/  db/  schemas/  storage/  queue/  <domain_a>/ …
├── integration/           # multi-component flows against real containers
├── e2e/                   # live external systems; marked `slow`
├── evals/                 # quality evaluation harness for non-deterministic components
└── fixtures/              # input samples + golden expected outputs (expected_*/)
```

### 9.2 Tiers and markers

| Tier | Marker | Needs | Run by |
|---|---|---|---|
| Unit | — | nothing (no network, no containers) | `make test-unit` |
| Integration | `integration` | Docker (testcontainers) **or** `<PREFIX>_TEST_DATABASE_URL` | `make test` |
| E2E | `slow` | live credentials / real external system | explicit opt-in |
| Evals | — | model/provider access | `make eval` |

- **Integration tests use real Postgres and Redis through testcontainers**, not SQLite: partial
  indexes, `ON CONFLICT`, `SKIP LOCKED` and native enums have no faithful substitute. Allow an
  env var to reuse an existing database.
- **`asyncio_mode = "auto"`.** Container fixtures are session-scoped.

### 9.3 Golden / snapshot tests

- Inputs live in `tests/fixtures/<kind>/`, expected outputs in `tests/fixtures/expected_<kind>/`.
- **Regenerate only through a pytest flag** (`--snapshot-update`) behind a Make target that tells
  you to review the diff. A golden diff is reviewed like a code diff.
- **Every bug fix in a deterministic component adds a fixture and its golden file.**

### 9.4 Architectural tests (structure enforced by tests)

- **Import isolation:** import `app.main` in a subprocess and assert that no worker-only package or
  heavy third-party module appears in `sys.modules`.
- **Single-owner modules:** grep that only `queue/broker.py` imports the queue library, and only
  the client module imports the provider SDK.
- **Round-trip tests** for persisted models (`model_dump_json` → `model_validate_json` is equal).
- **Settings tests:** every production refusal actually refuses.

---

## 10. Documentation

### 10.1 `` layout

```text
docs/
├── architecture.md          # components, data flow, process topology
├── runbook.md               # operating it: alerts, common failures, recovery
├── local-testing-guide.md   # host flow: make install / dev-migrate / dev
├── docker-testing-guide.md  # container flow: make up / up-multi
├── security-review.md       # threat model + controls
├── <topic>.md               # generated reports (marked "generated — do not edit")
└── adr/
    ├── 0001-<decision>.md
    └── NNNN-<decision>.md
```

**ADR format** (short; one decision per file; numbered, never renumbered):

```markdown
# ADR NNNN — <decision in one line>

Status: accepted | superseded by NNNN

<Context: what forced a decision, citing the spec section.>
<Decision: what was chosen, and what was explicitly not.>
<Consequences: what this costs, what it enables, what would make us revisit it.>
```

Write an ADR for: a dependency not in the spec, a choice between interchangeable
infrastructure, a deliberate deviation from the spec, and any rule a future engineer would
otherwise "simplify" away.

### 10.2 `CLAUDE.md` — the standing brief

Every accelerator has one at the root, with these sections in this order:

1. **Title + what it is** (one paragraph) and a pointer to `<spec>.md`.
2. **Design principles** — a table: `#`, principle, consequence.
3. **Rules that override convenience** — the two or three non-negotiables.
4. **Layer map** — layer → package(s) → one line each.
5. **Concurrency / topology** — if the service has workers.
6. **Local single-process mode** and the dev-only flags.
7. **"Read this section of the spec before touching this package"** — a package → §section table.
8. **Commands** — a table mirroring `make help`.
9. **Conventions** — bullets of hard-won rules, each stating the rule **and the failure it
   prevents**. ("A widget keeps its first value, so …" is a convention; "be careful with
   widgets" is not.)

Keep it current: a behaviour change that contradicts `CLAUDE.md` updates `CLAUDE.md` in the same
change.

### 10.3 `../README.md`

What the service is (one paragraph), a five-command **Quickstart**
(`cp .env.example .env` → `make install` → `make up` → `make migrate` → seed/smoke), the
day-to-day commands, and links to the spec, `CLAUDE.md`, the runbook and the ADRs.

### 10.4 Docstrings and comments

- Module docstrings state responsibility and spec section. Function docstrings explain **why**,
  plus the failure mode the code prevents, rather than restating the signature.
- Comment non-obvious configuration (Dockerfile, compose, Makefile, settings) with the reason and
  the symptom of getting it wrong ("…the failure is `alembic: not found`, which reads as a
  missing package and is actually a missing interpreter").

---

## 11. Deployment and CI

```text
deploy/
├── k8s/             # api-deployment, worker-deployment(s), services, configmap, secrets, hpa
├── terraform/       # managed DB, cache, storage, secrets; README with apply steps
├── observability/   # alert rules, dashboards
└── <helper>/        # small companion services (e.g. a reference webhook receiver)
```

- **Kubernetes manifests mirror the `multi` compose topology** (one Deployment per process type;
  the API scales on an HPA). The ConfigMap carries non-secret settings; secrets come from a secret
  store, never a committed manifest.
- **Terraform state and `.tfvars` are gitignored.**
- **CI/CD** (`azure-pipelines.yml` or `.github/workflows/`), with this minimum shape:
  1. `make install` → `make lint` → `make type` → `make test-unit` → `make test` (with Docker)
     on pull requests;
  2. on the deploy branch: update the code, `make up` (or build and push an image), poll
     `/healthz`, and **roll back automatically to the previous commit or image** if the health
     check fails;
  3. never store a personal access token in the pipeline — use the platform's per-run token.

---

## 12. Invariants vs. adaptable parts

**Keep identical in every accelerator**

- Top-level layout (section 2.1), the single `app/` package and the platform packages
  `core/`, `db/`, `storage/`, `api/`, `schemas/` and `cli/`.
- `uv` + committed `uv.lock`, hatchling, Python ≥ 3.12, ruff (line 100, same rule set), mypy
  with strict overrides, the pytest markers.
- One `Settings` class with sectioned `.env.example`; production refusal of dev flags.
- The Makefile target names and their meanings.
- The three-stage non-root Dockerfile with the venv built in place.
- A compose file with a single-process default and a `multi` profile; no bundled primary
  database.
- Test tiers, `tests/unit` mirroring `app/`, the import-isolation test, golden-file workflow.
- `docs/adr/` numbering and format, the `CLAUDE.md` section order.
- The security baseline (section 7.5) and logging conventions (section 7.4).

**Adapt per project**

- Domain package names and contents.
- Whether a queue, database, object store or cache exists (remove the matching packages, services,
  Make targets and settings together, and say so in `CLAUDE.md`).
- Provider extras and integration packages.
- Project-specific Make targets (data checks, generators, evals).
- CI platform (Azure Pipelines vs GitHub Actions), keeping the same stages.

---

## 13. Scaffolding checklist (for Claude Code)

Work top to bottom; verify each block before moving on.

**Tooling**
- [ ] `pyproject.toml` per section 3 (grouped deps, extras, dev extra, tool config).
- [ ] `uv lock` run, `uv.lock` committed; `.gitignore` notes that it is committed on purpose.
- [ ] `Makefile` per section 5; `make help` lists every target.

**Configuration**
- [ ] `app/core/config.py`: sectioned `Settings`, `get_settings()`, production-refusal validator.
- [ ] `.env.example`, sectioned identically; `.env` gitignored.

**Package skeleton**
- [ ] `app/main.py` (factory + lifespan), `app/api/{deps,errors,middleware}.py`,
      `app/api/v1/{router,health}.py` with `/healthz` and `/readyz`.
- [ ] `app/core/{exceptions,logging,telemetry,metrics,secrets,security,ids}.py`.
- [ ] `app/db/{session,models}.py` + `repositories/`, `alembic.ini`, `migrations/` with `0001_*`.
- [ ] `app/storage/{base,local,factory,keys}.py`.
- [ ] `app/cli/main.py` (Typer) registered in `[project.scripts]`.
- [ ] `app/queue/` + `app/orchestrator/` **only if** there is background work.
- [ ] Domain packages, each with a module docstring citing the spec.

**Containers**
- [ ] `Dockerfile` (three stages, `/opt/venv`, uid 10001, `HEALTHCHECK`), `.dockerignore`.
- [ ] `docker-compose.yml`: default single-process `app`, `multi` and `tools` profiles, Redis
      with healthcheck, no bundled primary DB, shared image tag.

**Tests**
- [ ] `tests/conftest.py` with session-scoped testcontainers fixtures plus an env override.
- [ ] `tests/unit/<package>/` mirroring `app/`; an import-isolation test; a settings-refusal test.
- [ ] `tests/integration/`, `tests/e2e/` (marked `slow`), and `tests/fixtures/` as needed.

**Docs**
- [ ] `<spec>.md` (or a stub pointing at where the spec lives).
- [ ] `CLAUDE.md` with the section order of 10.2.
- [ ] `../README.md` quickstart; `docs/architecture.md`, `docs/runbook.md`,
      `docs/local-testing-guide.md`, `docs/docker-testing-guide.md`.
- [ ] `docs/adr/0001-*.md` for the first real decision (e.g. queue or provider choice).

**Deploy / CI**
- [ ] `deploy/k8s/` mirroring the `multi` topology; `deploy/terraform/` if infrastructure is managed.
- [ ] CI pipeline: lint → type → unit → integration on PRs; deploy + health check + rollback.

**Done when:** `make install && make lint && make type && make test-unit` pass on a clean clone,
`make up` serves `/healthz`, and `make help` documents every target.
