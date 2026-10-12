.DEFAULT_GOAL := help
COMPOSE := docker compose

.PHONY: help install fmt lint type security test test-unit test-integration test-e2e snapshots dev cli-run hash-key build up logs down clean

help:  ## List targets
	@grep -hE '^[a-z0-9-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-17s %s\n", $$1, $$2}'

install:  ## Create the venv and install runtime + ai + dev dependencies
	uv sync --extra dev --extra ai

fmt:  ## Format and apply safe lint fixes
	uv run ruff format app tests
	uv run ruff check --fix app tests

lint:  ## Lint without modifying files
	uv run ruff format --check app tests
	uv run ruff check app tests

type:  ## Type-check (strict for app.core, app.schemas, app.storage)
	uv run mypy app

security:  ## Static security scan of the application package
	uv run bandit -q -c pyproject.toml -r app

test:  ## Full suite, excluding the slow (live credentials) tier
	uv run pytest -m "not slow" --cov=app --cov-report=term-missing

test-unit:  ## Unit tests only: no network, no containers
	uv run pytest tests/unit -q

test-integration:  ## HTTP API flows with faked Databricks / Fabric / Claude
	uv run pytest tests/integration -q

test-e2e:  ## Live tests (set E2E_* variables; see docs/local-testing-guide.md)
	uv run pytest tests/e2e -m slow -v

snapshots:  ## Regenerate golden notebooks - then REVIEW the diff
	uv run pytest tests/unit/conversion/test_golden.py --snapshot-update -q
	@echo "Golden files rewritten under tests/fixtures/expected_notebooks: review 'git diff' before committing."

dev:  ## Run the API on the host with auto-reload (reads .env)
	uv run uvicorn app.main:app --host 0.0.0.0 --port 9093 --reload --reload-dir app

cli-run:  ## Offline migration without the API: make cli-run SRC=./notebooks [OUT=migration_out]
	@test -n "$(SRC)" || (echo "usage: make cli-run SRC=<folder of notebooks> [OUT=migration_out]"; exit 1)
	uv run dbx2fabric run --source-dir "$(SRC)" --out "$(or $(OUT),migration_out)"

hash-key:  ## Generate an API key and its Argon2id hash for API_KEY_HASHES
	uv run dbx2fabric hash-key

build:  ## Docker: build the image
	$(COMPOSE) build

up:  ## Docker: build and start the service on :9093
	@mkdir -p output && chmod 777 output
	$(COMPOSE) up -d --build

logs:  ## Docker: follow the service logs
	$(COMPOSE) logs -f

down:  ## Docker: stop the stack
	$(COMPOSE) down

clean:  ## Remove caches and generated output (asks first)
	@read -p "Delete output/, migration_out/ and caches? [y/N] " ans && [ "$$ans" = y ] && \
	  rm -rf output migration_out .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage || true
