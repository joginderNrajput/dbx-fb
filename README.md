# dbx2fabric — Databricks → Microsoft Fabric migration service

An agentic FastAPI service that migrates Databricks to Microsoft Fabric. Point it at a live
workspace (Databricks credentials), or upload exported notebooks (`.ipynb`, `.py/.sql/.scala/.r`,
or a `.zip`) when you don't want to share credentials. It inventories, plans and converts
notebooks, jobs, tables, views, functions, secrets and more into Fabric artifacts. It also writes
a runbook, a post-migration report and validation notebooks. Given Fabric credentials, it deploys
everything to your workspace. In `agent` mode, Claude drives the migration and finishes the cells
the rules engine could not convert.

Conversion behaviour is that of the reference tool `dbx2fabric_migrate_v4.py`, ported into the
service and held to it by parity tests.

## Quickstart

```bash
cp .env.example .env          # local defaults: auth disabled, output in ./output
make install                  # uv venv with runtime + ai + dev extras
make dev                      # API on http://localhost:8000  (docs: /docs)
# or with Docker:
make up

curl -F "files=@my_notebook.ipynb" -F 'config={}' http://localhost:8000/api/v1/migrations/upload
```

Then follow the job at `GET /api/v1/migrations/{id}`. When it finishes, open `/report` or
download everything from `/download`.

## Day-to-day

| | |
|---|---|
| `make test` | full test suite (no network) |
| `make lint type security` | static checks |
| `make up` / `make down` | Docker stack |
| `make cli-run SRC=./notebooks` | offline migration without the API |
| `make hash-key` | create an API key for `API_KEY_HASHES` |

## Documentation

- Spec: [`dbx2fabric-migration-service.md`](dbx2fabric-migration-service.md)
- Brief for engineers and agents: [`CLAUDE.md`](CLAUDE.md)
- **How to test locally without Docker:** [`docs/local-testing-guide.md`](docs/local-testing-guide.md)
- **How to test with Docker:** [`docs/docker-testing-guide.md`](docs/docker-testing-guide.md)
- Architecture: [`docs/architecture.md`](docs/architecture.md) · Runbook: [`docs/runbook.md`](docs/runbook.md) ·
  Security: [`docs/security-review.md`](docs/security-review.md) · Decisions: [`docs/adr/`](docs/adr/)
- Project layout standard: [`docs/PROJECT_STRUCTURE_BLUEPRINT.md`](docs/PROJECT_STRUCTURE_BLUEPRINT.md)
