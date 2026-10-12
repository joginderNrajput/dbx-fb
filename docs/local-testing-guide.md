# Local testing guide — without Docker

This guide runs the service directly on your machine with `uv`, then exercises every way of
using it:

- offline notebooks (no Databricks credentials)
- a live Databricks workspace
- deploying to Fabric
- agent mode
- the automated test suite, and the full code + data migration

For the container flow see [`docker-testing-guide.md`](docker-testing-guide.md). Both flows
read the same `.env` and write to the same `./output` folder.

---

## 1. Prerequisites

| Need | Why | Check |
|---|---|---|
| Python ≥ 3.12 | runtime | `python3 --version` |
| [uv](https://docs.astral.sh/uv/getting-started/installation/) | venv + locked dependencies | `uv --version` |
| `make` | task runner (optional; every target is a plain `uv run …` command) | `make --version` |
| `curl` | API calls in this guide (or use Swagger UI at `/docs`) | `curl --version` |

Optional, only for the scenarios that need them:

- **Databricks**: workspace URL plus a personal access token (`dapi…`), or a service principal with
  OAuth M2M (client id + secret). Read access to workspace, jobs, Unity Catalog, secrets and
  clusters is enough.
- **Fabric**: a workspace id (the GUID in the workspace URL) plus an Entra ID service principal
  (tenant id, client id, secret) that is a Contributor on that workspace. The tenant setting
  "Service principals can use Fabric APIs" must be enabled. A short-lived bearer token works too.
- **Claude** (agent mode / `ai_fix`): an Anthropic API key.

Windows: use WSL2, or run the `uv run …` commands from the Makefile by hand in PowerShell. In
PowerShell, replace `\` line continuations with backticks.

---

## 2. Install and configure

```bash
git clone <repo-url> dbx-fb && cd dbx-fb
cp .env.example .env
make install            # = uv sync --extra dev --extra ai
```

Settings in `.env` that matter for local testing:

| Setting | Local value | Meaning |
|---|---|---|
| `AUTH_DISABLED` | `true` | no `X-API-Key` needed (refused in production) |
| `OUTPUT_DIR` | `output` | jobs, uploads and generated artifacts go to `./output/jobs/<id>/` |
| `ANTHROPIC_API_KEY` | empty or your key | needed only for `mode=agent` / `ai_fix` |
| `ALLOW_ENV_CREDENTIALS` | `false` | set `true` to let requests omit credentials and use `DATABRICKS_*` / `FABRIC_*` from `.env` |
| `FABRIC_ALLOW_AZ_CLI` | `false` | set `true` to deploy with your `az login` token (`"use_az_cli": true`) |

To test with authentication on, run `make hash-key`. Put the printed hash in
`API_KEY_HASHES='…'` (keep the single quotes), set `AUTH_DISABLED=false`, and send the key as
`-H "X-API-Key: <key>"` on every `/api/v1` call.

---

## 3. Run the automated tests first

```bash
make lint type security     # ruff, mypy, bandit
make test-unit              # ~100 unit tests, no network
make test-integration       # full HTTP flows with faked Databricks / Fabric / Claude
make test                   # everything except live tests, with coverage
```

What these prove without any credentials:

- **Parity** (`tests/unit/conversion/test_parity.py`): the service converts the fixtures exactly
  like `dbx2fabric_migrate_v4.py`: notebooks, pipelines, scripts and ledger items.
- **Golden notebooks** (`tests/unit/conversion/test_golden.py`): conversion output is stable. If
  you change a rule on purpose, run `make snapshots` and review the diff.
- **API flows** (`tests/integration/`): offline upload, live Databricks (mocked), deploy to Fabric
  (mocked), re-deploy, agent mode, AI failure fallback, validation import, request validation.

Live tests against real systems are opt-in:

```bash
export E2E_DATABRICKS_HOST=https://adb-xxxx.azuredatabricks.net E2E_DATABRICKS_TOKEN=dapi...
export E2E_DATABRICKS_PATH=/Shared            # keep the live run small
export E2E_FABRIC_TENANT_ID=... E2E_FABRIC_CLIENT_ID=... E2E_FABRIC_CLIENT_SECRET=... E2E_FABRIC_WORKSPACE_ID=...
make test-e2e
```

---

## 4. Start the API

```bash
make dev        # uvicorn app.main:app --reload on http://localhost:8000
```

Check it:

```bash
curl -s localhost:8000/healthz          # {"status":"ok"}
curl -s localhost:8000/readyz           # storage ok; reports whether AI is configured
```

Interactive API docs (try every endpoint from the browser): <http://localhost:8000/docs>.

The rest of this guide uses these shell helpers:

```bash
API=http://localhost:8000/api/v1
# poll a job until it finishes
wait_job() { while :; do S=$(curl -s $API/migrations/$1 | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])'); echo "$S"; [ "$S" = queued ] || [ "$S" = running ] || break; sleep 2; done; }
```

---

## 5. Scenario A — offline notebooks (no Databricks credentials)

The repository ships sample notebooks under `tests/fixtures/notebooks/`:

- `etl/load_sales.py`: Databricks source format with widgets, secrets, mounts, catalog names,
  `%run`, `%sql`, `%sh` and Auto Loader
- `explore.ipynb`: Databricks-exported notebook
- `report.sql`: SQL notebook

**A1. Upload individual files.** Folder names are kept when you pass `filename=`:

```bash
cd tests/fixtures/notebooks
curl -s -X POST $API/migrations/upload \
  -F "files=@etl/load_sales.py;filename=etl/load_sales.py" \
  -F "files=@explore.ipynb" \
  -F "files=@report.sql" \
  -F 'config={"mapping": {"catalog_to_lakehouse": {"main": "lh_sales"}, "mount_map": {"/mnt/raw": "Files/raw"}}}'
cd -
```

The response is `202` with the job `id` and links. Then:

```bash
ID=<id from the response>
wait_job $ID
curl -s $API/migrations/$ID | python3 -m json.tool        # status, inventory, summary, preflight
```

**A2. Upload a zip** (an exported folder, a repo checkout, a Databricks "Export → Source" zip):

```bash
(cd tests/fixtures/notebooks && zip -qr /tmp/notebooks.zip .)
curl -s -X POST $API/migrations/upload -F "files=@/tmp/notebooks.zip" -F 'config={}'
```

**A3. Upload with a full inventory.** If you ran the assessor (`dbx2fabric_assess.py`), add its
`assessment.json` (and optionally a `mapping.json`). Tables, jobs, secret scopes and the rest are
then migrated too, with notebook source taken from the uploaded files:

```bash
curl -s -X POST $API/migrations/upload \
  -F "files=@tests/fixtures/assessment.json" \
  -F "files=@/tmp/notebooks.zip" \
  -F 'config={"mapping": {"staging_storage": "abfss://stage@myacct.dfs.core.windows.net/dbx_export"}}'
```

**Inspect the results** (works for every scenario):

```bash
curl -s "$API/migrations/$ID/items?outcome=partial" | python3 -m json.tool   # what still needs work and why
curl -s $API/migrations/$ID/preflight | python3 -m json.tool                 # deploy blockers / warnings
curl -s $API/migrations/$ID/runbook                                          # step-by-step go-live
curl -s $API/migrations/$ID/report -o report.html && open report.html        # or xdg-open
curl -s $API/migrations/$ID/artifacts | python3 -m json.tool                 # list generated files
curl -s $API/migrations/$ID/artifacts/artifacts/notebooks/load_sales.ipynb   # one file
curl -s $API/migrations/$ID/download -o migration.zip                        # everything
```

The same files are on disk under `output/jobs/$ID/out/`.

**Check the conversion.** Open `load_sales.ipynb`:

- A tagged `parameters` cell replaces the widgets.
- `dbutils.secrets.get` became `notebookutils.credentials.getSecret`.
- `/mnt/raw` became `Files/raw` and `main.bronze.sales_raw` became `lh_sales.bronze.sales_raw`.
- `TODO[dbx2fabric:…]` comments mark what needs manual work: the mount call, Auto Loader, `%sh`.

**A4. Convert a single notebook synchronously** (nothing stored):

```bash
curl -s -X POST $API/convert/notebook -F file=@tests/fixtures/notebooks/report.sql | python3 -m json.tool
curl -s -X POST $API/convert/notebook -F file=@tests/fixtures/notebooks/explore.ipynb -F download=true -o explore.fabric.ipynb
curl -s -X POST $API/convert/job -H 'content-type: application/json' \
  -d "{\"name\": \"nightly\", \"settings\": $(cat tests/fixtures/job_settings.json)}" | python3 -m json.tool
```

**A5. No API at all (CLI):**

```bash
make cli-run SRC=tests/fixtures/notebooks OUT=migration_out
open migration_out/post_migration_report.html
```

---

## 6. Scenario B — live Databricks workspace

Check the credentials first:

```bash
curl -s -X POST $API/connections/databricks/test -H 'content-type: application/json' \
  -d '{"credentials": {"host": "https://adb-1234567890123456.7.azuredatabricks.net", "token": "dapi..."}}'
# {"ok": true, "host": "...", "user": "you@company.com"}
```

Start a migration. Limit `workspace_paths` / `catalogs` for a first run:

```bash
curl -s -X POST $API/migrations -H 'content-type: application/json' -d '{
  "source": {
    "type": "databricks",
    "credentials": {"host": "https://adb-1234567890123456.7.azuredatabricks.net", "token": "dapi..."},
    "workspace_paths": ["/Repos/team/etl", "/Shared"],
    "catalogs": ["main"],
    "include_jobs": true, "include_tables": true, "include_secret_scopes": true, "include_clusters": true
  },
  "mapping": {"keyvault_url_default": "https://my-vault.vault.azure.net/"}
}'
```

To use a service principal instead of a PAT, replace `token` with
`"client_id": "...", "client_secret": "..."`.

Discovery problems such as a schema you cannot read show up as `inventory.warnings` on the job;
they do not fail it. Inspect the results as in Scenario A.

Development shortcut: with `ALLOW_ENV_CREDENTIALS=true` and `DATABRICKS_HOST` /
`DATABRICKS_TOKEN` in `.env`, send `"credentials": {}`, or `"token": "env://DATABRICKS_TOKEN"`.

---

### 6b. Browse first, then migrate only what you picked

The browse API lists what is in the workspace. Credentials go in **headers**. With
`ALLOW_ENV_CREDENTIALS=true` and `.env` filled in, you can leave them out.

```bash
H=(-H "X-Databricks-Host: https://adb-<id>.<n>.azuredatabricks.net" -H "X-Databricks-Token: dapi...")
curl -s "${H[@]}" "$API/databricks/notebooks?path=/Workspace/Shared" | python3 -m json.tool
curl -s "${H[@]}" "$API/databricks/workspace?path=/Repos"            # one folder level
curl -s "${H[@]}" "$API/databricks/jobs?name=sales"
curl -s "${H[@]}" "$API/databricks/jobs/123456"                       # full job definition
curl -s "${H[@]}" "$API/databricks/clusters"
curl -s "${H[@]}" "$API/databricks/pipelines"                         # DLT / Lakeflow
curl -s "${H[@]}" "$API/databricks/warehouses"                        # also used by copy_data
curl -s "${H[@]}" "$API/databricks/catalogs"
curl -s "${H[@]}" "$API/databricks/catalogs/altryxtodatabricks_accelarator_01/schemas"
curl -s "${H[@]}" "$API/databricks/catalogs/altryxtodatabricks_accelarator_01/schemas/flip_local_test/tables"
curl -s "${H[@]}" "$API/databricks/secret-scopes"                     # key names only

F=(-H "X-Fabric-Tenant-Id: <tenant>" -H "X-Fabric-Client-Id: <app-id>" -H "X-Fabric-Client-Secret: <secret>")
curl -s "${F[@]}" "$API/fabric/workspaces"
curl -s "${F[@]}" "$API/fabric/workspaces/Alteryx_Fabric/lakehouses"  # id or name; shows schemas_enabled
```

Every list says where its ids go: `selection_key` and `id_field`. For example, jobs returns
`"selection_key": "jobs", "id_field": "job_id"`. Put the picked values into `source.selection`:

```bash
curl -s -X POST $API/migrations -H 'content-type: application/json' -d '{
  "source": {
    "type": "databricks",
    "credentials": {"host": "https://adb-...", "token": "dapi..."},
    "selection": {
      "notebooks": ["/Workspace/Shared/etl/load_sales", "/Repos/team/reporting"],
      "jobs": ["123456"],
      "clusters": ["0101-123456-abcd"],
      "pipelines": ["a1b2c3d4-..."],
      "warehouses": ["7f3c..."],
      "tables": ["altryxtodatabricks_accelarator_01.flip_local_test.orders",
                 "altryxtodatabricks_accelarator_01.flip_local_test"],
      "secret_scopes": ["kv-sales"]
    }
  },
  "target": {"workspace_name": "Alteryx_Fabric", "lakehouse": "Src_LK", "credentials": {"tenant_id": "...", "client_id": "...", "client_secret": "..."}},
  "options": {"deploy": true, "copy_data": true}
}'
```

With a selection, the job does **not** crawl the workspace. It reads only those objects, each
with one direct lookup. A folder path selects every notebook under it, and `catalog.schema`
selects all of its tables. Kinds you leave out are not migrated. Anything that can't be read (a
deleted job, a missing permission) shows up in the job's `inventory.warnings`.

Alternatively, `GET /databricks/inventory?path=...&catalog=...&schema=...` returns the whole
inventory in the engine's format. Send it, or a trimmed copy of its `assets`, as `assessment` in
`POST /migrations` to skip discovery completely.

---

## 7. Scenario C — deploy to Fabric

Check the Fabric credentials and workspace:

```bash
curl -s -X POST $API/connections/fabric/test -H 'content-type: application/json' -d '{
  "credentials": {"tenant_id": "<tenant-guid>", "client_id": "<app-id>", "client_secret": "<secret>"},
  "workspace_id": "<workspace-guid>"}'
```

**C1. Convert and deploy in one go.** This works for both upload and live sources:

```bash
curl -s -X POST $API/migrations/upload \
  -F "files=@/tmp/notebooks.zip" \
  -F 'config={
    "target": {"workspace_id": "<workspace-guid>",
               "credentials": {"tenant_id": "<tenant-guid>", "client_id": "<app-id>", "client_secret": "<secret>"}},
    "mapping": {"catalog_to_lakehouse": {"main": "lh_sales"}},
    "options": {"deploy": true, "deploy_only": ["notebook"]}
  }'
```

Start with `"deploy_only": ["notebook"]` as a smoke test. Open one notebook in Fabric and check
the default lakehouse and the parameter cell. Then deploy everything.

**C2. Deploy a finished migration later.** Credentials travel with this request only:

```bash
curl -s -X POST $API/migrations/$ID/deploy -H 'content-type: application/json' -d '{
  "target": {"workspace_id": "<workspace-guid>",
             "credentials": {"tenant_id": "<tenant-guid>", "client_id": "<app-id>", "client_secret": "<secret>"}},
  "only": [], "force": false}'
wait_job $ID
curl -s $API/migrations/$ID | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d["summary"]["deployed"], "deployed")'
```

If preflight has errors, the job fails with `validation_failed` and the error lists them. Fix the
mapping and run a new migration, or pass `"force": true` if you accept the risk. Deploys are
idempotent: re-running updates the existing items.

Other credential options: `"access_token": "<bearer>"`. In development, with
`FABRIC_ALLOW_AZ_CLI=true` after `az login`, use `"use_az_cli": true`.

**C3. Validate data after the copy** (see the runbook):

1. Run `artifacts/validation/databricks_source_metrics.py` in Databricks.
2. Run the `validate_migration` notebook in Fabric.
3. Upload its result:

```bash
curl -s -X POST $API/migrations/$ID/validation -F file=@results.json
```

Record manual work so the report stays accurate:

```bash
curl -s -X POST $API/migrations/$ID/results -H 'content-type: application/json' \
  -d '{"item_id": "Dashboard:Sales KPIs", "outcome": "needs_testing", "note": "rebuilt as Power BI report"}'
```

---

## 8. Scenario D — migrate everything (code + data) to a named workspace and lakehouse

This is the full migration: inventory, convert, deploy notebooks and pipelines, **copy every
table's data**, validate row counts and create the views. It targets the Fabric workspace and
lakehouse by name.

Requirements beyond Scenario C:
- a Databricks **SQL warehouse** the credentials can use (stopped warehouses auto-start);
- a Fabric **service principal**: a bare `access_token` cannot write to OneLake;
- a Fabric capacity that can run notebooks (the loader runs as a Fabric notebook job);
- network access to `*.azuredatabricks.net`, `api.fabric.microsoft.com`,
  `onelake.dfs.fabric.microsoft.com`, `login.microsoftonline.com` and, for
  `DATA_COPY_DISPOSITION=EXTERNAL_LINKS`, `*.blob.core.windows.net`.

```bash
curl -s -X POST $API/migrations -H 'content-type: application/json' -d '{
  "source": {
    "type": "databricks",
    "credentials": {"host": "https://adb-<id>.<n>.azuredatabricks.net", "token": "dapi..."},
    "workspace_paths": ["/Workspace/Shared"],
    "catalogs": ["altryxtodatabricks_accelarator_01"],
    "schemas": ["flip_local_test"]
  },
  "target": {
    "workspace_name": "Alteryx_Fabric",
    "lakehouse": "Src_LK",
    "credentials": {"tenant_id": "<tenant>", "client_id": "<app-id>", "client_secret": "<secret>"}
  },
  "options": {"deploy": true, "copy_data": true}
}'
wait_job $ID
curl -s $API/migrations/$ID | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d["status"], d["data_copy"], d["summary"]["migrated"], "migrated")'
curl -s "$API/migrations/$ID/items?type=Table/View" | python3 -m json.tool   # validation per table
```

What you get in Fabric:
- In lakehouse `Src_LK`, one Delta table per Databricks table, under the same schema name. If the
  lakehouse has no schemas, the table is named `schema_table` and the item explains why.
- The raw exports, under `Files/dbx_migration/...`.
- Notebooks `load_migrated_data_Src_LK` and `create_views_Src_LK` (both already run), plus every
  migrated notebook and pipeline.

Tables whose row counts match the source are `migrated`, with the counts attached. Use a
schema-enabled lakehouse if the migrated notebooks use `catalog.schema.table` names: they are
rewritten to `Src_LK.schema.table`.

**Development shortcut with `.env`.** With `ALLOW_ENV_CREDENTIALS=true` and the
`DATABRICKS_*`, `FABRIC_*` (including `FABRIC_WORKSPACE_NAME`, `FABRIC_LAKEHOUSE`) and `LLM_*`
values in `.env`, the request needs no credentials at all:

```bash
curl -s -X POST $API/migrations -H 'content-type: application/json' -d '{
  "source": {"type": "databricks", "catalogs": ["altryxtodatabricks_accelarator_01"], "schemas": ["flip_local_test"]},
  "options": {"deploy": true, "copy_data": true}}'
```

The live test suite runs the same flow from `.env`:

```bash
# .env: E2E_LIVE=1, E2E_DATABRICKS_CATALOG=..., E2E_DATABRICKS_SCHEMA=..., optional E2E_DATABRICKS_PATH / E2E_AGENT=1
make test-e2e
```

Data copy on a migration that already finished:

```bash
curl -s -X POST $API/migrations/$ID/deploy -H 'content-type: application/json' -d '{
  "target": {"workspace_name": "Alteryx_Fabric", "lakehouse": "Src_LK", "credentials": {"tenant_id": "...", "client_id": "...", "client_secret": "..."}},
  "copy_data": true, "databricks": {"host": "https://adb-...", "token": "dapi..."}}'
```

---

## 9. Scenario E — agent mode and AI cell fixing

Pick a provider in `.env` and restart `make dev`.

**Claude:** `LLM_PROVIDER=anthropic` and `ANTHROPIC_API_KEY=sk-ant-...`.

**Azure OpenAI:**

```bash
LLM_PROVIDER=azure_openai
LLM_BASE_URL=https://<resource>.openai.azure.com
LLM_API_KEY=<key>
LLM_MODEL=<deployment name, e.g. gpt-5.4-2>
LLM_API_VERSION=2025-01-01-preview
```

Check it with `curl -s -X POST $API/connections/llm/test`, which returns `{"ok": true, ...}`.
`/readyz` then shows `"ai": "configured"`.

```bash
curl -s -X POST $API/migrations/upload \
  -F "files=@/tmp/notebooks.zip" \
  -F 'config={"options": {"mode": "agent", "agent_instructions": "Prefer Lakehouse tables over Files paths."},
              "mapping": {"catalog_to_lakehouse": {"main": "lh_sales"}}}'
wait_job $ID
curl -s $API/migrations/$ID | python3 -c 'import sys,json;print(json.dumps(json.load(sys.stdin)["agent"], indent=2))'
curl -s $API/migrations/$ID/agent-trace | python3 -m json.tool      # every tool call the agent made
```

What to expect:

- The agent inventories, plans and converts. Then it reads each notebook with TODO markers and
  rewrites those cells. Items whose markers are all resolved move from `partial` to
  `needs_testing`; review those cells.
- It deploys only when `options.deploy` is true, Fabric credentials exist and preflight is clean.
- If the agent stops early or the Claude API fails, the deterministic pipeline finishes the job
  (`agent.safety_net` / `agent.stopped = "error"`). The job still produces artifacts and a report.

Lighter option: `"options": {"ai_fix": true}` in pipeline mode only rewrites cells that carry TODO
markers (at most `AI_MAX_CELLS_PER_JOB` per job).

---

## 10. Follow, cancel, clean up

```bash
curl -s $API/migrations | python3 -m json.tool          # newest first
curl -s -X POST $API/migrations/$ID/cancel              # stops at the next stage boundary
curl -s -X DELETE $API/migrations/$ID                   # removes output/jobs/$ID
```

Stop the server with Ctrl+C. Jobs still running at that moment are marked
`failed / interrupted` at the next start. Credentials are never stored, so start a new migration.

---

## 11. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `401 unauthorized` | `AUTH_DISABLED=false` without a key: send `X-API-Key`, or set `AUTH_DISABLED=true` for local tests |
| `422 … host resolves to a private or loopback address` | Databricks host must be public https; for local mocks set `ALLOW_PRIVATE_HOSTS=true` |
| `503 ai_unavailable` | `mode=agent` / `ai_fix` without a configured LLM (`ANTHROPIC_API_KEY`, or the `LLM_*` Azure settings), or the `ai` extra not installed for Claude (`make install`) |
| `413 payload_too_large` | raise `MAX_UPLOAD_MB` / `MAX_EXTRACTED_MB`, or upload a zip of only the notebooks |
| `502 databricks_error … 403` | token lacks permissions or expired; test with `/connections/databricks/test` |
| Notebook items `not_migrated: Source code not available` | upload that notebook, or provide Databricks credentials so it can be exported |
| Job `failed: validation_failed … preflight found errors` | see `/preflight`; usually invalid lakehouse names or duplicate notebook base names |
| Shortcut items stay `partial` | set `mapping.adls_connection_id` (Fabric ADLS Gen2 connection id) |
| Managed tables `not_migrated … staging_storage` | set `mapping.staging_storage`, re-run, then run `export_to_staging.py` in Databricks |
| `502 … failed -> 403 Forbidden` on every call, even with valid credentials | a corporate or sandbox egress proxy blocks the host (Databricks, Fabric, OneLake, Azure OpenAI): allow the domains listed in Scenario D |
| `data_copy.failed > 0`, item `Data export failed … TABLE_OR_VIEW_NOT_FOUND` / permission | the credentials cannot SELECT that table on the SQL warehouse |
| item `Data export failed … result download failed` | `*.blob.core.windows.net` unreachable: allow it, or set `DATA_COPY_DISPOSITION=INLINE` (≤ 25 MiB per table) |
| `OneLake … 401/403` | use a service principal (not a bare Fabric access_token) that is a Contributor on the workspace |
| item `Data load failed … loader notebook … Failed` | open `load_migrated_data_<lakehouse>` in Fabric and run it to see the Spark error; exported files stay in `Files/dbx_migration` |
| `Fabric workspace '…' not found` | the service principal is not a member of the workspace, or the tenant setting "Service principals can use Fabric APIs" is off |
| `Address already in use` on 8000 | another server (or `make up`) is running: `make down`, or `uv run uvicorn app.main:app --port 8001` |
