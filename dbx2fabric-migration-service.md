# Databricks → Microsoft Fabric Migration Service — Specification

Status: v1.0 · Owner: data platform accelerators · Code cites sections of this file as `§N.M`.

---

## 1. Purpose and scope

### 1.1 What the service does

An HTTP service (FastAPI) that migrates Databricks assets to Microsoft Fabric. It accepts:

- **a live Databricks workspace**: a workspace URL plus a PAT or an OAuth service principal. The
  service inventories notebooks, jobs, Unity Catalog tables/views, secret scopes and clusters, and
  exports notebook source through the Databricks REST API; or
- **offline sources** when the user does not want to share Databricks credentials: uploaded
  notebooks (`.ipynb`, Databricks source `.py/.sql/.scala/.r`), `.zip` archives of them, an
  optional `assessment.json` (a full inventory produced by `dbx2fabric_assess.py`) and an optional
  `mapping.json`.

For both, it converts the assets into Fabric artifacts. If Fabric credentials are supplied, it also
**deploys** them to a Fabric workspace. Artifacts include notebooks, Data Pipelines and schedules,
OneLake shortcut specs, Warehouse SQL, environment.yml, dbt projects, validation notebooks, a
runbook, a post-migration report and AI work orders.

Two execution modes:

| Mode | Who drives the stages | Needs |
|---|---|---|
| `pipeline` (default) | fixed order: inventory → plan → convert → preflight → [deploy] → report | nothing extra |
| `agent` | an LLM, through tools (§6): runs the stages, rewrites TODO cells, decides whether deploying is safe | Claude (`ANTHROPIC_API_KEY` + `ai` extra) or Azure OpenAI (`LLM_PROVIDER=azure_openai`) |

`options.ai_fix` (both modes) asks the LLM to rewrite only the cells that still carry TODO markers.
`options.copy_data` also moves the **table data** into Fabric and validates row counts (§5.9).

### 1.2 Reference implementation

`dbx2fabric_migrate_v4.py` at the repository root is the **single source of truth for conversion
behaviour**. It is kept verbatim. The service ports its logic into `app/conversion` and
`app/migration`. `tests/unit/conversion/test_parity.py` runs both on the same inputs and requires
identical output: notebooks, pipelines, scripts and ledger items. Deliberate deviations are listed
in ADR 0003 and nowhere else.

### 1.3 Out of scope

Secret values (never exportable), dashboards/DLT redesign (reported as manual work with AI work
orders), and tables above `DATA_COPY_MAX_ROWS` (they keep the generated export/copy artifacts that
run inside Databricks and Fabric).

---

## 2. Concepts

- **Asset**: one inventoried Databricks object `{type, name, location, meta}`. Types include
  Notebook, Job, Table/View, Volume, Secret Scope, Cluster, Library, DBFS Mount, Function, SQL
  Query, Connection, External Location, Permission, Dashboard, DLT Pipeline, ML Model…
- **Assessment**: `{summary, assets[]}`, the inventory. Produced by discovery (§5.1), by offline
  upload (§5.2) or supplied by the caller.
- **Mapping** (`mapping.json`): the editable routing configuration (§3.4).
- **Ledger** (`ledger.json`): one item per planned asset, with target, strategy, outcome,
  deployment state, actions, reasons, tests and artifacts (§3.5).
- **Outcomes**: `migrated` (nothing to test), `needs_testing`, `partial` (manual rework left;
  TODO markers), `not_migrated`.

---

## 3. Migration engine (deterministic)

### 3.1 Notebook parsing
Databricks source format is split on `COMMAND ----------`. Consecutive `MAGIC` lines become their
own cell, and `%sql/%md/%run/%pip/%sh/%fs/%scala/%r` magics are classified. `.ipynb` cells are read
directly. Output notebooks use the `synapse_pyspark` kernel; widgets become a `parameters`-tagged
cell.

### 3.2 Code rules
Rewrites: `dbutils.secrets.get` → `notebookutils.credentials.getSecret(<vault>, key)`; widgets →
parameter variables; `dbutils.notebook.run/exit`, `dbutils.fs.*` → `notebookutils`; mount paths →
`mount_map`; `catalog.schema.table` → `lakehouse.schema.table`; Databricks-only Spark conf removed.
Blockers get a `TODO[dbx2fabric:RULE]` comment (first matching rule per line). Needs-testing
patterns (streaming, CDF, time travel, Arrow, JDBC…) are counted as warnings.

### 3.3 Jobs → Data Pipelines
Notebook tasks become `TridentNotebook` activities (dependencies, retries, timeouts, base
parameters). Other task types are reported as omitted. Quartz cron becomes a Fabric schedule
(Cron/Daily/Weekly) where possible.

### 3.4 Mapping
Keys and defaults: `app/migration/mapping.py::DEFAULT_MAPPING`. The initial mapping is derived
from the inventory (one lakehouse per catalog, mounts, secret scopes, dbt jobs). Request overrides
are validated: unknown keys or wrong types are a 422 naming the key. Dict-valued keys merge.

### 3.5 Plan and ledger
Routing: OLAP schemas (`warehouse_schemas`) → Warehouse; OLTP connections → SQL database;
everything else → Lakehouse; code → Notebook; jobs → Data Pipeline (+ dbt job item when a job has
dbt tasks).

### 3.6 dbt
dbt project folders must be **uploaded with the sources**. `mapping.dbt_projects[job]` is a path
relative to the uploaded sources; any other path is refused. `profiles.yml` is retargeted to
dbt-fabric (Warehouse) or dbt-fabricspark (Lakehouse); Databricks-specific config is flagged.

### 3.7 Convert
Every ledger item is converted per its strategy. One failing asset never stops the run: it is
recorded on the item. Aggregated artifacts: copy notebooks per lakehouse, view notebooks,
`export_to_staging.py` (managed tables/volumes → your ADLS staging), functions/queries notebooks,
`warehouse/load_tables.sql`, `lakehouse/shortcuts.json`, `environment.yml`, security CSVs,
`ingestion/copy_specs.json`.

### 3.8 Validation
Generated notebooks compute row count, checksum and schema on both sides. Results uploaded to
`POST /migrations/{id}/validation` move passed tables to `migrated` and failures to `partial`.

### 3.9 AI work orders
Every `partial` / `not_migrated` item gets `ai_tasks/<item>.md`: why it needs work, source
metadata, Fabric target rules, and how to record the result.

### 3.10 Runbook
`RUNBOOK.md`: ordered steps from prerequisites to go-live, adapted to the artifacts generated.

### 3.11 Preflight
Static checks before deploy. Errors block deployment unless forced: invalid lakehouse names,
duplicate Fabric names, missing artifact files, leftover MAGIC lines, broken pipeline dependencies.
Warnings: TODO markers, remaining dbutils, Key Vault placeholders, missing connection id, blocked
staging.

### 3.12 Report
`post_migration_report.html` and `.csv`: counts per outcome and per asset type, and per item what
was done, what to test and what remains.

---

## 4. Orchestration

### 4.1 Session and service
`MigrationSession` (`app/migration/session.py`) owns one migration's state and stage operations.
Both modes drive it. `MigrationService` (`app/orchestrator/service.py`) validates requests, turns
credentials into in-memory objects, ingests uploads and submits the job.

### 4.2 Job record
`jobs/<id>/job.json`: status (`queued|running|succeeded|failed|cancelled`), mode, source kind,
current stage, timestamps, options (**no credentials**; only whether they were supplied),
inventory counts, ledger summary, preflight counts, upload stats, agent outcome, error
`{code, message}`.

### 4.3 Runner and budgets
In-process runner (ADR 0001): at most `MAX_CONCURRENT_JOBS` jobs per process, each with a
`JOB_TIMEOUT_S` wall-clock budget. Cancellation and timeout are observed at stage checkpoints.
Every job ends in a recorded terminal state. Jobs still active when the process stops are marked
`failed/interrupted` at the next start, because credentials are not persisted and the job cannot
resume.

### 4.4 Errors
One body for every error: `{"error": {"code", "message", "field"}}`. 400 malformed request
(unknown field named), 401 auth, 404 unknown id/file, 409 state conflict (job running), 413 too
large, 422 domain validation, 502 Databricks/Fabric upstream failure, 503 AI unavailable.

---

## 5. HTTP API (`/api/v1`, `X-API-Key` header)

### 5.1 Live Databricks source — `POST /migrations`
Body: `source {type: databricks, credentials {host, token | client_id+client_secret},
workspace_paths, include_jobs, include_tables, catalogs, schemas, include_secret_scopes,
include_clusters, include_pipelines, include_warehouses, max_notebooks, warehouse_id, selection}`, `target {workspace_id | workspace_name,
lakehouse, credentials}`, `mapping {...}`, `options {mode, deploy, deploy_only, force_deploy,
ai_fix, copy_data, agent_instructions}`, optional `assessment`. `target.lakehouse` sends every
catalog to one destination lakehouse (existing or created). The host must be
public https (private hosts only with `ALLOW_PRIVATE_HOSTS` in development). Partial discovery
failures (one schema, one path) become inventory warnings. → `202 {id, links}`.

### 5.2 Offline source — `POST /migrations/upload` (multipart)
`files`: one or more notebooks / `.zip` / `assessment.json` / `mapping.json`. `config`: JSON
string `{target, mapping, options, databricks?}`. Zip members are extracted with path-traversal,
size (`MAX_EXTRACTED_MB`) and count (`MAX_UPLOAD_FILES`) guards. Folder structure is kept; the
notebook name is the file's base name. Without `assessment.json`, every notebook becomes a
Notebook asset. With it, the uploaded files provide the source for the assessment's notebooks.

### 5.3 Source lookup
Uploaded files first (exact path with each extension, then any file with the same stem; never
outside the uploads), then the Databricks export API if credentials were given.

### 5.4 Fabric credentials
`access_token`, or `tenant_id + client_id + client_secret` (Entra ID client-credentials, scope
`FABRIC_SCOPE`), or `use_az_cli` (development only). `POST /connections/fabric/test` and
`POST /connections/databricks/test` validate credentials without starting a migration.

### 5.5 Deploy — `options.deploy` or `POST /migrations/{id}/deploy`
Runs preflight first and stops on errors unless forced. Ensures lakehouses (schemas enabled),
Warehouse and SQL database, then notebooks (with default lakehouse), shortcuts (needs
`adls_connection_id`), pipelines (notebook ids resolved; notebooks outside the migration must
already exist) and schedules. Re-running updates existing items instead of creating duplicates.
Per-item failures are recorded on the item.

Inspection: `GET /migrations`, `/{id}`, `/{id}/items`, `/{id}/preflight`, `/{id}/report`,
`/{id}/report.csv`, `/{id}/runbook`, `/{id}/artifacts[/{path}]`, `/{id}/download` (zip),
`/{id}/agent-trace`. Changes: `POST /{id}/cancel`, `POST /{id}/results` (record manual work),
`POST /{id}/validation`, `DELETE /{id}`.

### 5.10 Browse API and selection — `GET /databricks/*`, `GET /fabric/*`
Read-only listings to choose what to migrate (and where to), so the migration does not crawl the
workspace again. Credentials go in headers (never the query string): `X-Databricks-Host` +
`X-Databricks-Token` or `X-Databricks-Client-Id`/`-Secret`; `X-Fabric-Tenant-Id`/`-Client-Id`/
`-Client-Secret` or `X-Fabric-Access-Token`. Same host guards and dev env fallback as §5.1.

| Endpoint | Lists | `selection` key ← field |
|---|---|---|
| `/databricks/workspace?path=` | one folder level | `notebooks` ← `path` |
| `/databricks/notebooks?path=&limit=` | notebooks under folder(s), recursive | `notebooks` ← `path` |
| `/databricks/jobs?name=`, `/databricks/jobs/{id}` | jobs (tasks, notebook paths, schedule) / full definition | `jobs` ← `job_id` |
| `/databricks/clusters?include_job_clusters=` | all-purpose clusters | `clusters` ← `cluster_id` |
| `/databricks/pipelines?name=` | DLT / Lakeflow pipelines | `pipelines` ← `pipeline_id` |
| `/databricks/warehouses` | SQL warehouses (also for `source.warehouse_id`) | `warehouses` ← `id` |
| `/databricks/catalogs`, `/catalogs/{c}/schemas`, `/catalogs/{c}/schemas/{s}/tables` | Unity Catalog | `tables` ← `full_name` (a schema = all its tables) |
| `/databricks/secret-scopes` | scopes and key names | `secret_scopes` ← `name` |
| `/databricks/inventory?path=&catalog=&schema=&include_*=` | everything, in assessment format | send as `assessment` |
| `/fabric/workspaces`, `/fabric/workspaces/{id or name}/lakehouses`, `/…/items?type=` | Fabric targets | `target.workspace_*`, `target.lakehouse` |

Every list response is `{count, items, selection_key, id_field, truncated, warnings}`.
`POST /migrations` with `source.selection` (any non-empty list) inventories **only** those
objects, each by a direct lookup (`workspace/get-status`, `jobs/get`, `clusters/get`,
`unity-catalog/tables/{name}`, `pipelines/{id}`, `sql/warehouses/{id}`, `secrets/list`). A folder path
selects every notebook under it. Objects that cannot be read become inventory warnings, not
errors. Notebook source is still exported per selected notebook at convert time.

### 5.9 Data copy — `options.copy_data` (or `copy_data` on `POST /{id}/deploy`)
For every table (MANAGED / EXTERNAL / MATERIALIZED_VIEW / STREAMING_TABLE; views are recreated,
shortcut-served tables skipped):
1. `COUNT(*)` then `SELECT *` on a Databricks SQL warehouse (`source.warehouse_id`, else the first
   running one) through the Statement Execution API, `JSON_ARRAY` format, `DATA_COPY_DISPOSITION`
   `EXTERNAL_LINKS` (presigned downloads without auth header) or `INLINE` (≤ 25 MiB).
2. Each chunk becomes a JSON-lines file in OneLake `Files/<DATA_COPY_FOLDER>/<cat>/<schema>/<table>/`
   (DFS API, token audience `ONELAKE_SCOPE`; needs a service principal or `az` token).
3. A generated `load_migrated_data_<lakehouse>` notebook is deployed and run through the Fabric
   job scheduler (`RunNotebook`). It casts every column back to its Databricks type, writes Delta
   `schema.table` (`schema_table` in a lakehouse without schemas) and compares row counts.
4. Its results file updates the ledger: equal counts → `migrated` with the validation attached;
   otherwise `partial`. Then `create_views_<lakehouse>` runs so views exist over the loaded tables.
Tables above `DATA_COPY_MAX_ROWS` are skipped with a reason. Failures are per table.

### 5.6 Single-asset conversion
`POST /convert/notebook` (multipart file, optional mapping/catalogs, `download=true` returns the
.ipynb) and `POST /convert/job` (job settings JSON → pipeline + schedule). Nothing is stored.

### 5.7 Health
`/healthz` (liveness), `/readyz` (storage writable; AI availability reported), `/metrics`
(Prometheus). Not behind auth.

### 5.8 CLI
`dbx2fabric serve | run --source-dir | convert-notebook | hash-key`.

---

## 6. Agent (Claude)

### 6.1 Loop
A manual tool-use loop (`app/agents/runner.py`). Providers (`LLM_PROVIDER`): `anthropic`
(Messages API, model `AI_MODEL` default `claude-opus-5-5`, effort `AI_EFFORT`, server-side refusal
fallback) or `azure_openai` (chat completions with function tools on deployment `LLM_MODEL`,
`max_completion_tokens`; the client translates the conversation, ADR 0006).
`POST /connections/llm/test` checks the configured provider. All
tool results of a turn go back in one message. Budget: `AGENT_MAX_TURNS`. Stops on `finish`,
end of turn, refusal or budget. **Safety net:** if the agent stops before converting, or the
Claude API fails, the deterministic stages complete the migration. The job's `agent` block
records why.

### 6.2 Tools
`get_job_context, inventory_source, plan_migration, get_mapping, update_mapping, convert_assets,
list_items, get_item, read_notebook, update_notebook_cell, run_preflight, deploy_to_fabric,
copy_data_to_fabric, record_result, finish`. `copy_data_to_fabric` refuses unless copy_data was
requested. Guards in code, not just in the prompt:
- `deploy_to_fabric` refuses unless deploy was requested, Fabric credentials exist and preflight
  has no errors.
- `record_result` cannot claim `migrated`.
- Domain errors are returned to the model as tool errors.

Resolving every TODO marker in a notebook moves its items from `partial` to `needs_testing`
(never `migrated`).

### 6.3 Cell fixer
`options.ai_fix`: each code cell with a TODO marker is sent once (`CELL_FIX_PROMPT`). The fenced
answer replaces the cell. The budget is `AI_MAX_CELLS_PER_JOB`. A failed call leaves the cell and
the item `partial`.

---

## 7. Storage
`OUTPUT_DIR/jobs/<id>/{job.json, agent_trace.json, sources/, out/}`. `out/` holds
`assessment.json, mapping.json, ledger.json, artifacts/, ai_tasks/, RUNBOOK.md,
post_migration_report.*`. Keys are built only in `app/storage/keys.py`. Job ids are validated
(`mig_` + 20 hex chars) before they reach a path.

## 8. Security
### 8.1 Authentication
`X-API-Key` checked against Argon2id hashes (`API_KEY_HASHES`). `AUTH_DISABLED` is development
only and refused in production.
### 8.2 Credentials
Databricks/Fabric credentials arrive per request. They live only in memory for the job and are
never written to job.json, logs or artifacts. Re-deploys need the credentials again.
`env://NAME` references and settings fallbacks exist for development only
(`ALLOW_ENV_CREDENTIALS`).

## 9. Configuration and deployment
### 9.1 Settings
One typed `Settings` (`app/core/config.py`) read from env / `.env`. Production refuses dev flags.
### 9.2 Without Docker
`make install && make dev` (uv, Python ≥ 3.12).
### 9.3 With Docker
Three-stage image, non-root uid 10001, one uvicorn worker (ADR 0005). `docker compose up` mounts
`./output`.

## 10. Observability
structlog (JSON outside development) with `request_id`/`job_id`/`stage` bound and secret-named
keys redacted. Prometheus counters for jobs, durations, item outcomes, Fabric calls and AI calls.
