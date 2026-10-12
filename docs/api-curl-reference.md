# API curl reference

Copy-paste curl commands for **every** endpoint of the service (53 routes). Set the variables in
§0 once per shell (replace each `<...>` placeholder; the quotes keep the block pasteable even
before you do). Every command below reads them.

**Credentials always travel in the JSON request body** (never in headers or the URL):

| System | Accepted | Rule |
|---|---|---|
| Databricks | service principal `{host, client_id, client_secret}` **or** personal access token `{host, personal_access_token}` | if `client_id` + `client_secret` are present the service principal (OAuth M2M) is used and the token is ignored; otherwise the token is used. Sending only one of `client_id` / `client_secret` is a 422. `token` is accepted as an alias of `personal_access_token`. |
| Fabric | service principal `{tenant_id, client_id, client_secret}` **only** | all three are required |

- Interactive docs (same endpoints, try-it-out): `http://localhost:8000/docs`
- Errors always look like `{"error": {"code", "message", "field"}}`.
- `X-API-Key` (the service's own API key, not a Databricks/Fabric credential) is required unless
  the server runs with `AUTH_DISABLED=true` (local default); it is included everywhere.
- Long-running work (`POST /migrations`, `/upload`, `/deploy`) returns `202` with a job `id`; poll
  `GET /migrations/{id}` until `status` is `succeeded`, `failed` or `cancelled`.

Contents: [0 Setup](#0-setup) · [1 Health](#1-health) · [2 Connection checks](#2-connection-checks) ·
[3 Browse Databricks](#3-browse-databricks) · [4 Browse Fabric](#4-browse-fabric) ·
[5 Start a migration](#5-start-a-migration) · [6 Follow and inspect](#6-follow-and-inspect-a-migration) ·
[7 Deploy, validate, record](#7-deploy-validate-record) · [8 Convert one asset](#8-convert-one-asset-synchronous) ·
[9 Cancel and delete](#9-cancel-and-delete) · [10 Recipes](#10-end-to-end-recipes)

---

## 0. Setup

```bash
# ── service ──
export BASE="http://localhost:8000"
export API="$BASE/api/v1"
export API_KEY="local-dev"                    # any value when AUTH_DISABLED=true; else a key from `make hash-key`

# ── Databricks (source) ──
export DBX_HOST="https://adb-<workspace-id>.<n>.azuredatabricks.net"
export DBX_CLIENT_ID="<service-principal-application-id>"
export DBX_CLIENT_SECRET="<service-principal-oauth-secret>"
export DBX_TOKEN="<dapi-personal-access-token>"
export DBX_CATALOG="<catalog>"                # e.g. altryxtodatabricks_accelarator_01
export DBX_SCHEMA="<schema>"                  # e.g. flip_local_test
export DBX_PATH="/Workspace/Shared"           # a workspace folder to browse / migrate

# ── Fabric (target): service principal only ──
export FAB_TENANT="<entra-tenant-id>"
export FAB_CLIENT="<service-principal-application-id>"
export FAB_SECRET="<service-principal-client-secret>"
export FAB_WS_NAME="<workspace-name>"         # e.g. Alteryx_Fabric
export FAB_WS_ID="<workspace-guid>"           # optional alternative to the name
export FAB_LAKEHOUSE="<lakehouse-name>"       # e.g. Src_LK

# ── filled in as you go ──
export JOB_ID="<migration-id>"                # from a 202 response, e.g. mig_0123456789abcdef0123
export DBX_JOB_ID="<databricks-job-id>"       # from POST /databricks/jobs
```

Then build the credential objects **once** (run this after the block above; re-run it if you
change a variable). Pick ONE Databricks method:

```bash
# Databricks - Option A: service principal (OAuth M2M)
export DBX_CREDS="{\"host\": \"$DBX_HOST\", \"client_id\": \"$DBX_CLIENT_ID\", \"client_secret\": \"$DBX_CLIENT_SECRET\"}"

# Databricks - Option B: personal access token (use instead of A)
# export DBX_CREDS="{\"host\": \"$DBX_HOST\", \"personal_access_token\": \"$DBX_TOKEN\"}"

# Fabric - service principal
export FAB_CREDS="{\"tenant_id\": \"$FAB_TENANT\", \"client_id\": \"$FAB_CLIENT\", \"client_secret\": \"$FAB_SECRET\"}"

echo "$DBX_CREDS" | python3 -m json.tool >/dev/null && echo "DBX_CREDS ok"
echo "$FAB_CREDS" | python3 -m json.tool >/dev/null && echo "FAB_CREDS ok"
```

Optional helpers (bash/zsh):

```bash
# Pretty-print JSON (falls back to raw output)
pp() { python3 -m json.tool 2>/dev/null || cat; }

# Wait for a migration to finish and print its final status
wait_job() {
  [ -n "$1" ] || { echo "wait_job: no migration id (did the POST fail?)" >&2; return 1; }
  while :; do
    S=$(curl -s "$API/migrations/$1" -H "X-API-Key: $API_KEY" | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
    echo "$S"; [ "$S" = queued ] || [ "$S" = running ] || break; sleep 3
  done
}

# Capture the id of a 202 response:  JOB_ID=$(curl ... | job_id)
# On an error response it prints the error body to stderr and leaves JOB_ID empty.
job_id() { python3 -c 'import sys,json;d=json.load(sys.stdin);print(d["id"]) if "id" in d else sys.exit("request failed: "+json.dumps(d))'; }
```

> Development shortcut: with `ALLOW_ENV_CREDENTIALS=true` and `DATABRICKS_*` / `FABRIC_*` set in
> the server's `.env`, the `credentials` objects can be omitted (`{}` bodies). Refused in production.

---

## 1. Health

Not behind auth.

```bash
# Liveness: the process answers
curl -s $BASE/healthz

# Readiness: output folder writable; reports whether an LLM is configured
curl -s $BASE/readyz | pp

# Prometheus metrics
curl -s $BASE/metrics | head -40
```

---

## 2. Connection checks

Validate credentials without starting anything.

```bash
# Databricks -> {"ok": true, "host": ..., "auth_type": "service_principal" | "personal_access_token", "user": ...}
curl -s -X POST $API/connections/databricks/test \
  -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' \
  --data @- <<EOF | pp
{"credentials": $DBX_CREDS}
EOF

# Databricks - explicit service principal (no variables needed beyond §0)
curl -s -X POST $API/connections/databricks/test \
  -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' \
  --data @- <<EOF | pp
{"credentials": {"host": "$DBX_HOST", "client_id": "$DBX_CLIENT_ID", "client_secret": "$DBX_CLIENT_SECRET"}}
EOF

# Databricks - explicit personal access token
curl -s -X POST $API/connections/databricks/test \
  -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' \
  --data @- <<EOF | pp
{"credentials": {"host": "$DBX_HOST", "personal_access_token": "$DBX_TOKEN"}}
EOF

# Fabric service principal + workspace by name (or "workspace_id": "$FAB_WS_ID")
curl -s -X POST $API/connections/fabric/test \
  -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' \
  --data @- <<EOF | pp
{"credentials": $FAB_CREDS, "workspace_name": "$FAB_WS_NAME"}
EOF

# LLM (Claude or Azure OpenAI, whichever the server is configured for)
curl -s -X POST $API/connections/llm/test -H "X-API-Key: $API_KEY" | pp
```

---

## 3. Browse Databricks

Read-only listings, exposed as `POST` so the credentials stay in the body. Filters go in the
body too; path segments only name a resource. Every list returns
`{count, items, selection_key, id_field, truncated, warnings}`: put `item[id_field]` into
`source.selection.<selection_key>` of `POST /migrations` (§5).

```bash
# One folder level (folders, repos, notebooks) - for tree pickers
curl -s -X POST $API/databricks/workspace -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{"credentials": $DBX_CREDS, "path": "/"}
EOF

# All notebooks under one or more folders (recursive)
curl -s -X POST $API/databricks/notebooks -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{"credentials": $DBX_CREDS, "paths": ["$DBX_PATH"], "limit": 1000}
EOF

# Jobs (optional case-insensitive name filter)
curl -s -X POST $API/databricks/jobs -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{"credentials": $DBX_CREDS, "name": null, "limit": 1000}
EOF

# One job's full definition (tasks, schedule, parameters)
curl -s -X POST $API/databricks/jobs/$DBX_JOB_ID -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{"credentials": $DBX_CREDS}
EOF

# Clusters (all-purpose; set include_job_clusters=true for job/pipeline clusters)
curl -s -X POST $API/databricks/clusters -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{"credentials": $DBX_CREDS, "include_job_clusters": false}
EOF

# DLT / Lakeflow pipelines (optional name filter)
curl -s -X POST $API/databricks/pipelines -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{"credentials": $DBX_CREDS, "name": null}
EOF

# SQL warehouses (also pick one as source.warehouse_id for copy_data)
curl -s -X POST $API/databricks/warehouses -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{"credentials": $DBX_CREDS}
EOF

# Unity Catalog: catalogs -> schemas -> tables
curl -s -X POST $API/databricks/catalogs -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{"credentials": $DBX_CREDS, "include_system": false}
EOF
curl -s -X POST $API/databricks/catalogs/$DBX_CATALOG/schemas -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{"credentials": $DBX_CREDS}
EOF
curl -s -X POST $API/databricks/catalogs/$DBX_CATALOG/schemas/$DBX_SCHEMA/tables -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{"credentials": $DBX_CREDS}
EOF

# Secret scopes and key names (values are never readable)
curl -s -X POST $API/databricks/secret-scopes -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{"credentials": $DBX_CREDS}
EOF

# Unity Catalog volumes / functions / registered models of a schema (id = full name)
for kind in volumes functions models; do
curl -s -X POST $API/databricks/catalogs/$DBX_CATALOG/schemas/$DBX_SCHEMA/$kind -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{"credentials": $DBX_CREDS}
EOF
done

# SQL queries, SQL alerts, AI/BI dashboards, serving endpoints, Git folders, external locations,
# connections (optional case-insensitive name filter)
for kind in queries alerts dashboards serving-endpoints repos external-locations connections; do
curl -s -X POST $API/databricks/$kind -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{"credentials": $DBX_CREDS, "name": null}
EOF
done

# Cluster libraries (they migrate with their cluster: select the cluster id)
curl -s -X POST $API/databricks/libraries -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{"credentials": $DBX_CREDS}
EOF

# Non-notebook workspace files under folders (.py modules, configs, ...)
curl -s -X POST $API/databricks/workspace-files -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{"credentials": $DBX_CREDS, "paths": ["$DBX_PATH"], "limit": 1000}
EOF

# Everything at once, in the engine's assessment format (saved to send as "assessment" in §5.4).
# Every include_* flag defaults to true; set the ones you do not want to false.
curl -s -X POST $API/databricks/inventory -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' -o inventory.json --data @- <<EOF
{"credentials": $DBX_CREDS, "workspace_paths": ["$DBX_PATH"], "catalogs": ["$DBX_CATALOG"], "schemas": ["$DBX_SCHEMA"],
 "include_jobs": true, "include_tables": true, "include_secret_scopes": true,
 "include_clusters": true, "include_pipelines": true, "include_warehouses": true,
 "include_volumes": true, "include_functions": true, "include_models": true,
 "include_sql_assets": true, "include_external_locations": true, "include_connections": true,
 "include_serving_endpoints": true, "include_repos": true, "include_libraries": true,
 "include_workspace_files": true}
EOF
python3 -c 'import json;print(json.load(open("inventory.json")).get("summary"))'
```

---

## 4. Browse Fabric

Pick the target workspace and lakehouse (service principal in the body). A workspace in the path
can be its id or its display name.

```bash
# Workspaces visible to the service principal (optional name filter)
curl -s -X POST $API/fabric/workspaces -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{"credentials": $FAB_CREDS, "name": null}
EOF

# Lakehouses of a workspace (schemas_enabled, OneLake paths, SQL endpoint)
curl -s -X POST "$API/fabric/workspaces/$FAB_WS_NAME/lakehouses" -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{"credentials": $FAB_CREDS}
EOF

# Items already in the workspace, optionally by type (Notebook, DataPipeline, Lakehouse, Warehouse, ...)
curl -s -X POST "$API/fabric/workspaces/$FAB_WS_NAME/items" -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{"credentials": $FAB_CREDS, "type": "Notebook"}
EOF
```

---

## 5. Start a migration

All return `202 {"id": ..., "links": {...}}`. Prefix any of them with `JOB_ID=$(` and end with
`| job_id)` to capture the id.

### 5.1 Live Databricks: selected objects only (recommended)

Use ids/paths from §3. Only these are migrated, each fetched directly (no crawl).

```bash
curl -s -X POST $API/migrations -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{
  "source": {
    "type": "databricks",
    "credentials": $DBX_CREDS,
    "selection": {
      "notebooks": ["$DBX_PATH"],
      "jobs": ["$DBX_JOB_ID"],
      "clusters": [],
      "pipelines": [],
      "warehouses": [],
      "tables": ["$DBX_CATALOG.$DBX_SCHEMA"],
      "secret_scopes": [],
      "volumes": ["$DBX_CATALOG.$DBX_SCHEMA"],
      "functions": ["$DBX_CATALOG.$DBX_SCHEMA"],
      "models": [],
      "queries": [],
      "alerts": [],
      "dashboards": [],
      "serving_endpoints": [],
      "repos": [],
      "external_locations": [],
      "connections": [],
      "workspace_files": []
    }
  },
  "mapping": {"keyvault_url_default": "https://<your-vault>.vault.azure.net/"}
}
EOF
```

### 5.2 Live Databricks: everything under folders / catalogs (crawl)

```bash
curl -s -X POST $API/migrations -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{
  "source": {
    "type": "databricks",
    "credentials": $DBX_CREDS,
    "workspace_paths": ["$DBX_PATH"],
    "catalogs": ["$DBX_CATALOG"],
    "schemas": ["$DBX_SCHEMA"],
    "include_jobs": true, "include_tables": true, "include_secret_scopes": true,
    "include_clusters": true, "include_pipelines": true, "include_warehouses": true
  }
}
EOF
```

### 5.3 Full migration: code + deploy + data to a named workspace and lakehouse

Converts, deploys notebooks/pipelines, copies every table's data, validates row counts and creates
views. Needs a Databricks SQL warehouse; Fabric uses the service principal.

```bash
curl -s -X POST $API/migrations -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{
  "source": {
    "type": "databricks",
    "credentials": $DBX_CREDS,
    "selection": {"notebooks": ["$DBX_PATH"], "tables": ["$DBX_CATALOG.$DBX_SCHEMA"]},
    "warehouse_id": null
  },
  "target": {
    "workspace_name": "$FAB_WS_NAME",
    "lakehouse": "$FAB_LAKEHOUSE",
    "credentials": $FAB_CREDS
  },
  "options": {"deploy": true, "copy_data": true}
}
EOF
```

Variants of `options`:
`{"deploy": true, "deploy_only": ["notebook"]}` (smoke test) ·
`{"deploy": true, "force_deploy": true}` (ignore preflight errors) ·
`{"ai_fix": true}` (LLM rewrites TODO cells) ·
`{"mode": "agent", "agent_instructions": "Prefer Lakehouse tables over Files paths."}` (LLM drives the stages).

### 5.4 Live Databricks with a saved inventory (no discovery at all)

```bash
python3 - <<'PY'
import json, os
body = {"source": {"type": "databricks", "credentials": json.loads(os.environ["DBX_CREDS"])},
        "assessment": json.load(open("inventory.json"))}
json.dump(body, open("migration_body.json", "w"))
PY
curl -s -X POST $API/migrations -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @migration_body.json | pp
```

### 5.5 Agent mode (Claude or Azure OpenAI drives the migration)

```bash
curl -s -X POST $API/migrations -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{
  "source": {"type": "databricks", "credentials": $DBX_CREDS, "selection": {"notebooks": ["$DBX_PATH"]}},
  "options": {"mode": "agent", "agent_instructions": "Keep behaviour identical; explain every gap in a comment."}
}
EOF
```

### 5.6 Offline notebooks (no Databricks credentials)

Multipart upload; the `config` form field is the JSON request body. Run from the repository root
(uses the sample notebooks in `tests/fixtures/notebooks`).

```bash
# Individual files (filename= keeps the folder structure)
curl -s -X POST $API/migrations/upload -H "X-API-Key: $API_KEY" \
  -F "files=@tests/fixtures/notebooks/etl/load_sales.py;filename=etl/load_sales.py" \
  -F "files=@tests/fixtures/notebooks/explore.ipynb" \
  -F "files=@tests/fixtures/notebooks/report.sql" \
  -F 'config={"mapping": {"catalog_to_lakehouse": {"main": "lh_sales"}, "mount_map": {"/mnt/raw": "Files/raw"}}}' | pp

# A zip of a whole export
(cd tests/fixtures/notebooks && zip -qr /tmp/notebooks.zip .)
curl -s -X POST $API/migrations/upload -H "X-API-Key: $API_KEY" \
  -F "files=@/tmp/notebooks.zip" -F 'config={}' | pp

# Notebooks + assessment.json (+ optional mapping.json) from the assessor
curl -s -X POST $API/migrations/upload -H "X-API-Key: $API_KEY" \
  -F "files=@tests/fixtures/assessment.json" -F "files=@/tmp/notebooks.zip" \
  -F 'config={"mapping": {"staging_storage": "abfss://stage@<account>.dfs.core.windows.net/dbx_export"}}' | pp

# Offline + deploy to Fabric
curl -s -X POST $API/migrations/upload -H "X-API-Key: $API_KEY" \
  -F "files=@/tmp/notebooks.zip" \
  -F "config={\"target\": {\"workspace_name\": \"$FAB_WS_NAME\", \"lakehouse\": \"$FAB_LAKEHOUSE\", \"credentials\": $FAB_CREDS},
              \"options\": {\"deploy\": true}}" | pp

# Offline notebooks + data copy (Databricks credentials only to read tables through a SQL warehouse)
curl -s -X POST $API/migrations/upload -H "X-API-Key: $API_KEY" \
  -F "files=@/tmp/notebooks.zip" \
  -F "config={\"databricks\": $DBX_CREDS,
              \"target\": {\"workspace_name\": \"$FAB_WS_NAME\", \"lakehouse\": \"$FAB_LAKEHOUSE\", \"credentials\": $FAB_CREDS},
              \"options\": {\"deploy\": true, \"copy_data\": true}}" | pp
```

---

## 6. Follow and inspect a migration

```bash
# List migrations (newest first)
curl -s "$API/migrations?limit=20" -H "X-API-Key: $API_KEY" | pp

# One migration: status, stage, inventory, summary, preflight counts, data_copy, agent, error
#   options.databricks_auth shows which Databricks method was used (never the secret)
curl -s $API/migrations/$JOB_ID -H "X-API-Key: $API_KEY" | pp

# Poll until done
wait_job $JOB_ID

# Ledger items (optional filters: outcome=migrated|needs_testing|partial|not_migrated, type=Notebook|Job|Table/View|...)
curl -s "$API/migrations/$JOB_ID/items" -H "X-API-Key: $API_KEY" | pp
curl -s "$API/migrations/$JOB_ID/items?outcome=partial" -H "X-API-Key: $API_KEY" | pp
curl -s "$API/migrations/$JOB_ID/items?type=Table/View" -H "X-API-Key: $API_KEY" | pp

# Preflight (deploy blockers and warnings)
curl -s $API/migrations/$JOB_ID/preflight -H "X-API-Key: $API_KEY" | pp

# Reports and runbook
curl -s $API/migrations/$JOB_ID/report -H "X-API-Key: $API_KEY" -o report.html
curl -s $API/migrations/$JOB_ID/report.csv -H "X-API-Key: $API_KEY" -o report.csv
curl -s $API/migrations/$JOB_ID/runbook -H "X-API-Key: $API_KEY"

# Agent steps (mode=agent)
curl -s $API/migrations/$JOB_ID/agent-trace -H "X-API-Key: $API_KEY" | pp

# Generated files: list, fetch one, download everything as a zip
curl -s $API/migrations/$JOB_ID/artifacts -H "X-API-Key: $API_KEY" | pp
curl -s $API/migrations/$JOB_ID/artifacts/artifacts/notebooks/load_sales.ipynb -H "X-API-Key: $API_KEY" -o load_sales.ipynb
curl -s $API/migrations/$JOB_ID/artifacts/RUNBOOK.md -H "X-API-Key: $API_KEY"
curl -s $API/migrations/$JOB_ID/download -H "X-API-Key: $API_KEY" -o migration.zip
```

---

## 7. Deploy, validate, record

Run these one at a time: while a deploy job runs, validation / results calls return `409`.

```bash
# Deploy (or re-deploy) a finished migration; credentials travel with this request only.
# "only" picks kinds: environment, notebook, shortcuts, pipeline (empty = all)
curl -s -X POST $API/migrations/$JOB_ID/deploy -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{
  "target": {"workspace_name": "$FAB_WS_NAME", "lakehouse": "$FAB_LAKEHOUSE", "credentials": $FAB_CREDS},
  "only": ["notebook"],
  "force": false
}
EOF
```

```bash
# Deploy everything + copy table data, volume files and workspace files (Databricks credentials again)
curl -s -X POST $API/migrations/$JOB_ID/deploy -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{
  "target": {"workspace_name": "$FAB_WS_NAME", "lakehouse": "$FAB_LAKEHOUSE", "credentials": $FAB_CREDS},
  "only": [],
  "copy_data": true,
  "databricks": $DBX_CREDS,
  "warehouse_id": null
}
EOF
```

```bash
# Import validation results (results.json written by the validate_migration notebook in Fabric)
curl -s -X POST $API/migrations/$JOB_ID/validation -H "X-API-Key: $API_KEY" -F "file=@results.json" | pp

# Record manual / AI work done outside the service (outcome: migrated|needs_testing|partial|not_migrated)
curl -s -X POST $API/migrations/$JOB_ID/results -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<'EOF' | pp
{"item_id": "Dashboard:Sales KPIs", "outcome": "needs_testing", "note": "Rebuilt as a Power BI report", "target_id": null}
EOF
```

---

## 8. Convert one asset (synchronous)

Nothing is stored and no credentials are needed; useful for trying the rules on a single file.

```bash
# Notebook -> JSON report (outcome, changes, TODO blockers, warnings, converted notebook)
curl -s -X POST $API/convert/notebook -H "X-API-Key: $API_KEY" \
  -F "file=@tests/fixtures/notebooks/etl/load_sales.py" \
  -F 'mapping={"catalog_to_lakehouse": {"main": "lh_sales"}, "mount_map": {"/mnt/raw": "Files/raw"}}' \
  -F 'catalogs=["main"]' | pp

# Notebook -> download the Fabric .ipynb
curl -s -X POST $API/convert/notebook -H "X-API-Key: $API_KEY" \
  -F "file=@tests/fixtures/notebooks/explore.ipynb" -F "download=true" -o explore.fabric.ipynb

# Databricks job settings -> Fabric Data Pipeline + schedule
curl -s -X POST $API/convert/job -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' \
  --data "{\"name\": \"nightly_sales\", \"settings\": $(cat tests/fixtures/job_settings.json), \"mapping\": {}}" | pp
```

---

## 9. Cancel and delete

```bash
# Cancel a queued/running migration (stops at the next stage boundary)
curl -s -X POST $API/migrations/$JOB_ID/cancel -H "X-API-Key: $API_KEY" | pp

# Delete a finished migration and its files (204)
curl -s -o /dev/null -w "%{http_code}\n" -X DELETE $API/migrations/$JOB_ID -H "X-API-Key: $API_KEY"
```

---

## 10. End-to-end recipes

### Browse → pick → migrate everything (code + data) → check

```bash
# 1. find what to migrate
curl -s -X POST $API/databricks/notebooks -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{"credentials": $DBX_CREDS, "paths": ["$DBX_PATH"]}
EOF
curl -s -X POST $API/databricks/catalogs/$DBX_CATALOG/schemas/$DBX_SCHEMA/tables -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | pp
{"credentials": $DBX_CREDS}
EOF

# 2. start the migration and capture its id
JOB_ID=$(curl -s -X POST $API/migrations -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' --data @- <<EOF | job_id
{"source": {"type": "databricks", "credentials": $DBX_CREDS,
            "selection": {"notebooks": ["$DBX_PATH"], "tables": ["$DBX_CATALOG.$DBX_SCHEMA"]}},
 "target": {"workspace_name": "$FAB_WS_NAME", "lakehouse": "$FAB_LAKEHOUSE", "credentials": $FAB_CREDS},
 "options": {"deploy": true, "copy_data": true}}
EOF
); echo "$JOB_ID"

# 3. wait, then look at the outcome
wait_job $JOB_ID
curl -s $API/migrations/$JOB_ID -H "X-API-Key: $API_KEY" | python3 -c \
  'import sys,json;d=json.load(sys.stdin);print(d["status"]);print(d["summary"]);print(d.get("data_copy"));print(d.get("error"))'
curl -s "$API/migrations/$JOB_ID/items?outcome=partial" -H "X-API-Key: $API_KEY" | pp
```

### Offline smoke test (no credentials, works on a fresh clone)

```bash
JOB_ID=$(curl -s -X POST $API/migrations/upload -H "X-API-Key: $API_KEY" \
  -F "files=@tests/fixtures/notebooks/etl/load_sales.py;filename=etl/load_sales.py" \
  -F "files=@tests/fixtures/notebooks/explore.ipynb" -F 'config={}' | job_id)
wait_job $JOB_ID
curl -s "$API/migrations/$JOB_ID/items" -H "X-API-Key: $API_KEY" | pp
curl -s $API/migrations/$JOB_ID/download -H "X-API-Key: $API_KEY" -o migration.zip && unzip -l migration.zip | tail -5
```

---

### Status codes you will see

| Code | Meaning |
|---|---|
| 200 / 202 / 204 | OK / job accepted / deleted |
| 400 | malformed request; `field` names the unknown or invalid field (e.g. `credentials.client_secret` missing for Fabric) |
| 401 | missing or invalid `X-API-Key` |
| 404 | unknown migration, file or item |
| 405 | wrong method (browse endpoints are `POST`) |
| 409 | the migration is running (or already finished, for cancel) |
| 413 | upload too large (`MAX_UPLOAD_MB`) |
| 422 | validation: missing credentials, half a Databricks service principal, bad tenant id, private Databricks host, unknown mapping key, preflight errors |
| 502 | Databricks / Fabric / Entra ID returned an error (message included) |
| 503 | agent mode / ai_fix / llm test without a configured LLM |
