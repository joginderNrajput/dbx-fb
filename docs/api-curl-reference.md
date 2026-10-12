# API curl reference (Postman-ready)

Every request of the service (53 routes) as **one self-contained curl command** that Postman can
import as is. No shell variables, heredocs or pipes: values come from Postman variables
(`{{baseUrl}}`, `{{dbx_host}}`, ...), which Postman substitutes when it sends the request.

## How to use it in Postman

1. **Create the environment once.** Postman → *Environments* → *Import* →
   [`docs/postman/dbx2fabric.postman_environment.json`](postman/dbx2fabric.postman_environment.json),
   then fill in the values (table below) and select the environment (top right).
2. **Import a request.** Copy one curl block → Postman *Import* (or `Ctrl/Cmd+O`) → *Raw text* →
   paste → *Continue* → *Import*. Pasting the curl straight into the URL bar of a new request
   works too. Postman keeps the `{{...}}` placeholders and fills them from the environment.
3. **Send.** Long-running calls return `202` with an `id`; put it in `job_id` (or add the test
   script below so Postman does it for you), then poll *Get one migration* until `status` is
   `succeeded`, `failed` or `cancelled`.

### Environment variables

| Variable | Example | Used for |
|---|---|---|
| `baseUrl` | `http://localhost:8000` | every request |
| `apiKey` | `local-dev` | `X-API-Key` header (any value when the server runs with `AUTH_DISABLED=true`; else a key from `make hash-key`) |
| `dbx_host` | `https://adb-1234567890123456.7.azuredatabricks.net` | Databricks workspace URL |
| `dbx_client_id` | service principal application id | Databricks **service principal** (leave empty to use the token) |
| `dbx_client_secret` | service principal OAuth secret | Databricks **service principal** (leave empty to use the token) |
| `dbx_token` | `dapi...` | Databricks **personal access token** (leave empty when using the service principal) |
| `dbx_path` | `/Workspace/Shared` | a workspace folder to browse / migrate |
| `dbx_catalog` | `altryxtodatabricks_accelarator_01` | Unity Catalog catalog |
| `dbx_schema` | `flip_local_test` | Unity Catalog schema |
| `dbx_job_id` | `123456789` | a Databricks job id (from *List jobs*) |
| `fab_tenant_id` | Entra tenant id | Fabric service principal |
| `fab_client_id` | service principal application id | Fabric service principal |
| `fab_client_secret` | service principal client secret | Fabric service principal |
| `fab_workspace` | `Alteryx_Fabric` | Fabric workspace **name or id** |
| `fab_lakehouse` | `Src_LK` | destination lakehouse (existing or created) |
| `job_id` | `mig_0123456789abcdef0123` | a migration id (from a `202` response) |

Mark the secret variables as type *secret* in Postman. Keep credentials in the environment, not in
a shared collection: the service never stores them, but a collection export would.

### Credentials (always in the JSON body)

| System | What the body carries | Rule |
|---|---|---|
| Databricks | `{"host", "client_id", "client_secret", "personal_access_token"}` | If `client_id` **and** `client_secret` are non-empty, the service principal (OAuth M2M) is used and the token is ignored. Otherwise the token is used. Empty strings count as "not sent", so the same body works for both: **fill either `dbx_client_id` + `dbx_client_secret`, or `dbx_token`, and leave the other(s) empty.** Only one of id / secret filled → `422`. |
| Fabric | `{"tenant_id", "client_id", "client_secret"}` | Service principal only; all three required. |

Every variable used in a request must exist in the environment (an undefined `{{var}}` is sent
literally). Unused ones stay defined with an empty value.

### Save the migration id automatically (optional)

On any request that starts a migration (`POST /migrations`, `/upload`, `/deploy`), open the
*Scripts → Post-response* (older Postman: *Tests*) tab and paste:

```javascript
const body = pm.response.json();
if (body.id) { pm.environment.set("job_id", body.id); }
```

### Responses

- Errors always look like `{"error": {"code", "message", "field"}}`; `field` names the bad input.
- Interactive docs with the same endpoints: `{{baseUrl}}/docs`.
- Browse lists return `{count, items, selection_key, id_field, truncated, warnings}`: put
  `item[id_field]` into `source.selection.<selection_key>` of *Start a migration*.

Contents: [1 Health](#1-health) · [2 Connection checks](#2-connection-checks) ·
[3 Browse Databricks](#3-browse-databricks) · [4 Browse Fabric](#4-browse-fabric) ·
[5 Start a migration](#5-start-a-migration) · [6 Follow and inspect](#6-follow-and-inspect-a-migration) ·
[7 Deploy, validate, record](#7-deploy-validate-record) · [8 Convert one asset](#8-convert-one-asset-synchronous) ·
[9 Cancel and delete](#9-cancel-and-delete) · [Recipe](#recipe-browse--pick--migrate--check) ·
[Status codes](#status-codes)

---

## 1. Health

No API key needed.

#### Liveness

```bash
curl --location --request GET '{{baseUrl}}/healthz'
```

#### Readiness (output folder writable, LLM configured?)

```bash
curl --location --request GET '{{baseUrl}}/readyz'
```

#### Prometheus metrics

```bash
curl --location --request GET '{{baseUrl}}/metrics'
```

---

## 2. Connection checks

Validate credentials without starting anything.

#### Test Databricks credentials

Response: `{"ok": true, "host": ..., "auth_type": "service_principal" | "personal_access_token", "user": ...}` - `auth_type` shows which method was used.

```bash
curl --location --request POST '{{baseUrl}}/api/v1/connections/databricks/test' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  }
}'
```

#### Test Fabric service principal + workspace

Use `"workspace_id"` instead of `"workspace_name"` if `fab_workspace` holds a GUID.

```bash
curl --location --request POST '{{baseUrl}}/api/v1/connections/fabric/test' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "tenant_id": "{{fab_tenant_id}}",
    "client_id": "{{fab_client_id}}",
    "client_secret": "{{fab_client_secret}}"
  },
  "workspace_name": "{{fab_workspace}}"
}'
```

#### Test the LLM (Claude or Azure OpenAI, as configured on the server)

```bash
curl --location --request POST '{{baseUrl}}/api/v1/connections/llm/test' \
--header 'X-API-Key: {{apiKey}}'
```

---

## 3. Browse Databricks

Read-only listings. They are `POST` only so that the credentials travel in the body; filters are
body fields, path segments name a resource.

#### Workspace folder (one level: folders, repos, notebooks, files)

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/workspace' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  },
  "path": "{{dbx_path}}"
}'
```

#### Notebooks under folders (recursive)

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/notebooks' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  },
  "paths": [
    "{{dbx_path}}"
  ],
  "limit": 1000
}'
```

#### List jobs

`name` is an optional case-insensitive filter, e.g. `"name": "nightly"`.

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/jobs' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  },
  "name": null,
  "limit": 1000
}'
```

#### One job (full definition: tasks, schedule, parameters)

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/jobs/{{dbx_job_id}}' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  }
}'
```

#### Clusters

All-purpose clusters; `true` adds job/pipeline clusters.

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/clusters' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  },
  "include_job_clusters": false
}'
```

#### Cluster libraries (they migrate with their cluster: select the cluster id)

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/libraries' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  }
}'
```

#### DLT / Lakeflow pipelines

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/pipelines' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  },
  "name": null
}'
```

#### SQL warehouses (also pick one as source.warehouse_id for copy_data)

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/warehouses' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  }
}'
```

#### Unity Catalog: catalogs

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/catalogs' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  },
  "include_system": false
}'
```

#### Unity Catalog: schemas of a catalog

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/catalogs/{{dbx_catalog}}/schemas' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  }
}'
```

#### Unity Catalog: tables and views of a schema

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/catalogs/{{dbx_catalog}}/schemas/{{dbx_schema}}/tables' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  }
}'
```

#### Unity Catalog: volumes of a schema

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/catalogs/{{dbx_catalog}}/schemas/{{dbx_schema}}/volumes' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  }
}'
```

#### Unity Catalog: functions of a schema

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/catalogs/{{dbx_catalog}}/schemas/{{dbx_schema}}/functions' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  }
}'
```

#### Unity Catalog: registered ML models of a schema

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/catalogs/{{dbx_catalog}}/schemas/{{dbx_schema}}/models' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  }
}'
```

#### Secret scopes and key names (values are never readable)

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/secret-scopes' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  }
}'
```

#### SQL queries

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/queries' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  },
  "name": null
}'
```

#### SQL alerts

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/alerts' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  },
  "name": null
}'
```

#### AI/BI (Lakeview) dashboards

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/dashboards' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  },
  "name": null
}'
```

#### Model serving endpoints

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/serving-endpoints' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  },
  "name": null
}'
```

#### Git folders (repos)

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/repos' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  },
  "name": null
}'
```

#### Unity Catalog external locations

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/external-locations' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  },
  "name": null
}'
```

#### Lakehouse Federation connections (endpoint options only)

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/connections' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  },
  "name": null
}'
```

#### Workspace files (non-notebook files under folders)

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/workspace-files' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  },
  "paths": [
    "{{dbx_path}}"
  ],
  "limit": 1000
}'
```

#### Everything at once (assessment format)

Every `include_*` flag defaults to `true`; set the ones you do not want to `false`. The response can be sent back as `assessment` in *Start a migration* to skip discovery (5.4).

```bash
curl --location --request POST '{{baseUrl}}/api/v1/databricks/inventory' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  },
  "workspace_paths": [
    "{{dbx_path}}"
  ],
  "catalogs": [
    "{{dbx_catalog}}"
  ],
  "schemas": [
    "{{dbx_schema}}"
  ],
  "include_jobs": true,
  "include_tables": true,
  "include_secret_scopes": true,
  "include_clusters": true,
  "include_pipelines": true,
  "include_warehouses": true,
  "include_volumes": true,
  "include_functions": true,
  "include_models": true,
  "include_sql_assets": true,
  "include_external_locations": true,
  "include_connections": true,
  "include_serving_endpoints": true,
  "include_repos": true,
  "include_libraries": true,
  "include_workspace_files": true
}'
```

---

## 4. Browse Fabric

Pick the target. `{{fab_workspace}}` in a path can be the workspace name or its id.

#### Workspaces visible to the service principal

```bash
curl --location --request POST '{{baseUrl}}/api/v1/fabric/workspaces' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "tenant_id": "{{fab_tenant_id}}",
    "client_id": "{{fab_client_id}}",
    "client_secret": "{{fab_client_secret}}"
  },
  "name": null
}'
```

#### Lakehouses of a workspace (schemas_enabled, OneLake paths, SQL endpoint)

```bash
curl --location --request POST '{{baseUrl}}/api/v1/fabric/workspaces/{{fab_workspace}}/lakehouses' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "tenant_id": "{{fab_tenant_id}}",
    "client_id": "{{fab_client_id}}",
    "client_secret": "{{fab_client_secret}}"
  }
}'
```

#### Items in a workspace

`type`: `Notebook`, `DataPipeline`, `Lakehouse`, `Warehouse`, `Environment`, ... or `null` for all.

```bash
curl --location --request POST '{{baseUrl}}/api/v1/fabric/workspaces/{{fab_workspace}}/items' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "credentials": {
    "tenant_id": "{{fab_tenant_id}}",
    "client_id": "{{fab_client_id}}",
    "client_secret": "{{fab_client_secret}}"
  },
  "type": "Notebook"
}'
```

---

## 5. Start a migration

All return `202 {"id": ..., "links": {...}}` (save the id in `job_id`).

### 5.1 Live Databricks

#### Selected objects only (recommended)

Ids and paths come from section 3. Only these objects are migrated, each fetched directly (no crawl). A folder path selects every notebook under it; `catalog.schema` selects every table / volume / function of the schema. Remove `jobs` if `dbx_job_id` is not set.

```bash
curl --location --request POST '{{baseUrl}}/api/v1/migrations' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "source": {
    "type": "databricks",
    "credentials": {
      "host": "{{dbx_host}}",
      "client_id": "{{dbx_client_id}}",
      "client_secret": "{{dbx_client_secret}}",
      "personal_access_token": "{{dbx_token}}"
    },
    "selection": {
      "notebooks": [
        "{{dbx_path}}"
      ],
      "jobs": [
        "{{dbx_job_id}}"
      ],
      "clusters": [],
      "pipelines": [],
      "warehouses": [],
      "tables": [
        "{{dbx_catalog}}.{{dbx_schema}}"
      ],
      "secret_scopes": [],
      "volumes": [
        "{{dbx_catalog}}.{{dbx_schema}}"
      ],
      "functions": [
        "{{dbx_catalog}}.{{dbx_schema}}"
      ],
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
  "mapping": {
    "keyvault_url_default": "https://your-vault.vault.azure.net/"
  }
}'
```

#### Everything under folders / catalogs (crawl)

Omitted `include_*` flags default to `true` (see the inventory request in section 3 for the full list).

```bash
curl --location --request POST '{{baseUrl}}/api/v1/migrations' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "source": {
    "type": "databricks",
    "credentials": {
      "host": "{{dbx_host}}",
      "client_id": "{{dbx_client_id}}",
      "client_secret": "{{dbx_client_secret}}",
      "personal_access_token": "{{dbx_token}}"
    },
    "workspace_paths": [
      "{{dbx_path}}"
    ],
    "catalogs": [
      "{{dbx_catalog}}"
    ],
    "schemas": [
      "{{dbx_schema}}"
    ],
    "include_jobs": true,
    "include_tables": true,
    "include_volumes": true,
    "include_functions": true,
    "include_clusters": true,
    "include_libraries": true
  }
}'
```

### 5.2 Full migration: code + deploy + data

#### Convert, deploy and copy data to a named workspace and lakehouse

Deploys the Environment, notebooks and pipelines; copies volume files and table data; validates
row counts; creates views. Needs a Databricks SQL warehouse (`warehouse_id`: `null` = first available).
Other `options`: `{"deploy": true, "deploy_only": ["notebook"]}` (smoke test) ·
`{"deploy": true, "force_deploy": true}` (ignore preflight errors) · `{"ai_fix": true}` (LLM rewrites TODO cells).

```bash
curl --location --request POST '{{baseUrl}}/api/v1/migrations' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "source": {
    "type": "databricks",
    "credentials": {
      "host": "{{dbx_host}}",
      "client_id": "{{dbx_client_id}}",
      "client_secret": "{{dbx_client_secret}}",
      "personal_access_token": "{{dbx_token}}"
    },
    "selection": {
      "notebooks": [
        "{{dbx_path}}"
      ],
      "tables": [
        "{{dbx_catalog}}.{{dbx_schema}}"
      ],
      "volumes": [
        "{{dbx_catalog}}.{{dbx_schema}}"
      ]
    },
    "warehouse_id": null
  },
  "target": {
    "workspace_name": "{{fab_workspace}}",
    "lakehouse": "{{fab_lakehouse}}",
    "credentials": {
      "tenant_id": "{{fab_tenant_id}}",
      "client_id": "{{fab_client_id}}",
      "client_secret": "{{fab_client_secret}}"
    }
  },
  "options": {
    "deploy": true,
    "copy_data": true
  }
}'
```

### 5.3 Agent mode

#### The LLM drives the stages

```bash
curl --location --request POST '{{baseUrl}}/api/v1/migrations' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "source": {
    "type": "databricks",
    "credentials": {
      "host": "{{dbx_host}}",
      "client_id": "{{dbx_client_id}}",
      "client_secret": "{{dbx_client_secret}}",
      "personal_access_token": "{{dbx_token}}"
    },
    "selection": {
      "notebooks": [
        "{{dbx_path}}"
      ]
    }
  },
  "options": {
    "mode": "agent",
    "agent_instructions": "Keep behaviour identical; explain every gap in a comment."
  }
}'
```

### 5.4 With a saved inventory (no discovery)

Run *Everything at once* (section 3), copy its whole response, and paste it as the value of
`assessment` (replace the example object below).

#### Migrate a saved inventory

```bash
curl --location --request POST '{{baseUrl}}/api/v1/migrations' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "source": {
    "type": "databricks",
    "credentials": {
      "host": "{{dbx_host}}",
      "client_id": "{{dbx_client_id}}",
      "client_secret": "{{dbx_client_secret}}",
      "personal_access_token": "{{dbx_token}}"
    }
  },
  "assessment": {
    "assets": [
      {
        "type": "Notebook",
        "name": "load_sales",
        "location": "/Workspace/Shared/load_sales",
        "meta": {
          "source_path": "/Workspace/Shared/load_sales",
          "language": "PYTHON"
        }
      }
    ]
  }
}'
```

### 5.5 Offline notebooks (no Databricks credentials)

Multipart upload. After importing, open the *Body → form-data* tab and **pick the files again**
with *Select Files* (Postman cannot carry local file paths across machines). `files` can repeat:
notebooks (`.py .sql .scala .r .ipynb`), a `.zip` export, `assessment.json`, `mapping.json`.
`config` is a JSON string.

#### Upload notebooks

The repository has sample notebooks in `tests/fixtures/notebooks/`.

```bash
curl --location --request POST '{{baseUrl}}/api/v1/migrations/upload' \
--header 'X-API-Key: {{apiKey}}' \
--form 'files=@"/path/to/load_sales.py"' \
--form 'files=@"/path/to/explore.ipynb"' \
--form 'config={"mapping": {"catalog_to_lakehouse": {"main": "lh_sales"}, "mount_map": {"/mnt/raw": "Files/raw"}}}'
```

#### Upload a zip export

```bash
curl --location --request POST '{{baseUrl}}/api/v1/migrations/upload' \
--header 'X-API-Key: {{apiKey}}' \
--form 'files=@"/path/to/notebooks.zip"' \
--form 'config={}'
```

#### Upload notebooks + assessment.json from the assessor

```bash
curl --location --request POST '{{baseUrl}}/api/v1/migrations/upload' \
--header 'X-API-Key: {{apiKey}}' \
--form 'files=@"/path/to/assessment.json"' \
--form 'files=@"/path/to/notebooks.zip"' \
--form 'config={"mapping": {"staging_storage": "abfss://stage@youraccount.dfs.core.windows.net/dbx_export"}}'
```

#### Upload + deploy to Fabric

```bash
curl --location --request POST '{{baseUrl}}/api/v1/migrations/upload' \
--header 'X-API-Key: {{apiKey}}' \
--form 'files=@"/path/to/notebooks.zip"' \
--form 'config={"target": {"workspace_name": "{{fab_workspace}}", "lakehouse": "{{fab_lakehouse}}", "credentials": {"tenant_id": "{{fab_tenant_id}}", "client_id": "{{fab_client_id}}", "client_secret": "{{fab_client_secret}}"}}, "options": {"deploy": true}}'
```

#### Upload + deploy + data copy (Databricks credentials only to read tables)

```bash
curl --location --request POST '{{baseUrl}}/api/v1/migrations/upload' \
--header 'X-API-Key: {{apiKey}}' \
--form 'files=@"/path/to/notebooks.zip"' \
--form 'config={"databricks": {"host": "{{dbx_host}}", "client_id": "{{dbx_client_id}}", "client_secret": "{{dbx_client_secret}}", "personal_access_token": "{{dbx_token}}"}, "target": {"workspace_name": "{{fab_workspace}}", "lakehouse": "{{fab_lakehouse}}", "credentials": {"tenant_id": "{{fab_tenant_id}}", "client_id": "{{fab_client_id}}", "client_secret": "{{fab_client_secret}}"}}, "options": {"deploy": true, "copy_data": true}}'
```

---

## 6. Follow and inspect a migration

#### List migrations (newest first)

```bash
curl --location --request GET '{{baseUrl}}/api/v1/migrations?limit=20' \
--header 'X-API-Key: {{apiKey}}'
```

#### Get one migration (poll this)

`status`, `stage`, inventory, summary, preflight counts, `data_copy`, agent, error. `options.databricks_auth` shows which Databricks method was used (never the secret).

```bash
curl --location --request GET '{{baseUrl}}/api/v1/migrations/{{job_id}}' \
--header 'X-API-Key: {{apiKey}}'
```

#### Ledger items

```bash
curl --location --request GET '{{baseUrl}}/api/v1/migrations/{{job_id}}/items' \
--header 'X-API-Key: {{apiKey}}'
```

#### Ledger items filtered

`outcome`: `migrated`, `needs_testing`, `partial`, `not_migrated`. `type`: `Notebook`, `Job`, `Table/View`, `Volume`, `Library`, ... (either can be left out).

```bash
curl --location --request GET '{{baseUrl}}/api/v1/migrations/{{job_id}}/items?outcome=partial&type=Notebook' \
--header 'X-API-Key: {{apiKey}}'
```

#### Preflight (deploy blockers and warnings)

```bash
curl --location --request GET '{{baseUrl}}/api/v1/migrations/{{job_id}}/preflight' \
--header 'X-API-Key: {{apiKey}}'
```

#### HTML report

Use *Send and Download* to save the file.

```bash
curl --location --request GET '{{baseUrl}}/api/v1/migrations/{{job_id}}/report' \
--header 'X-API-Key: {{apiKey}}'
```

#### CSV report

Use *Send and Download* to save the file.

```bash
curl --location --request GET '{{baseUrl}}/api/v1/migrations/{{job_id}}/report.csv' \
--header 'X-API-Key: {{apiKey}}'
```

#### Runbook (Markdown)

```bash
curl --location --request GET '{{baseUrl}}/api/v1/migrations/{{job_id}}/runbook' \
--header 'X-API-Key: {{apiKey}}'
```

#### Agent steps (mode=agent)

```bash
curl --location --request GET '{{baseUrl}}/api/v1/migrations/{{job_id}}/agent-trace' \
--header 'X-API-Key: {{apiKey}}'
```

#### List generated files

```bash
curl --location --request GET '{{baseUrl}}/api/v1/migrations/{{job_id}}/artifacts' \
--header 'X-API-Key: {{apiKey}}'
```

#### Fetch one generated file

Any path from *List generated files*, e.g. `RUNBOOK.md`, `ledger.json`, `artifacts/pipelines/pl_nightly.json`.

```bash
curl --location --request GET '{{baseUrl}}/api/v1/migrations/{{job_id}}/artifacts/artifacts/notebooks/load_sales.ipynb' \
--header 'X-API-Key: {{apiKey}}'
```

#### Download everything as a zip

Use *Send and Download*.

```bash
curl --location --request GET '{{baseUrl}}/api/v1/migrations/{{job_id}}/download' \
--header 'X-API-Key: {{apiKey}}'
```

---

## 7. Deploy, validate, record

One at a time: while a deploy runs, validation / results calls return `409`.

#### Deploy (or re-deploy) a finished migration

`only` picks the kinds to deploy (`[]` = all). Credentials are needed again: they are never stored.

```bash
curl --location --request POST '{{baseUrl}}/api/v1/migrations/{{job_id}}/deploy' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "target": {
    "workspace_name": "{{fab_workspace}}",
    "lakehouse": "{{fab_lakehouse}}",
    "credentials": {
      "tenant_id": "{{fab_tenant_id}}",
      "client_id": "{{fab_client_id}}",
      "client_secret": "{{fab_client_secret}}"
    }
  },
  "only": [
    "environment",
    "notebook",
    "shortcuts",
    "pipeline"
  ],
  "force": false
}'
```

#### Deploy everything + copy data (files and tables)

```bash
curl --location --request POST '{{baseUrl}}/api/v1/migrations/{{job_id}}/deploy' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "target": {
    "workspace_name": "{{fab_workspace}}",
    "lakehouse": "{{fab_lakehouse}}",
    "credentials": {
      "tenant_id": "{{fab_tenant_id}}",
      "client_id": "{{fab_client_id}}",
      "client_secret": "{{fab_client_secret}}"
    }
  },
  "only": [],
  "copy_data": true,
  "databricks": {
    "host": "{{dbx_host}}",
    "client_id": "{{dbx_client_id}}",
    "client_secret": "{{dbx_client_secret}}",
    "personal_access_token": "{{dbx_token}}"
  },
  "warehouse_id": null
}'
```

#### Import validation results

`results.json` is written by the generated `validate_migration` notebook in Fabric. Re-select the file in *Body → form-data* after importing.

```bash
curl --location --request POST '{{baseUrl}}/api/v1/migrations/{{job_id}}/validation' \
--header 'X-API-Key: {{apiKey}}' \
--form 'file=@"/path/to/results.json"'
```

#### Record manual / AI work done outside the service

`item_id` comes from *Ledger items*; `outcome`: `migrated`, `needs_testing`, `partial`, `not_migrated`.

```bash
curl --location --request POST '{{baseUrl}}/api/v1/migrations/{{job_id}}/results' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "item_id": "Dashboard:Sales KPIs",
  "outcome": "needs_testing",
  "note": "Rebuilt as a Power BI report",
  "target_id": null
}'
```

---

## 8. Convert one asset (synchronous)

Nothing is stored and no credentials are needed.

#### Notebook → JSON report

Re-select the file in *Body → form-data* after importing.

```bash
curl --location --request POST '{{baseUrl}}/api/v1/convert/notebook' \
--header 'X-API-Key: {{apiKey}}' \
--form 'file=@"/path/to/load_sales.py"' \
--form 'mapping={"catalog_to_lakehouse": {"main": "lh_sales"}, "mount_map": {"/mnt/raw": "Files/raw"}}' \
--form 'catalogs=["main"]'
```

#### Notebook → Fabric .ipynb file

Use *Send and Download*.

```bash
curl --location --request POST '{{baseUrl}}/api/v1/convert/notebook' \
--header 'X-API-Key: {{apiKey}}' \
--form 'file=@"/path/to/explore.ipynb"' \
--form 'download=true'
```

#### Databricks job settings → Fabric Data Pipeline + schedule

`settings` is a job's `settings` object as returned by *One job*.

```bash
curl --location --request POST '{{baseUrl}}/api/v1/convert/job' \
--header 'X-API-Key: {{apiKey}}' \
--header 'Content-Type: application/json' \
--data-raw '{
  "name": "nightly_sales",
  "settings": {
    "schedule": {
      "quartz_cron_expression": "0 30 2 * * ?",
      "timezone_id": "Asia/Kolkata",
      "pause_status": "UNPAUSED"
    },
    "tasks": [
      {
        "task_key": "load",
        "notebook_task": {
          "notebook_path": "/Repos/team/etl/load_sales",
          "base_parameters": {
            "env": "prod"
          }
        },
        "timeout_seconds": 3600,
        "max_retries": 2
      },
      {
        "task_key": "publish",
        "depends_on": [
          {
            "task_key": "load"
          }
        ],
        "notebook_task": {
          "notebook_path": "/Repos/team/etl/publish"
        }
      }
    ]
  },
  "mapping": {}
}'
```

---

## 9. Cancel and delete

#### Cancel a queued / running migration

Stops at the next stage boundary.

```bash
curl --location --request POST '{{baseUrl}}/api/v1/migrations/{{job_id}}/cancel' \
--header 'X-API-Key: {{apiKey}}'
```

#### Delete a finished migration and its files

Returns `204`.

```bash
curl --location --request DELETE '{{baseUrl}}/api/v1/migrations/{{job_id}}' \
--header 'X-API-Key: {{apiKey}}'
```

---

## Recipe: browse → pick → migrate → check

1. *Test Databricks credentials* and *Test Fabric service principal + workspace* (section 2).
2. *Notebooks under folders*, *Unity Catalog: tables of a schema*, *List jobs* (section 3): note
   the ids / paths you want.
3. *Lakehouses of a workspace* (section 4): pick `fab_lakehouse`.
4. *Convert, deploy and copy data* (5.2) with your selection; save `job_id`.
5. *Get one migration* until `status` is `succeeded`; read `summary` and `data_copy`.
6. *Ledger items filtered* with `outcome=partial` / `not_migrated` for what still needs work,
   *Download everything as a zip* for the generated artifacts and the runbook.

## Status codes

| Code | Meaning |
|---|---|
| 200 / 202 / 204 | OK / job accepted / deleted |
| 400 | malformed request; `field` names the invalid field |
| 401 | missing or invalid `X-API-Key` |
| 404 | unknown migration, file or item |
| 405 | wrong method (browse endpoints are `POST`) |
| 409 | the migration is running (or already finished, for cancel) |
| 413 | upload too large (`MAX_UPLOAD_MB`) |
| 422 | validation: missing credentials, half a Databricks service principal, bad tenant id, private Databricks host, unknown mapping key, preflight errors |
| 502 | Databricks / Fabric / Entra ID returned an error (message included) |
| 503 | agent mode / ai_fix / LLM test without a configured LLM |
