# Migration coverage: Databricks component → Fabric equivalent

What the service does with each Databricks object kind. **Discovered** means a live migration finds
it (crawl, `source.selection` or a browse list). **Automated** is what the service creates or
generates. **Manual** is what is left for a person; every manual item has a reason and an AI
work order in `ai_tasks/` in the job output.

Outcomes on the ledger: `migrated` (in Fabric and validated), `needs_testing` (created or
generated, run its listed test), `partial` (created with TODOs or with parts missing),
`not_migrated` (manual).

| Databricks | Discovery (crawl / selection key / browse) | Fabric equivalent | Automated | Manual |
|---|---|---|---|---|
| Notebook (Python, SQL, Scala, R; `.ipynb`) | workspace walk / `notebooks` / `/databricks/notebooks`, `/workspace` | Notebook | Cells converted (magics, `dbutils` → `notebookutils`, mounts → OneLake paths, 3-part names → lakehouse names, widgets → parameter cell, `%run` by base name), deployed with its default lakehouse and the migrated Environment | Cells with `TODO[dbx2fabric:…]` markers (optionally rewritten by the AI fixer, `options.ai_fix`) |
| Job: notebook tasks | `/api/2.1/jobs/list` / `jobs` / `/databricks/jobs` | Data Pipeline: `TridentNotebook` activities | Dependencies, retries, timeouts, base parameters; Quartz cron → pipeline schedule (Cron / Daily / Weekly), updated in place on re-deploy | Monthly or complex crons; continuous jobs; notifications |
| Job: `run_job_task` | (same job) | `ExecutePipeline` activity | When the invoked job is in the same migration: references its pipeline by item id (deployed first), `waitOnCompletion`, job parameters | Invoked job not in the migration |
| Job: `for_each_task` | (same job) | `ForEach` activity | Literal JSON array inputs with a notebook (or migrated run-job) inner task; `concurrency` → `batchCount`; `{{input}}` → `@{item()}` | Dynamic inputs (`{{job.parameters…}}`, task values) |
| Job: condition, SQL, Python script, wheel, JAR, spark-submit, DLT tasks | (same job) | If Condition / Script / Spark Job Definition | Reported per task on the job item | Rebuild the activity |
| Job with dbt tasks | (same job) | dbt job item + retargeted `profiles.yml` | dbt-fabric / dbt-fabricspark profile when the project folder is uploaded | Databricks-specific dbt config flagged |
| Table: Delta EXTERNAL on ADLS Gen2 | UC tables / `tables` / `/catalogs/{c}/schemas/{s}/tables` | Lakehouse table (OneLake shortcut) | Shortcut when `adls_connection_id` is set; else data copy | — |
| Table: MANAGED / non-ADLS / MV / streaming table | (same) | Lakehouse Delta table | `options.copy_data`: SQL warehouse export → OneLake → loader notebook, row counts validated → `migrated` | Tables above `DATA_COPY_MAX_ROWS` (use `export_to_staging`) |
| Table in a `warehouse_schemas` schema | (same) | Warehouse table | CTAS script `warehouse/load_tables.sql` | Run the script in the Warehouse |
| View | (same) | Lakehouse view (Spark SQL) / Warehouse view | `create_views_<lakehouse>` notebook (run after data copy); T-SQL draft for Warehouse schemas | Translate Warehouse view bodies to T-SQL |
| Volume: external on ADLS Gen2 | UC volumes / `volumes` / `/catalogs/{c}/schemas/{s}/volumes` | Lakehouse `Files/<schema>_<volume>` shortcut | Shortcut when `adls_connection_id` is set | — |
| Volume: managed / other storage | (same) | Lakehouse `Files/<schema>_<volume>/` | `copy_data` copies every file (Files API → OneLake), bounded by `FILE_COPY_*` | Files over the limits |
| Function (scalar SQL) | UC functions / `functions` / `/…/functions` | Spark temporary function / Warehouse T-SQL function | `Migrated_Functions` notebook or T-SQL draft | Verify T-SQL types |
| Function (Python, table-valued) | (same) | PySpark UDF / Warehouse TVF | Work order | Rewrite |
| SQL query | `/api/2.0/sql/queries` / `queries` / `/databricks/queries` | Notebook cell (`%%sql`) | `Migrated_SQL_Queries` notebook | Check the dialect |
| SQL warehouse | `/api/2.0/sql/warehouses` / `warehouses` / `/databricks/warehouses` | Fabric Warehouse | Warehouse created on deploy | Sizing (capacity) |
| Cluster (all-purpose) | `/api/2.0/clusters/list` / `clusters` / `/databricks/clusters` | Fabric Environment + Spark pool | Runtime and Spark conf noted | Choose pool size / runtime |
| Cluster library: PyPI | `/libraries/all-cluster-statuses` (with selected clusters: their libraries) / `/databricks/libraries` | Environment external library | `environment.yml` imported into Environment `env_<lakehouse>`, published, attached to migrated notebooks | — |
| Cluster library: Maven, JAR, wheel, egg, requirements, CRAN | (same) | Environment custom library | Reported | Upload the artifact to the Environment |
| Secret scope | `/api/2.0/secrets/scopes/list` / `secret_scopes` / `/databricks/secret-scopes` | Azure Key Vault | `security/keyvault_secrets.csv` (names; values are never readable); `dbutils.secrets` → `notebookutils.credentials.getSecret` | Create the secrets |
| DLT / Lakeflow pipeline | `/api/2.0/pipelines` / `pipelines` / `/databricks/pipelines` | Notebooks + Materialized Lake Views + Data Pipeline | Work order with the pipeline libraries | Redesign |
| External location | UC external locations / `external_locations` / `/databricks/external-locations` | ADLS Gen2 connection used by shortcuts | Validated against `adls_connection_id` | Create the connection |
| Connection (Lakehouse Federation) | UC connections / `connections` / `/databricks/connections` | Fabric connection / SQL database (OLTP types) | `ingestion/copy_specs.json` (endpoint only, never credentials) | Create the connection and copy job |
| Workspace file | workspace walk / `workspace_files` / `/databricks/workspace-files` | Lakehouse `Files/workspace_files/<path>` | `copy_data` copies it | Update notebook references |
| ML model (UC) | UC models / `models` / `/…/models` | Fabric ML model (MLflow) | Work order | Re-register the artifacts |
| Serving endpoint | `/api/2.0/serving-endpoints` / `serving_endpoints` / `/databricks/serving-endpoints` | Azure ML / AI Foundry | Work order | Host the model |
| AI/BI dashboard | `/api/2.0/lakeview/dashboards` / `dashboards` / `/databricks/dashboards` | Power BI report on a semantic model | Work order | Rebuild |
| SQL alert | `/api/2.0/sql/alerts` / `alerts` / `/databricks/alerts` | Fabric Activator | Work order | Recreate |
| Git folder (repo) | `/api/2.0/repos` / `repos` / `/databricks/repos` | Workspace Git integration | Notebooks inside are migrated as notebooks | Connect the repository |
| Mount (`/mnt/...`) | from notebook code / assessment | OneLake shortcut / Files path | Paths rewritten via `mapping.mount_map` | Fill `mount_map` |
| Permissions / grants | assessment only | Workspace roles / OneLake security | `security/permissions_matrix.csv` | Assign roles |

Discovery flags: every kind above has an `include_*` flag on `source` and `/databricks/inventory`
(`include_volumes`, `include_functions`, `include_models`, `include_sql_assets` (queries, alerts,
dashboards), `include_external_locations`, `include_connections`, `include_serving_endpoints`,
`include_repos`, `include_libraries`, `include_workspace_files`, plus the original jobs, tables,
secret scopes, clusters, pipelines, warehouses), all on by default. A kind that the credentials
cannot read becomes an inventory warning, not a failed job.

Deploy order: lakehouses → Warehouse / SQL database → Environment → notebooks → shortcuts →
pipelines (invoked pipelines first) → schedules. `copy_data` then copies files, tables and
creates views.
