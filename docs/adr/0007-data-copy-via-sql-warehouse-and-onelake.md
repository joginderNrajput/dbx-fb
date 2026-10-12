# ADR 0007 — Copy table data through a SQL warehouse, OneLake files and a Fabric loader notebook

Status: accepted

Context: "migrate everything" includes table data. Fabric cannot read Databricks-managed (UC
default) storage, and the reference tool's route needs a customer-owned ADLS staging account plus
manual notebook runs (spec §3.7).

Decision: the service moves data itself (spec §5.9): Statement Execution API (JSON_ARRAY, chunked)
→ JSON-lines files in OneLake Files → a generated loader notebook run through the Fabric job
API, which types the columns and writes Delta tables, then validates row counts. Considered and
not chosen:
- a Databricks job writing straight to OneLake: needs cluster/node-type choices per cloud, and
  Fabric credentials inside Databricks;
- the Fabric "Load to Tables" API: unsupported for schema-enabled lakehouses;
- Arrow export: would add pyarrow for a typing problem the loader solves with casts.

Consequences: no staging account or manual step for tables up to `DATA_COPY_MAX_ROWS`. Data
passes through the service one chunk at a time (memory bounded per chunk). Needs a SQL warehouse,
a Fabric capacity that can run notebooks, and a service principal for OneLake. Larger tables keep
the generated export/copy route.
