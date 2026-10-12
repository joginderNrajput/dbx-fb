# ADR 0003 — Faithful port of dbx2fabric_migrate_v4.py, held by parity tests

Status: accepted

Context: `dbx2fabric_migrate_v4.py` is the single source of truth for conversion behaviour
(spec §1.2). A service rewrite risks silently changing generated notebooks.

Decision: port the conversion and migration logic line by line into `app/conversion` and
`app/migration`, keep the original file verbatim at the root, and run both on the same fixtures
in `tests/unit/conversion/test_parity.py`. Deliberate deviations (the complete list):

1. Source code comes from an injected lookup (uploads, then the Databricks export API) instead of
   CLI flags. The "source not available" message names the upload/credentials options.
2. Catalogs named in `mapping.catalog_to_lakehouse` are rewritten even without a table inventory,
   so offline notebook uploads get lakehouse names.
3. dbt project folders must be inside the uploaded sources; the service never reads arbitrary
   server paths.
4. Texts that named the CLI (runbook, validation notebook, AI work orders) name the API endpoints.
5. `.ipynb` uploads detect their language from notebook metadata.
6. A table/volume location that is not ADLS Gen2 fails with a clear message instead of a Python
   unpacking error (same outcome: not migrated).
7. Deploy errors are typed (`FabricError` with HTTP status), so "already exists" (409) is detected
   by status code instead of string matching.

8. Additions that do not change generated artifacts: deploy warns when a target lakehouse is not
   schema-enabled, and `options.copy_data` (spec §5.9) moves table data, which the reference
   tool left to generated notebooks.

Consequences: any behaviour change must update the parity test or this list.
