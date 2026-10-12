"""RUNBOOK.md: the ordered steps that take generated artifacts live in Fabric. Spec §3.10."""

from __future__ import annotations

from pathlib import Path
from typing import Any

__all__ = ["write_runbook"]


def write_runbook(out: str | Path, cfg: dict[str, Any], ledger: dict[str, Any]) -> None:
    arts = ledger.get("artifacts", [])

    def kinds(k: str) -> list[str]:
        return [e["name"] for e in arts if e["kind"] == k]

    export = any(e["name"].startswith("databricks/export") for e in arts)
    blocked = sum(
        1
        for it in ledger.get("items", [])
        if any("staging_storage" in r for r in it.get("reasons", []))
    )
    validation = any(e["name"] == "validate_migration" for e in arts)
    copies = [n for n in kinds("notebook") if n.startswith("copy_tables_")]
    steps = [
        "# Runbook: moving this migration into Fabric",
        "",
        "## 0. Prerequisites",
        "- A Fabric workspace on a paid capacity or trial; you are Contributor or above; note the workspace id (the GUID in the workspace URL).",
        "- Tenant setting 'Users can create Fabric items' is on. The service principal (or token) used for deploy is a workspace Contributor.",
        "- Create an Azure Key Vault and give the Fabric users/notebooks read access to secrets.",
        "",
        "## 1. Fill in the mapping",
        "- `workspace_id`, `keyvault_urls`, `mount_map`, `warehouse_schemas`, lakehouse names (send them as `mapping` when you create the migration).",
        "- `adls_connection_id`: create an ADLS Gen2 connection in Fabric (Settings > Manage connections) and paste its id. Needed for shortcuts.",
        "- `staging_storage`: an ADLS Gen2 container you own. Needed for managed tables and volumes."
        + (
            ""
            if export
            else f" **Required for this run: {blocked} managed tables/volumes are blocked until it is set.**"
            if blocked
            else " (not needed for this run)"
        ),
        "",
    ]
    n = 2
    if export:
        steps += [
            f"## {n}. In DATABRICKS: stage managed data",
            "- Run `artifacts/databricks/export_to_staging.py` as a notebook. Give the Fabric identity read access to the staging container.",
            "",
        ]
        n += 1
    steps += [
        f"## {n}. Re-run conversion",
        "Create a new migration (`POST /api/v1/migrations` or `/api/v1/migrations/upload`) with the updated mapping.",
        "",
        f"## {n + 1}. Smoke test the deployment (notebooks only)",
        "`GET /api/v1/migrations/{id}/preflight` first: fix every ERROR; read the warnings.",
        '`POST /api/v1/migrations/{id}/deploy` with `{"only": ["notebook"], "target": {...}}`',
        "- Open one notebook in Fabric: check the default lakehouse is attached, the parameter cell is recognised and `lakehouse.schema.table` names resolve. If anything is off, fix the mapping and re-run before deploying everything.",
        "",
        f"## {n + 2}. Full deployment",
        "`POST /api/v1/migrations/{id}/deploy` with the Fabric target.",
        "Runs preflight first and stops on errors. Creates lakehouses, Warehouse/SQL database items, notebooks, shortcuts, pipelines and schedules. Safe to re-run: existing items are updated.",
        "",
        f"## {n + 3}. Manual steps after deployment",
        "- Create the secrets listed in `artifacts/security/keyvault_secrets.csv` in Key Vault."
        if kinds("csv")
        else "- No secrets to recreate.",
        f"- Run the copy notebooks once, in order: {', '.join(copies) or 'none'}. Each checks its own row counts.",
        "- In the Warehouse SQL editor run `artifacts/warehouse/load_tables.sql` (after the lakehouse tables exist and the notebooks that write gold tables have run)."
        if "warehouse/load_tables.sql" in kinds("script")
        else "- No Warehouse scripts.",
        "- Create a Fabric Environment from `artifacts/environment/environment.yml` and attach it to notebooks that need the libraries."
        if kinds("environment")
        else "- No environment changes.",
        "- Give users access (see `artifacts/security/permissions_matrix.csv`) using workspace roles and OneLake security.",
        "- Enable pipeline schedules only after the first successful manual run.",
        "",
        f"## {n + 4}. Validate",
        *(
            []
            if validation or not blocked
            else [
                "- The validation notebooks are generated only once tables can be copied (set `staging_storage` and re-run convert). The steps below apply after that."
            ]
        ),
        "1. In DATABRICKS run `artifacts/validation/databricks_source_metrics.py`; download `source_metrics.json`.",
        "2. Upload it to the lakehouse under Files/validation/.",
        "3. In FABRIC run the `validate_migration` notebook. It writes Files/validation/results.json; download it.",
        "4. `POST /api/v1/migrations/{id}/validation` with the results.json file - passed tables move to Migrated, failures to Partial.",
        "",
        f"## {n + 5}. Go-live",
        "- Every 'Needs testing' item has been run once; 'Partially migrated' and 'Not migrated' items are done or consciously deferred.",
        "- Run both platforms in parallel for one full cycle and compare outputs before switching consumers.",
        "",
    ]
    (Path(out) / "RUNBOOK.md").write_text("\n".join(steps), "utf-8")
