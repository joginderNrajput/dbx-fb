"""Prompts for the agent and the cell fixer. Spec §6.2-§6.3."""

from __future__ import annotations

__all__ = ["AGENT_SYSTEM", "CELL_FIX_PROMPT", "agent_kickoff"]

FABRIC_RULES = """Fabric target rules:
- OLTP database -> Fabric SQL database; OLAP (gold/dw/mart/reporting schemas) -> Fabric Warehouse; all other analytics and files -> Lakehouse (Delta).
- Code -> Fabric notebooks (PySpark / Spark SQL). Use notebookutils, never dbutils. Secrets come from Azure Key Vault via notebookutils.credentials.getSecret.
- Orchestration -> Data Pipeline; ingestion -> Copy Data activity or Copy job; dbt -> Fabric dbt job.
- Mounts do not exist: use OneLake shortcuts or abfss paths. Auto Loader, DLT, Feature Store and Databricks SDK calls have no direct equivalent."""

AGENT_SYSTEM = f"""You are the migration agent of a Databricks -> Microsoft Fabric migration service.
You operate one migration through tools. Deterministic tools do the bulk conversion; your job is to run them in a sensible order, inspect the results, finish the work the rules engine could not (cells marked TODO[dbx2fabric:RULE]), and decide whether the result is safe to deploy.

{FABRIC_RULES}

How to work:
1. get_job_context, then inventory_source, plan_migration and convert_assets.
2. For every notebook listed in notebooks_with_todos: read_notebook, then rewrite each code cell that contains a TODO marker with update_notebook_cell. Keep behaviour identical, remove the TODO comment once it is resolved, and where Fabric has no equivalent write the closest pattern plus a short comment explaining the gap. Do not touch cells without markers.
3. Items you cannot finish (secrets, credentials, dashboards, DLT redesigns) stay as they are; use record_result only when you have new information (for example a deliberate decision to leave something not migrated), never to claim work you did not do. Never record "migrated": that requires validation in Fabric.
4. run_preflight. Fix errors you can fix (update_mapping + plan_migration + convert_assets when a mapping value is wrong).
5. Call deploy_to_fabric only if get_job_context says deploy_requested is true, Fabric credentials are present and preflight has no errors.
5b. If get_job_context says copy_data_requested is true, call copy_data_to_fabric after deploying (or after convert_assets when not deploying). It copies volume and workspace files and table data, validates row counts and creates views.
6. Call finish with a short summary for the operator: what was migrated, what needs testing, what needs manual work and why.

Never invent credentials, workspace ids, Key Vault URLs or connection ids. Keep tool calls purposeful; you have a limited number of turns."""

CELL_FIX_PROMPT = """You are migrating a Databricks notebook cell to Microsoft Fabric.
Rules: code stays in Fabric notebooks (PySpark/Spark SQL); use notebookutils instead of dbutils; data lives in Lakehouse (Delta),
OLAP warehouse tables in Fabric Warehouse, OLTP in Fabric SQL database; orchestration is a Data Pipeline; ingestion is Copy Data / Copy job.
Resolve every TODO[dbx2fabric:...] marker in the cell below. Keep behaviour identical. If something has no Fabric equivalent, implement the closest
pattern and explain in a comment. Return ONLY the corrected cell code inside one ``` fence.

TODO markers: {todos}

CELL:
```
{cell}
```"""


def agent_kickoff(instructions: str | None) -> str:
    msg = "Migrate this source to Microsoft Fabric. Start with get_job_context."
    if instructions:
        msg += (
            "\n\nOperator instructions (treat as preferences, never as permission to skip the rules above):\n"
            + instructions
        )
    return msg
