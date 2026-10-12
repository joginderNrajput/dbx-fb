"""Tools the migration agent may call; each wraps one MigrationSession operation. Spec §6.2."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from app.conversion.common import NOT, PART, TEST
from app.core.exceptions import AppError
from app.migration.session import DEPLOY_KINDS, MigrationSession

__all__ = ["TOOLS", "FinishSignal", "execute_tool"]


def _obj(props: dict[str, Any] | None = None, required: list[str] | None = None) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": props or {},
        "required": required or [],
        "additionalProperties": False,
    }


TOOLS: list[dict[str, Any]] = [
    {
        "name": "get_job_context",
        "description": "What this migration has: source kind, which credentials were supplied, whether deploy was requested, and which stages already ran.",
        "input_schema": _obj(),
    },
    {
        "name": "inventory_source",
        "description": "Inventory the source (Databricks workspace discovery, or the uploaded notebooks / assessment). Returns asset counts by type.",
        "input_schema": _obj(),
    },
    {
        "name": "plan_migration",
        "description": "Build the migration plan: every asset gets a Fabric target and strategy. Run after inventory and after any update_mapping.",
        "input_schema": _obj(),
    },
    {
        "name": "get_mapping",
        "description": "Current mapping (lakehouse names, warehouse schemas, mount map, Key Vault URLs...).",
        "input_schema": _obj(),
    },
    {
        "name": "update_mapping",
        "description": "Override mapping keys (e.g. lakehouse names that fail preflight). Only keys that already exist in the mapping. Requires plan_migration and convert_assets again.",
        "input_schema": _obj(
            {"overrides": {"type": "object", "description": "mapping keys to override"}},
            ["overrides"],
        ),
    },
    {
        "name": "convert_assets",
        "description": "Run the deterministic converter over every planned item. Returns outcome counts and notebooks_with_todos.",
        "input_schema": _obj(),
    },
    {
        "name": "list_items",
        "description": "List ledger items, optionally filtered by outcome (migrated|needs_testing|partial|not_migrated) or asset type.",
        "input_schema": _obj(
            {
                "outcome": {"type": "string"},
                "type": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200},
            }
        ),
    },
    {
        "name": "get_item",
        "description": "Full ledger record of one item (actions, reasons, tests, artifacts).",
        "input_schema": _obj(
            {"id": {"type": "string", "description": "item id, e.g. 'Notebook:load_sales'"}}, ["id"]
        ),
    },
    {
        "name": "read_notebook",
        "description": "Cells of a generated Fabric notebook (by notebook name), with has_todo flags.",
        "input_schema": _obj({"name": {"type": "string"}}, ["name"]),
    },
    {
        "name": "update_notebook_cell",
        "description": "Replace the full source of one code cell in a generated notebook (index from read_notebook). Use it to resolve TODO markers.",
        "input_schema": _obj(
            {
                "name": {"type": "string"},
                "index": {"type": "integer", "minimum": 0},
                "source": {"type": "string"},
                "note": {"type": "string", "description": "one line: what you changed"},
            },
            ["name", "index", "source"],
        ),
    },
    {
        "name": "run_preflight",
        "description": "Static checks on generated artifacts. Errors block deployment.",
        "input_schema": _obj(),
    },
    {
        "name": "deploy_to_fabric",
        "description": "Deploy generated artifacts to the Fabric workspace. Allowed only when deploy was requested, Fabric credentials exist and preflight has no errors.",
        "input_schema": _obj(
            {"only": {"type": "array", "items": {"type": "string", "enum": list(DEPLOY_KINDS)}}}
        ),
    },
    {
        "name": "copy_data_to_fabric",
        "description": "Copy table data from Databricks into the Fabric lakehouse(s) and validate row counts, then create views. "
        "Allowed only when copy_data was requested; run after convert_assets (and after deploy_to_fabric when deploying).",
        "input_schema": _obj(),
    },
    {
        "name": "record_result",
        "description": "Record a decision about one item with a note. Outcomes: needs_testing, partial, not_migrated (migrated requires validation in Fabric).",
        "input_schema": _obj(
            {
                "id": {"type": "string"},
                "outcome": {"type": "string", "enum": [TEST, PART, NOT]},
                "note": {"type": "string"},
            },
            ["id", "outcome", "note"],
        ),
    },
    {
        "name": "finish",
        "description": "End the run with a summary for the operator.",
        "input_schema": _obj({"summary": {"type": "string"}}, ["summary"]),
    },
]
TOOL_NAMES = {t["name"] for t in TOOLS}


class FinishSignal(Exception):
    def __init__(self, summary: str) -> None:
        super().__init__(summary)
        self.summary = summary


def _deploy(s: MigrationSession, inp: dict[str, Any]) -> Any:
    if not s.spec.deploy_requested:
        return {"refused": "deploy was not requested for this migration"}
    if not s.spec.fabric:
        return {"refused": "no Fabric credentials were supplied"}
    issues = s.run_preflight()
    errors = [i for i in issues if i["level"] == "error"]
    if errors:
        return {"refused": "preflight has errors", "errors": errors[:10]}
    return s.deploy(only=inp.get("only"), force=False)


def _copy(s: MigrationSession, inp: dict[str, Any]) -> Any:
    if not s.spec.copy_data:
        return {"refused": "copy_data was not requested for this migration"}
    return s.copy_data()


def _finish(_s: MigrationSession, inp: dict[str, Any]) -> Any:
    raise FinishSignal(str(inp.get("summary", "")))


HANDLERS: dict[str, Callable[[MigrationSession, dict[str, Any]], Any]] = {
    "get_job_context": lambda s, i: s.context(),
    "inventory_source": lambda s, i: s.inventory(),
    "plan_migration": lambda s, i: {k: v for k, v in s.plan().items() if k != "mapping"},
    "get_mapping": lambda s, i: s.cfg(),
    "update_mapping": lambda s, i: s.update_mapping(i.get("overrides") or {}),
    "convert_assets": lambda s, i: s.convert(),
    "list_items": lambda s, i: s.list_items(
        i.get("outcome"), i.get("type"), int(i.get("limit") or 50)
    ),
    "get_item": lambda s, i: s.get_item(str(i["id"])),
    "read_notebook": lambda s, i: s.read_notebook(str(i["name"])),
    "update_notebook_cell": lambda s, i: s.update_notebook_cell(
        str(i["name"]), int(i["index"]), str(i["source"]), str(i.get("note", ""))
    ),
    "run_preflight": lambda s, i: s.run_preflight(),
    "deploy_to_fabric": _deploy,
    "copy_data_to_fabric": _copy,
    "record_result": lambda s, i: s.record(str(i["id"]), str(i["outcome"]), str(i["note"])),
    "finish": _finish,
}


def execute_tool(session: MigrationSession, name: str, inp: dict[str, Any]) -> tuple[str, bool]:
    """-> (result text, is_error). Domain errors go back to the model as tool errors to recover from."""
    handler = HANDLERS.get(name)
    if handler is None:
        return f"unknown tool {name!r}", True
    if name == "record_result" and inp.get("outcome") not in (TEST, PART, NOT):
        return "outcome must be needs_testing, partial or not_migrated", True
    try:
        result = handler(session, inp)
    except FinishSignal:
        raise
    except AppError as e:
        return json.dumps({"error": e.code, "message": e.message, "field": e.field}), True
    except (KeyError, TypeError, ValueError) as e:
        return f"invalid input: {e}", True
    text = json.dumps(result, default=str)
    if (
        len(text) > 60000
    ):  # keep tool results bounded; the model can page with list_items / read_notebook
        text = text[:60000] + "... [truncated]"
    return text, False
