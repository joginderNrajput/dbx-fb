"""MigrationSession: one migration's state and the stage operations on it. Spec §4.1.

Both execution modes drive this object: pipeline mode calls the stages in a fixed order, agent
mode exposes them as tools to Claude (app/agents/tools.py). Credentials live on the session in
memory only and are never written to disk.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from app.conversion.common import OUTCOMES, PART, TEST, load_json, save_json
from app.core.config import Settings
from app.core.exceptions import ValidationFailed
from app.databricks.auth import DatabricksCredentials
from app.databricks.client import DatabricksClient
from app.databricks.discovery import DiscoveryOptions, Selection, discover, discover_selected
from app.fabric.auth import FabricCredentials, acquire_token
from app.fabric.client import Fabric
from app.fabric.deploy import deploy
from app.fabric.onelake import OneLake
from app.migration.data_copy import copy_data
from app.migration.engine import CellFixer, convert_all
from app.migration.ledger import do_plan, load_ledger, record_result, resolve, summarize_ledger
from app.migration.mapping import init_mapping, load_cfg, merge_mapping, validate_overrides
from app.migration.preflight import preflight
from app.migration.report import write_report
from app.sources.lookup import SourceResolver
from app.sources.offline import build_offline_assessment

__all__ = ["DEPLOY_KINDS", "MigrationSession", "SessionSpec"]

DEPLOY_KINDS = ("notebook", "shortcuts", "pipeline")
STAGES = ("inventory", "plan", "convert", "preflight", "deploy", "copy_data", "report")


@dataclass
class SessionSpec:
    """Everything a migration needs; built by the orchestrator from the API request."""

    source_kind: str  # "databricks" | "upload"
    sources_dir: Path
    out_dir: Path
    databricks: DatabricksCredentials | None = None
    discovery: DiscoveryOptions = field(default_factory=DiscoveryOptions)
    selection: Selection | None = None  # picked objects only (spec §5.10)
    uploaded_notebooks: list[str] = field(default_factory=list)
    assessment: dict[str, Any] | None = None  # uploaded / supplied inventory
    uploaded_mapping: dict[str, Any] = field(default_factory=dict)
    mapping_overrides: dict[str, Any] = field(default_factory=dict)
    fabric: FabricCredentials | None = None
    workspace_id: str | None = None
    workspace_name: str | None = None
    target_lakehouse: str | None = None  # every catalog lands in this lakehouse
    copy_data: bool = False
    warehouse_id: str | None = None
    deploy_requested: bool = False
    deploy_only: list[str] = field(default_factory=list)
    force_deploy: bool = False
    ai_fix: bool = False


class MigrationSession:
    def __init__(
        self,
        spec: SessionSpec,
        settings: Settings,
        *,
        cell_fixer: CellFixer | None = None,
        checkpoint: Callable[[str], None] | None = None,
        dbx_transport: httpx.BaseTransport | None = None,
        fabric_transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.spec = spec
        self.settings = settings
        self.cell_fixer = cell_fixer
        self._checkpoint = checkpoint or (lambda _stage: None)
        self._dbx_transport = dbx_transport
        self._fabric_transport = fabric_transport
        self._dbx: DatabricksClient | None = None
        self.done: list[str] = []
        self.inventory_warnings: list[str] = []
        self.preflight_result: list[dict[str, str]] | None = None
        self.deployed = False
        spec.out_dir.mkdir(parents=True, exist_ok=True)

    # ── plumbing ──
    @property
    def out(self) -> Path:
        return self.spec.out_dir

    def stage(self, name: str) -> None:
        self._checkpoint(name)

    def _mark(self, name: str) -> None:
        if name not in self.done:
            self.done.append(name)

    def dbx(self) -> DatabricksClient | None:
        if self.spec.databricks and self._dbx is None:
            self._dbx = DatabricksClient(
                self.spec.databricks, self.settings, transport=self._dbx_transport
            )
        return self._dbx

    def close(self) -> None:
        if self._dbx:
            self._dbx.close()

    def assessment(self) -> dict[str, Any]:
        p = self.out / "assessment.json"
        if not p.exists():
            raise ValidationFailed("run inventory first")
        data: dict[str, Any] = load_json(p)
        return data

    def cfg(self) -> dict[str, Any]:
        return load_cfg(self.out / "mapping.json")

    def context(self) -> dict[str, Any]:
        s = self.spec
        return {
            "source_kind": s.source_kind,
            "stages_done": self.done,
            "databricks_credentials": bool(s.databricks),
            "fabric_credentials": bool(s.fabric),
            "workspace_id": s.workspace_id or "",
            "deploy_requested": s.deploy_requested,
            "copy_data_requested": s.copy_data,
            "workspace_name": s.workspace_name or "",
            "target_lakehouse": s.target_lakehouse or "",
            "deploy_only": s.deploy_only,
            "ai_fix": s.ai_fix,
            "uploaded_notebooks": len(s.uploaded_notebooks),
            "assessment_supplied": s.assessment is not None,
        }

    # ── stages ──
    def inventory(self) -> dict[str, Any]:
        self.stage("inventory")
        s = self.spec
        if s.assessment is not None:
            assess = s.assessment
        elif s.source_kind == "databricks":
            client = self.dbx()
            if client is None:
                raise ValidationFailed(
                    "Databricks credentials are required for a databricks source",
                    field="source.credentials",
                )
            if s.selection and not s.selection.is_empty():
                assess = discover_selected(client, s.selection, s.discovery.max_notebooks)
            else:
                assess = discover(client, s.discovery)
        else:
            if not s.uploaded_notebooks:
                raise ValidationFailed(
                    "no notebooks were uploaded (accepted: .ipynb .py .sql .scala .r, or a .zip of them)",
                    field="files",
                )
            assess = build_offline_assessment(s.sources_dir, s.uploaded_notebooks)
        self.inventory_warnings = list(assess.get("warnings", []))
        save_json(self.out / "assessment.json", assess)
        self._mark("inventory")
        by_type: dict[str, int] = {}
        for a in assess["assets"]:
            by_type[a["type"]] = by_type.get(a["type"], 0) + 1
        return {
            "total_assets": len(assess["assets"]),
            "by_type": by_type,
            "warnings": self.inventory_warnings,
        }

    def update_mapping(self, overrides: dict[str, Any]) -> dict[str, Any]:
        validate_overrides(overrides)
        self.spec.mapping_overrides = merge_mapping(self.spec.mapping_overrides, overrides)
        self.done = [
            d for d in self.done if d == "inventory"
        ]  # plan and everything after must re-run
        return self.spec.mapping_overrides

    def plan(self) -> dict[str, Any]:
        self.stage("plan")
        assess = self.assessment()
        cfg = init_mapping(assess)
        if self.spec.target_lakehouse:
            lh = self.spec.target_lakehouse
            cfg["lakehouse_default"] = lh
            cfg["catalog_to_lakehouse"] = {c: lh for c in cfg["catalog_to_lakehouse"]}
        cfg = merge_mapping(cfg, self.spec.uploaded_mapping)
        cfg = merge_mapping(cfg, self.spec.mapping_overrides)
        if self.spec.workspace_id and not cfg.get("workspace_id"):
            cfg["workspace_id"] = self.spec.workspace_id
        save_json(self.out / "mapping.json", cfg)
        ledger = do_plan(assess, cfg, self.out, source="assessment.json")
        self._mark("plan")
        targets: dict[str, int] = {}
        for i in ledger["items"]:
            key = f"{i['target_type']} ({i['strategy']})"
            targets[key] = targets.get(key, 0) + 1
        return {"items": len(ledger["items"]), "targets": targets, "mapping": cfg}

    def convert(self) -> dict[str, Any]:
        self.stage("convert")
        if "plan" not in self.done:
            raise ValidationFailed("run plan before convert")
        resolver = SourceResolver(self.spec.sources_dir, self.dbx())
        ledger = convert_all(
            self.out,
            self.assessment(),
            self.cfg(),
            source_lookup=resolver,
            cell_fixer=self.cell_fixer if self.spec.ai_fix else None,
            dbt_root=self.spec.sources_dir,
        )
        write_report(self.out)
        self._mark("convert")
        summary = summarize_ledger(ledger)
        summary["notebooks_with_todos"] = [
            e["name"]
            for e in ledger["artifacts"]
            if e["kind"] == "notebook" and self._todo_count(e) > 0
        ]
        return summary

    def run_preflight(self) -> list[dict[str, str]]:
        self.stage("preflight")
        issues = [
            {"level": lvl, "where": w, "message": m}
            for lvl, w, m in preflight(self.out, self.cfg())
        ]
        self.preflight_result = issues
        self._mark("preflight")
        return issues

    def deploy(self, only: list[str] | None = None, force: bool | None = None) -> dict[str, Any]:
        self.stage("deploy")
        if not self.spec.fabric:
            raise ValidationFailed(
                "Fabric credentials are required to deploy", field="target.credentials"
            )
        kinds = only if only is not None else self.spec.deploy_only
        bad = [k for k in kinds if k not in DEPLOY_KINDS]
        if bad:
            raise ValidationFailed(
                f"unknown deploy kinds {bad}; allowed: {list(DEPLOY_KINDS)}", field="only"
            )
        fab = self._fabric()
        try:
            ledger = deploy(
                self.out,
                self.cfg(),
                fab,
                self._workspace(fab),
                only=kinds,
                force=self.spec.force_deploy if force is None else force,
            )
        finally:
            fab.close()
        write_report(self.out)
        self.deployed = True
        self._mark("deploy")
        return summarize_ledger(ledger)

    def _token(self, scope: str | None = None) -> str:
        if not self.spec.fabric:
            raise ValidationFailed("Fabric credentials are required", field="target.credentials")
        with httpx.Client(
            timeout=self.settings.FABRIC_TIMEOUT_S, transport=self._fabric_transport
        ) as http:
            return acquire_token(self.spec.fabric, self.settings, http, scope=scope)

    def _fabric(self) -> Fabric:
        return Fabric(self._token(), self.settings, transport=self._fabric_transport)

    def _workspace(self, fab: Fabric) -> str | None:
        """Workspace id, resolving target.workspace_name once (then remembered for this job)."""
        if not self.spec.workspace_id and self.spec.workspace_name:
            self.spec.workspace_id = fab.find_workspace(self.spec.workspace_name)
        return self.spec.workspace_id or self.cfg().get("workspace_id") or None

    def copy_data(self) -> dict[str, Any]:
        """Copy table data into the Fabric lakehouse(s) and validate row counts (spec §5.9)."""
        self.stage("copy_data")
        if "convert" not in self.done:
            raise ValidationFailed("run convert before copy_data")
        client = self.dbx()
        if client is None:
            raise ValidationFailed(
                "copy_data needs Databricks credentials (the data is read through a SQL warehouse)",
                field="source.credentials",
            )
        fab = self._fabric()
        lake = OneLake(
            self._token(self.settings.ONELAKE_SCOPE),
            self.settings,
            transport=self._fabric_transport,
        )
        try:
            ws = self._workspace(fab)
            if not ws:
                raise ValidationFailed(
                    "copy_data needs target.workspace_id or target.workspace_name",
                    field="target.workspace_id",
                )
            summary = copy_data(
                self.out,
                self.cfg(),
                client,
                fab,
                lake,
                ws,
                self.settings,
                warehouse_id=self.spec.warehouse_id,
                checkpoint=self.stage,
            )
        finally:
            fab.close()
            lake.close()
        write_report(self.out)
        self._mark("copy_data")
        return summary

    def report(self) -> dict[str, Any]:
        self.stage("report")
        s = write_report(self.out)
        self._mark("report")
        return s

    # ── inspection / edits (agent tools and API) ──
    def list_items(
        self, outcome: str | None = None, type_: str | None = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        led = load_ledger(self.out)
        rows = [
            i
            for i in led["items"]
            if (not outcome or i["outcome"] == outcome) and (not type_ or i["type"] == type_)
        ]
        return [
            {
                "id": i["id"],
                "type": i["type"],
                "target": f"{i['target_type']} / {i['target_name']}",
                "outcome": i["outcome"],
                "deployment": i["deployment"],
                "reasons": i["reasons"][:3],
            }
            for i in rows[:limit]
        ]

    def get_item(self, item_id: str) -> dict[str, Any]:
        for i in load_ledger(self.out)["items"]:
            if i["id"] == item_id:
                item: dict[str, Any] = i
                return item
        raise ValidationFailed(f"item not found: {item_id}", field="id")

    def _notebook_entry(self, name: str) -> dict[str, Any]:
        for e in load_ledger(self.out).get("artifacts", []):
            if e["kind"] == "notebook" and e["name"] == name:
                entry: dict[str, Any] = e
                return entry
        raise ValidationFailed(f"generated notebook not found: {name}", field="name")

    def _todo_count(self, entry: dict[str, Any]) -> int:
        nb = load_json(resolve(self.out, entry["path"]))
        return sum(
            "".join(c["source"]).count("TODO[dbx2fabric:")
            for c in nb["cells"]
            if c["cell_type"] == "code"
        )

    def read_notebook(self, name: str) -> dict[str, Any]:
        e = self._notebook_entry(name)
        nb = load_json(resolve(self.out, e["path"]))
        cells = [
            {
                "index": i,
                "type": c["cell_type"],
                "source": "".join(c["source"])[:8000],
                "has_todo": "TODO[dbx2fabric:" in "".join(c["source"]),
            }
            for i, c in enumerate(nb["cells"])
        ]
        return {
            "name": name,
            "items": e.get("items", []),
            "lakehouse": e.get("lakehouse"),
            "cells": cells,
        }

    def update_notebook_cell(
        self, name: str, index: int, source: str, note: str = "", actor: str = "agent"
    ) -> dict[str, Any]:
        """Replace one code cell; when no TODO markers remain, Partial items become Needs testing."""
        e = self._notebook_entry(name)
        path = resolve(self.out, e["path"])
        nb = load_json(path)
        if not 0 <= index < len(nb["cells"]) or nb["cells"][index]["cell_type"] != "code":
            raise ValidationFailed(f"cell {index} is not a code cell of {name}", field="index")
        nb["cells"][index]["source"] = source.splitlines(True)
        save_json(path, nb)
        remaining = self._todo_count(e)
        led = load_ledger(self.out)
        for it in led["items"]:
            if it["id"] in e.get("items", []):
                it["actions"].append(
                    f"[{actor}] rewrote cell {index}"
                    + (f": {note}" if note else "")
                    + " - review required"
                )
                if remaining == 0 and it["outcome"] == PART:
                    it["outcome"] = TEST
                    it["reasons"] = [r for r in it["reasons"] if not r.startswith("TODO ")]
        save_json(self.out / "ledger.json", led)
        return {"notebook": name, "cell": index, "remaining_todo_markers": remaining}

    def record(
        self, item_id: str, outcome: str, note: str, *, actor: str = "agent"
    ) -> dict[str, Any]:
        if outcome not in OUTCOMES:
            raise ValidationFailed(f"outcome must be one of {OUTCOMES}", field="outcome")
        return record_result(self.out, item_id, outcome, note, actor=actor)

    def outcome_counts(self) -> dict[str, int]:
        try:
            led = load_ledger(self.out)
        except Exception:
            return {}
        out: dict[str, int] = {}
        for i in led["items"]:
            out[i["outcome"]] = out.get(i["outcome"], 0) + 1
        return out
