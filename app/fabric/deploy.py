"""Push generated artifacts to a Fabric workspace and record per-item results. Spec §5.5."""

from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from typing import Any

from app.conversion.common import NOT, PART, TEST, load_json, save_json
from app.core.exceptions import FabricError, ValidationFailed
from app.core.logging import get_logger
from app.fabric.client import Fabric, b64
from app.migration.ledger import resolve
from app.migration.preflight import preflight

__all__ = ["deploy"]

log = get_logger(__name__)


def deploy(
    out: str | Path,
    cfg: dict[str, Any],
    fab: Fabric,
    workspace_id: str | None,
    *,
    only: Iterable[str] = (),
    force: bool = False,
) -> dict[str, Any]:
    out = Path(out)
    ledger = load_json(out / "ledger.json")
    errs = [i for i in preflight(out, cfg) if i[0] == "error"]
    if errs and not force:
        raise ValidationFailed(
            "preflight found errors (see /preflight; force_deploy to deploy anyway): "
            + "; ".join(f"{w}: {m}" for _, w, m in errs[:3]),
            field="preflight",
        )
    ws = workspace_id or cfg["workspace_id"]
    if not ws:
        raise ValidationFailed(
            "Fabric workspace id required (target.workspace_id or mapping workspace_id)",
            field="target.workspace_id",
        )
    only_set = {x for x in only if x}

    def want(k: str) -> bool:
        return not only_set or k in only_set

    ids: dict[str, str] = {}
    items = {i["id"]: i for i in ledger["items"]}
    arts = ledger.get("artifacts", [])

    def mark(e: dict[str, Any], ok: bool, msg: str = "") -> None:
        for iid in e.get("items", []):
            it = items.get(iid)
            if it:
                it["deployment"] = "deployed" if ok else "failed"
                if not ok:
                    it["outcome"] = NOT
                    it["reasons"].append("Deployment failed: " + msg)
                elif msg:
                    it["actions"].append(msg)

    lhs = (
        {e["lakehouse"] for e in arts if e.get("lakehouse")}
        | set(cfg["catalog_to_lakehouse"].values())
        | {cfg["lakehouse_default"]}
    )
    for lh in sorted(lhs):
        ids["LH:" + lh], how = fab.ensure(
            ws, "lakehouses", "Lakehouse", lh, {"creationPayload": {"enableSchemas": True}}
        )
        log.info("fabric_item_ensured", kind="lakehouse", name=lh, how=how)
    if any(i["target_type"] == "Warehouse" for i in items.values()):
        ids["WH"], how = fab.ensure(ws, "warehouses", "Warehouse", cfg["warehouse_name"])
        log.info("fabric_item_ensured", kind="warehouse", name=cfg["warehouse_name"], how=how)
        for it in items.values():
            if it["type"] == "SQL Warehouse":
                it["deployment"], it["target_id"] = "deployed", ids["WH"]
    if any(i["target_type"] == "SQL database" for i in items.values()):
        ids["SQLDB"], how = fab.ensure(ws, "sqlDatabases", "SQLDatabase", cfg["sqldb_name"])
        log.info("fabric_item_ensured", kind="sql_database", name=cfg["sqldb_name"], how=how)
    for e in [x for x in arts if x["kind"] == "notebook" and want("notebook")]:
        try:
            nb = load_json(resolve(out, e["path"]))
            lh = e.get("lakehouse") or cfg["lakehouse_default"]
            nb["metadata"]["dependencies"] = {
                "lakehouse": {
                    "default_lakehouse": ids["LH:" + lh],
                    "default_lakehouse_name": lh,
                    "default_lakehouse_workspace_id": ws,
                }
            }
            definition = {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.ipynb",
                        "payload": b64(nb),
                        "payloadType": "InlineBase64",
                    }
                ],
            }
            nid, how = fab.ensure(ws, "notebooks", "Notebook", e["name"], definition=definition)
            ids["NB:" + e["name"]] = nid
            for iid in e["items"]:
                if iid in items:
                    items[iid]["target_id"] = nid
            mark(e, True)
            log.info("fabric_item_ensured", kind="notebook", name=e["name"], how=how)
        except FabricError as ex:
            mark(e, False, ex.message)
            log.warning("fabric_item_failed", kind="notebook", name=e["name"], error=ex.message)
    for e in [x for x in arts if x["kind"] == "shortcuts" and want("shortcuts")]:
        for sc in load_json(resolve(out, e["path"])):
            it = items.get(sc["item"])
            sc["target"]["adlsGen2"]["connectionId"] = (
                sc["target"]["adlsGen2"]["connectionId"] or cfg["adls_connection_id"]
            )
            try:
                if not sc["target"]["adlsGen2"]["connectionId"]:
                    raise FabricError("adls_connection_id not set in mapping.json")
                body = {k: v for k, v in sc.items() if k in ("path", "name", "target")}
                try:
                    fab.req(
                        "POST",
                        f"/workspaces/{ws}/items/{ids['LH:' + sc['lakehouse']]}/shortcuts",
                        body,
                    )
                except FabricError as ex:
                    if (ex.details or {}).get("status") != 409:  # already exists: fine on re-run
                        raise
                if it:
                    it["deployment"] = "deployed"
                    if it["outcome"] == PART:
                        it["outcome"] = TEST
                        it["reasons"] = [r for r in it["reasons"] if "adls_connection_id" not in r]
            except FabricError as ex:
                if it:
                    it["deployment"] = "failed"
                    it["outcome"] = NOT
                    it["reasons"].append("Shortcut failed: " + ex.message)
    for e in [x for x in arts if x["kind"] == "pipeline" and want("pipeline")]:
        try:
            text = resolve(out, e["path"]).read_text("utf-8").replace("@@WS@@", ws)
            for nm in set(re.findall(r"@@NB:([^@]+)@@", text)):
                if "NB:" + nm not in ids:
                    found = fab.find(ws, "Notebook", nm)
                    if found:
                        ids["NB:" + nm] = found
            miss = [m for m in re.findall(r"@@NB:([^@]+)@@", text) if "NB:" + m not in ids]
            if miss:
                raise FabricError(f"notebook(s) not deployed: {sorted(set(miss))}")
            text = re.sub(r"@@NB:([^@]+)@@", lambda m: ids["NB:" + m.group(1)], text)
            definition = {
                "parts": [
                    {
                        "path": "pipeline-content.json",
                        "payload": b64(text),
                        "payloadType": "InlineBase64",
                    }
                ]
            }
            pid, how = fab.ensure(
                ws, "dataPipelines", "DataPipeline", e["name"], definition=definition
            )
            for iid in e["items"]:
                if iid in items:
                    items[iid]["target_id"] = pid
            msg = ""
            if e.get("schedule"):
                try:
                    fab.req(
                        "POST",
                        f"/workspaces/{ws}/items/{pid}/jobs/Pipeline/schedules",
                        load_json(resolve(out, e["schedule"])),
                    )
                    msg = "schedule created"
                except FabricError as ex:
                    for iid in e["items"]:
                        items[iid]["reasons"].append("Schedule not created: " + ex.message)
            mark(e, True, msg)
            log.info("fabric_item_ensured", kind="pipeline", name=e["name"], how=how)
        except FabricError as ex:
            mark(e, False, ex.message)
            log.warning("fabric_item_failed", kind="pipeline", name=e["name"], error=ex.message)
    ledger["deployed"] = datetime.now().isoformat(timespec="seconds")
    save_json(out / "ledger.json", ledger)
    return ledger
