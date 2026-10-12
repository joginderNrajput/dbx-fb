"""Push generated artifacts to a Fabric workspace and record per-item results. Spec §5.5."""

from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from typing import Any

from app.conversion.common import NOT, PART, TEST, load_json, save_json
from app.conversion.jobs import pipeline_refs
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
    no_schemas: set[str] = set()
    for lh in sorted(lhs):
        ids["LH:" + lh], how = fab.ensure(
            ws, "lakehouses", "Lakehouse", lh, {"creationPayload": {"enableSchemas": True}}
        )
        log.info("fabric_item_ensured", kind="lakehouse", name=lh, how=how)
        try:
            if not fab.lakehouse_has_schemas(ws, ids["LH:" + lh]):
                no_schemas.add(lh)
        except FabricError:
            pass  # property read is advisory; deploy continues
    if any(i["target_type"] == "Warehouse" for i in items.values()):
        ids["WH"], how = fab.ensure(ws, "warehouses", "Warehouse", cfg["warehouse_name"])
        log.info("fabric_item_ensured", kind="warehouse", name=cfg["warehouse_name"], how=how)
        for it in items.values():
            if it["type"] == "SQL Warehouse":
                it["deployment"], it["target_id"] = "deployed", ids["WH"]
    if any(i["target_type"] == "SQL database" for i in items.values()):
        ids["SQLDB"], how = fab.ensure(ws, "sqlDatabases", "SQLDatabase", cfg["sqldb_name"])
        log.info("fabric_item_ensured", kind="sql_database", name=cfg["sqldb_name"], how=how)
    env_art = next((x for x in arts if x["kind"] == "environment"), None)
    if env_art and want("environment"):
        env_id = _deploy_environment(out, cfg, fab, ws, env_art, items)
        if env_id:
            ids["ENV"] = env_id
    for e in [x for x in arts if x["kind"] == "notebook" and want("notebook")]:
        try:
            nb = load_json(resolve(out, e["path"]))
            lh = e.get("lakehouse") or cfg["lakehouse_default"]
            deps: dict[str, Any] = {
                "lakehouse": {
                    "default_lakehouse": ids["LH:" + lh],
                    "default_lakehouse_name": lh,
                    "default_lakehouse_workspace_id": ws,
                }
            }
            if "ENV" in ids:
                deps["environment"] = {"environmentId": ids["ENV"], "workspaceId": ws}
            nb["metadata"]["dependencies"] = deps

            def definition_of(nb: dict[str, Any]) -> dict[str, Any]:
                return {
                    "format": "ipynb",
                    "parts": [
                        {
                            "path": "notebook-content.ipynb",
                            "payload": b64(nb),
                            "payloadType": "InlineBase64",
                        }
                    ],
                }

            try:
                nid, how = fab.ensure(
                    ws, "notebooks", "Notebook", e["name"], definition=definition_of(nb)
                )
            except FabricError:
                if "environment" not in deps:
                    raise
                # the environment attachment must never cost the notebook: retry without it
                deps.pop("environment")
                nid, how = fab.ensure(
                    ws, "notebooks", "Notebook", e["name"], definition=definition_of(nb)
                )
                for iid in e["items"]:
                    if iid in items:
                        items[iid]["reasons"].append(
                            "Environment not attached (rejected by Fabric): attach it in the notebook settings"
                        )
            ids["NB:" + e["name"]] = nid
            for iid in e["items"]:
                if iid in items:
                    items[iid]["target_id"] = nid
            mark(e, True)
            if lh in no_schemas:
                for iid in e["items"]:
                    if iid in items:
                        items[iid]["reasons"].append(
                            f"Lakehouse {lh} is not schema-enabled: lakehouse.schema.table names will not resolve; use a schema-enabled lakehouse"
                        )
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
    pipelines = _dependency_order(
        out, [x for x in arts if x["kind"] == "pipeline" and want("pipeline")]
    )
    for e in pipelines:
        try:
            text = resolve(out, e["path"]).read_text("utf-8").replace("@@WS@@", ws)
            for kind, typ in (("NB", "Notebook"), ("PL", "DataPipeline")):
                for nm in set(re.findall(rf"@@{kind}:([^@]+)@@", text)):
                    if f"{kind}:{nm}" not in ids:
                        found = fab.find(ws, typ, nm)
                        if found:
                            ids[f"{kind}:{nm}"] = found
            miss = [m for m in re.findall(r"@@NB:([^@]+)@@", text) if "NB:" + m not in ids]
            if miss:
                raise FabricError(f"notebook(s) not deployed: {sorted(set(miss))}")
            miss = [m for m in pipeline_refs(text) if "PL:" + m not in ids]
            if miss:
                raise FabricError(
                    f"invoked pipeline(s) not deployed (or a reference cycle): {sorted(miss)}"
                )
            text = re.sub(r"@@NB:([^@]+)@@", lambda m: ids["NB:" + m.group(1)], text)
            text = re.sub(r"@@PL:([^@]+)@@", lambda m: ids["PL:" + m.group(1)], text)
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
            ids["PL:" + e["name"]] = pid
            for iid in e["items"]:
                if iid in items:
                    items[iid]["target_id"] = pid
            msg = ""
            if e.get("schedule"):
                try:
                    msg = _ensure_schedule(fab, ws, pid, load_json(resolve(out, e["schedule"])))
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


ENV_ENDPOINT = "environments"


def _deploy_environment(
    out: Path,
    cfg: dict[str, Any],
    fab: Fabric,
    ws: str,
    env_art: dict[str, Any],
    items: dict[str, dict[str, Any]],
) -> str | None:
    """Cluster libraries -> a published Fabric Environment that migrated notebooks attach to.

    environment.yml replaces the staged external libraries; publish is a long-running operation.
    Failures are recorded on the Library/Cluster items and never stop the deploy.
    """
    env_items = [
        i
        for i in items.values()
        if i["type"] == "Cluster"
        or (i["type"] == "Library" and any("environment.yml" in a for a in i["actions"]))
    ]
    name = "env_" + cfg["lakehouse_default"]
    try:
        env_id, how = fab.ensure(ws, ENV_ENDPOINT, "Environment", name)
        base = f"/workspaces/{ws}/environments/{env_id}/staging"
        fab.upload(
            f"{base}/libraries/importExternalLibraries",
            resolve(out, env_art["path"]).read_bytes(),
            "environment.yml",
        )
        st, h, _ = fab.req("POST", f"{base}/publish?beta=false")
        note = f"Fabric Environment '{name}' {how}; libraries imported and published"
        if st == 202:
            try:
                fab.wait(h)
            except FabricError as ex:
                note = f"Fabric Environment '{name}': publish not confirmed ({ex.message}); check it in the workspace"
        log.info("fabric_item_ensured", kind="environment", name=name, how=how)
    except FabricError as ex:
        log.warning("fabric_item_failed", kind="environment", name=name, error=ex.message)
        for it in env_items:
            it["reasons"].append(
                f"Environment not deployed ({ex.message}): create it and import environment.yml manually"
            )
        return None
    for it in env_items:
        it["target_id"] = env_id
        it["actions"].append(note)
        if it["type"] == "Library":
            it["deployment"] = "deployed"
    return env_id


def _dependency_order(out: Path, pipes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Invoked pipelines first: an Execute Pipeline activity references the target's item id.

    Pipelines in a reference cycle keep their original order and fail with a clear message."""
    by_name = {e["name"]: e for e in pipes}
    deps: dict[str, set[str]] = {}
    for e in pipes:
        try:
            refs = pipeline_refs(resolve(out, e["path"]).read_text("utf-8"))
        except (OSError, ValueError):
            refs = set()
        deps[e["name"]] = {r for r in refs if r in by_name and r != e["name"]}
    ordered: list[dict[str, Any]] = []
    done: set[str] = set()
    while len(done) < len(pipes):
        ready = [e for e in pipes if e["name"] not in done and deps[e["name"]] <= done]
        if not ready:  # cycle
            ready = [e for e in pipes if e["name"] not in done]
        ordered += ready
        done |= {e["name"] for e in ready}
    return ordered


def _ensure_schedule(fab: Fabric, ws: str, pid: str, schedule: dict[str, Any]) -> str:
    """One schedule per migrated pipeline: a re-deploy updates it instead of adding another."""
    base = f"/workspaces/{ws}/items/{pid}/jobs/Pipeline/schedules"
    try:
        existing = fab.list_paged(base)
    except FabricError:
        existing = []
    if existing and existing[0].get("id"):
        fab.req("PATCH", f"{base}/{existing[0]['id']}", schedule)
        return "schedule updated"
    fab.req("POST", base, schedule)
    return "schedule created"


def deploy_notebook(
    fab: Fabric, ws: str, name: str, nb: dict[str, Any], lakehouse: str, lakehouse_id: str
) -> str:
    """Create or update one notebook with its default lakehouse attached; returns the item id."""
    nb["metadata"]["dependencies"] = {
        "lakehouse": {
            "default_lakehouse": lakehouse_id,
            "default_lakehouse_name": lakehouse,
            "default_lakehouse_workspace_id": ws,
        }
    }
    definition = {
        "format": "ipynb",
        "parts": [
            {"path": "notebook-content.ipynb", "payload": b64(nb), "payloadType": "InlineBase64"}
        ],
    }
    nid, how = fab.ensure(ws, "notebooks", "Notebook", name, definition=definition)
    log.info("fabric_item_ensured", kind="notebook", name=name, how=how)
    return nid
