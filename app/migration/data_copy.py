"""Copy table data Databricks -> Fabric Lakehouse and validate it. Spec §5.9.

Route (no storage account of the customer needed):
1. COUNT(*) then ``SELECT *`` on a Databricks SQL warehouse (Statement Execution API, JSON_ARRAY).
2. Each result chunk -> one JSON-lines file in OneLake ``Files/<DATA_COPY_FOLDER>/<cat>/<schema>/<table>/``.
3. A generated loader notebook (one per lakehouse) is deployed and run through the Fabric job
   API: it casts every column back to its Databricks type, writes a Delta table
   ``schema.table`` (``schema_table`` in a lakehouse without schemas) and checks the row count.
4. The notebook's results file updates the ledger: equal row counts -> migrated.
Views are then created by running the generated ``create_views_<lakehouse>`` notebook.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from app.conversion.common import MIG, NOT, PART, TEST, load_json, safe, save_json
from app.conversion.notebook import build_ipynb
from app.core.config import Settings
from app.core.exceptions import DatabricksError, FabricError, ValidationFailed
from app.core.logging import get_logger
from app.databricks.client import DatabricksClient
from app.fabric.client import Fabric
from app.fabric.deploy import deploy_notebook
from app.fabric.onelake import OneLake
from app.migration.ledger import resolve

__all__ = [
    "COPYABLE_TYPES",
    "LOADER_CODE",
    "copy_data",
    "copy_files",
    "pick_warehouse",
    "rows_to_jsonl",
]

log = get_logger(__name__)
COPYABLE_TYPES = ("MANAGED", "EXTERNAL", "MATERIALIZED_VIEW", "STREAMING_TABLE")

LOADER_CODE = """import json
from pyspark.sql import functions as F

def _col(c, ty):
    base = ty.split("<")[0].split("(")[0].strip().lower()
    ref = F.col("`" + c.replace("`", "``") + "`")
    if base in ("array", "struct", "map"):
        return F.from_json(ref, ty).alias(c)
    if base in ("string", "varchar", "char", "void"):
        return ref.alias(c)
    if base == "binary":
        return F.unbase64(ref).alias(c)
    return ref.cast(ty).alias(c)

results = []
for t in MANIFEST:
    try:
        target = t["target"]
        if SCHEMAS_ENABLED:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{t['schema']}`")
        raw = spark.read.schema(", ".join("`" + c.replace("`", "``") + "` STRING" for c, _ in t["columns"])).json(t["path"])
        df = raw.select([_col(c, ty) for c, ty in t["columns"]])
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
        n = spark.table(target).count()
        ok = n == t["rows"]
        results.append({"id": t["id"], "status": "PASS" if ok else "FAIL", "target": target, "rows_source": t["rows"], "rows_target": n,
                        "note": "" if ok else f"rows {t['rows']} != {n}"})
    except Exception as e:
        results.append({"id": t["id"], "status": "ERROR", "target": t.get("target"), "note": str(e)[:800]})
    print(results[-1])
notebookutils.fs.put(RESULTS_PATH, json.dumps(results), True)
bad = [r for r in results if r["status"] != "PASS"]
print(f"{len(results) - len(bad)} table(s) loaded and validated, {len(bad)} need attention")"""


def rows_to_jsonl(columns: list[str], rows: list[list[Any]]) -> bytes:
    """One JSON object per row; values stay strings (typed back by the loader notebook)."""
    return "".join(
        json.dumps(dict(zip(columns, r, strict=False)), ensure_ascii=False) + "\n" for r in rows
    ).encode()


def pick_warehouse(dbx: DatabricksClient, warehouse_id: str | None) -> str:
    if warehouse_id:
        return warehouse_id
    whs = dbx.list_warehouses()
    if not whs:
        raise ValidationFailed(
            "no Databricks SQL warehouse is visible to these credentials; create one or pass source.warehouse_id",
            field="source.warehouse_id",
        )
    running = [w for w in whs if w.get("state") == "RUNNING"]
    return str((running or whs)[0]["id"])  # a stopped warehouse auto-starts on the first statement


def _quote(name: str) -> str:
    """`cat`.`schema`.`table` with embedded backticks doubled: names (possibly from an uploaded
    assessment) can only ever be identifiers in the generated SELECT, never SQL."""
    return ".".join("`" + p.replace("`", "``") + "`" for p in name.split("."))


def _complete(it: dict[str, Any], r: dict[str, Any]) -> None:
    it["validation"] = {k: r.get(k) for k in ("status", "note", "rows_source", "rows_target")}
    if r.get("status") == "PASS":
        it["outcome"] = MIG
        it["deployment"] = "deployed"
        # reasons about staging / manual copy no longer apply: the data is in Fabric
        it["reasons"] = [
            x
            for x in it["reasons"]
            if "staging_storage" not in x
            and "Materialized/streaming" not in x
            and "adls_connection_id" not in x
        ]
        it["actions"].append(
            f"Data copied to {r.get('target')} and validated: {r.get('rows_target')} rows match the source"
        )
    else:
        it["outcome"] = PART
        it["reasons"].append(f"Data copy {r.get('status')}: {r.get('note')}")


def _clean(path: str) -> str:
    """A OneLake-relative path from a source path: no empty, '.' or '..' segments."""
    return "/".join(p for p in path.split("/") if p not in ("", ".", ".."))


class _Budget:
    def __init__(self, settings: Settings) -> None:
        self.files_left = settings.FILE_COPY_MAX_FILES
        self.bytes_left = settings.FILE_COPY_MAX_TOTAL_MB * 1024 * 1024
        self.max_file = settings.FILE_COPY_MAX_FILE_MB * 1024 * 1024

    def take(self, size: int) -> str:
        """'' when the file fits, else why it is skipped."""
        if size > self.max_file:
            return f"larger than FILE_COPY_MAX_FILE_MB ({size // 1048576} MB)"
        if self.files_left <= 0 or size > self.bytes_left:
            return "file copy budget exhausted (FILE_COPY_MAX_FILES / FILE_COPY_MAX_TOTAL_MB)"
        self.files_left -= 1
        self.bytes_left -= size
        return ""


def copy_files(
    out: Path,
    cfg: dict[str, Any],
    dbx: DatabricksClient,
    fab: Fabric,
    lake: OneLake,
    ws: str,
    settings: Settings,
    *,
    checkpoint: Callable[[str], None],
    lakehouse_id: Callable[[str], str],
) -> dict[str, Any]:
    """UC volumes no shortcut serves, and workspace files, -> lakehouse ``Files/`` (spec §5.9).

    A volume goes to ``Files/<schema>_<volume>/`` (the name its shortcut would have had), a
    workspace file to ``Files/workspace_files/<path>``. Reads use the Databricks Files API /
    workspace export; writes use OneLake. Per-item results land on the ledger.
    """
    ledger = load_json(out / "ledger.json")
    budget = _Budget(settings)
    summary: dict[str, Any] = {
        "volumes": 0,
        "workspace_files": 0,
        "files": 0,
        "bytes": 0,
        "failed": 0,
    }
    for it in ledger["items"]:
        if it["type"] == "Volume":
            if cfg["adls_connection_id"] and "Files shortcut" in it.get("actions", []):
                continue  # a OneLake shortcut serves it in place
            checkpoint("copy_files")
            cat, sch, vol = (it["name"].split(".") + ["", "", ""])[:3]
            if not (cat and sch and vol):
                continue
            lh = cfg["catalog_to_lakehouse"].get(cat, cfg["lakehouse_default"])
            src_root = f"/Volumes/{cat}/{sch}/{vol}"
            dest_root = f"Files/{_clean(f'{sch}_{vol}')}"
            n = size = 0
            skipped: list[str] = []
            try:
                lid = lakehouse_id(lh)
                stack = [src_root]
                while stack:
                    d = stack.pop()
                    for e in dbx.list_directory(d):
                        path = str(e.get("path") or "")
                        if not path.startswith(src_root + "/"):
                            continue
                        if e.get("is_directory"):
                            stack.append(path.rstrip("/"))
                            continue
                        why = budget.take(int(e.get("file_size") or 0))
                        if why:
                            skipped.append(f"{path}: {why}")
                            continue
                        data = dbx.download_file(path)
                        lake.upload(ws, lid, f"{dest_root}/{_clean(path[len(src_root) :])}", data)
                        n, size = n + 1, size + len(data)
            except (DatabricksError, FabricError) as e:
                it["reasons"].append(f"Volume file copy failed after {n} file(s): {e.message}")
                it["outcome"] = PART if n else it["outcome"]
                summary["failed"] += 1
                continue
            finally:
                summary["files"] += n
                summary["bytes"] += size
            summary["volumes"] += 1
            it["actions"].append(
                f"Copied {n} file(s) ({size / 1048576:.1f} MB) from {src_root} to lakehouse {lh} {dest_root}"
            )
            it["reasons"] = [
                r
                for r in it["reasons"]
                if "copy files into OneLake" not in r and "staging_storage" not in r
            ] + [f"Not copied - {x}" for x in skipped[:20]]
            it["outcome"] = PART if skipped else TEST
            it["deployment"] = "deployed"
            it["target_id"] = lid
            it["tests"].append(f"Compare the file list of {dest_root} with {src_root}")
        elif it["type"] == "Workspace File":
            checkpoint("copy_files")
            lh = cfg["lakehouse_default"]
            dest = f"Files/workspace_files/{_clean(it['name'])}"
            try:
                data = dbx.export_file(it["name"])
                why = budget.take(len(data))
                if why:
                    it["reasons"].append(f"Not copied - {why}")
                    continue
                lid = lakehouse_id(lh)
                lake.upload(ws, lid, dest, data)
            except (DatabricksError, FabricError) as e:
                it["reasons"].append(f"Workspace file copy failed: {e.message}")
                summary["failed"] += 1
                continue
            summary["workspace_files"] += 1
            summary["files"] += 1
            summary["bytes"] += len(data)
            it["actions"].append(f"Copied to lakehouse {lh} {dest}")
            it["outcome"], it["deployment"], it["target_id"] = TEST, "deployed", lid
            it["tests"].append(
                "Update notebook references to the file (/Workspace/... -> /lakehouse/default/"
                + dest
                + ")"
            )
    save_json(out / "ledger.json", ledger)
    return summary


def copy_data(
    out: Path,
    cfg: dict[str, Any],
    dbx: DatabricksClient,
    fab: Fabric,
    lake: OneLake,
    ws: str,
    settings: Settings,
    *,
    warehouse_id: str | None,
    checkpoint: Callable[[str], None],
) -> dict[str, Any]:
    lh_ids: dict[str, str] = {}
    lh_schemas: dict[str, bool] = {}

    def lakehouse_id(lh: str) -> str:
        if lh not in lh_ids:
            lh_ids[lh], _ = fab.ensure(
                ws, "lakehouses", "Lakehouse", lh, {"creationPayload": {"enableSchemas": True}}
            )
            lh_schemas[lh] = fab.lakehouse_has_schemas(ws, lh_ids[lh])
        return lh_ids[lh]

    files = copy_files(
        out, cfg, dbx, fab, lake, ws, settings, checkpoint=checkpoint, lakehouse_id=lakehouse_id
    )
    ledger = load_json(out / "ledger.json")
    assess = load_json(out / "assessment.json")
    metas = {f"{a['type']}:{a['name']}": a.get("meta", {}) for a in assess["assets"]}
    items = {i["id"]: i for i in ledger["items"]}
    todo: list[dict[str, Any]] = []
    for it in ledger["items"]:
        if it["type"] != "Table/View":
            continue
        meta = metas.get(it["id"], {})
        if meta.get("table_type") not in COPYABLE_TYPES:
            continue
        if it["strategy"] == "shortcut" and cfg["adls_connection_id"]:
            continue  # served by a OneLake shortcut, no copy needed
        todo.append(it)
    summary: dict[str, Any] = {
        "tables": len(todo),
        "copied": 0,
        "failed": 0,
        "skipped": 0,
        "views_created": [],
        "files": files,
    }
    if not todo:
        ledger["data_copy"] = summary
        save_json(out / "ledger.json", ledger)
        return summary
    wh = pick_warehouse(dbx, warehouse_id)
    summary["warehouse_id"] = wh
    folder = settings.DATA_COPY_FOLDER.strip("/")
    run_id = datetime.now().strftime("%Y%m%d%H%M%S")
    manifests: dict[str, list[dict[str, Any]]] = {}
    for it in todo:
        checkpoint("copy_data")
        cat, sch, tbl = (it["name"].split(".") + ["", "", ""])[:3]
        lh = cfg["catalog_to_lakehouse"].get(cat, cfg["lakehouse_default"])
        lakehouse_id(lh)
        try:
            cnt = dbx.execute_statement(
                wh,
                f"SELECT COUNT(*) FROM {_quote(it['name'])}",  # nosec B608
                disposition="INLINE",
                timeout_s=settings.DATA_COPY_STATEMENT_TIMEOUT_S,
            )
            total = int(((cnt.get("result") or {}).get("data_array") or [["0"]])[0][0])
            if total > settings.DATA_COPY_MAX_ROWS:
                it["reasons"].append(
                    f"Data copy skipped: {total} rows > DATA_COPY_MAX_ROWS={settings.DATA_COPY_MAX_ROWS}; use databricks/export_to_staging.py + copy notebook"
                )
                summary["skipped"] += 1
                continue
            stmt = dbx.execute_statement(
                wh,
                f"SELECT * FROM {_quote(it['name'])}",  # nosec B608
                disposition=settings.DATA_COPY_DISPOSITION,
                timeout_s=settings.DATA_COPY_STATEMENT_TIMEOUT_S,
            )
            manifest = stmt.get("manifest") or {}
            columns = [
                (c["name"], c.get("type_text") or c.get("type_name") or "string")
                for c in (manifest.get("schema") or {}).get("columns", [])
            ]
            rel = f"Files/{folder}/{safe(cat)}/{safe(sch)}/{safe(tbl)}"
            lake.delete(ws, lh_ids[lh], rel)
            n_chunks = (
                int(manifest.get("total_chunk_count") or 0)
                if int(manifest.get("total_row_count") or 0)
                else 0
            )
            names = [c for c, _ in columns]
            written = 0
            for idx in range(max(n_chunks, 1)):
                rows = dbx.statement_chunk_rows(stmt, idx) if n_chunks else []
                written += len(rows)
                lake.upload(
                    ws, lh_ids[lh], f"{rel}/part-{idx:05d}.json", rows_to_jsonl(names, rows)
                )
            target = f"{sch}.{tbl}" if lh_schemas[lh] else f"{safe(sch)}_{safe(tbl)}"
            manifests.setdefault(lh, []).append(
                {
                    "id": it["id"],
                    "schema": sch,
                    "table": tbl,
                    "target": target,
                    "path": rel,
                    "columns": columns,
                    "rows": total,
                }
            )
            it["actions"].append(f"Exported {written} rows from Databricks to OneLake {rel}")
            if not lh_schemas[lh]:
                it["reasons"].append(
                    f"Lakehouse {lh} has no schemas: table written as {target}; 3-part names in migrated notebooks will not resolve"
                )
        except (DatabricksError, FabricError) as e:
            it["outcome"] = PART if it["outcome"] != NOT else NOT
            it["reasons"].append(f"Data export failed: {e.message}")
            summary["failed"] += 1
    for lh, manifest_rows in manifests.items():
        checkpoint("load_data")
        results_path = f"Files/{folder}/_results/load_{run_id}.json"
        nb = build_ipynb(
            [
                dict(
                    kind="md",
                    src=f"# Load migrated Databricks data into `{lh}`\nGenerated by dbx2fabric: reads JSON-lines exports under `Files/{folder}/`, casts columns to their Databricks types, writes Delta tables and validates row counts.",
                ),
                dict(
                    kind="code",
                    tags=["parameters"],
                    src=f"RESULTS_PATH = {json.dumps(results_path)}\nSCHEMAS_ENABLED = {lh_schemas[lh]}",
                ),
                dict(kind="code", src="MANIFEST = " + json.dumps(manifest_rows, indent=1)),
                dict(kind="code", src=LOADER_CODE),
            ]
        )
        name = f"load_migrated_data_{safe(lh)}"
        save_json(out / "artifacts" / "notebooks" / f"{name}.ipynb", nb)
        try:
            nid = deploy_notebook(fab, ws, name, nb, lh, lh_ids[lh])
            job = fab.run_notebook(
                ws, nid, poll_s=settings.FABRIC_JOB_POLL_S, timeout_s=settings.FABRIC_JOB_TIMEOUT_S
            )
            if job.get("status") != "Completed":
                raise FabricError(
                    f"loader notebook {name} ended {job.get('status')}: {json.dumps(job.get('failureReason'))[:400]}"
                )
            results = json.loads(lake.read(ws, lh_ids[lh], results_path))
        except FabricError as e:
            for m in manifest_rows:
                items[m["id"]]["outcome"] = PART
                items[m["id"]]["reasons"].append(
                    f"Data load failed: {e.message} (exported files are in {m['path']}; run notebook {name} in Fabric)"
                )
                summary["failed"] += 1
            continue
        for r in results:
            it = items.get(r.get("id", ""))
            if it:
                _complete(it, r)
                summary["copied" if r.get("status") == "PASS" else "failed"] += 1
        # views depend on the loaded tables: run the generated view notebook for this lakehouse
        vname = f"create_views_{safe(lh)}"
        ventry = next(
            (
                e
                for e in ledger.get("artifacts", [])
                if e["kind"] == "notebook" and e["name"] == vname
            ),
            None,
        )
        if ventry:
            try:
                vid = deploy_notebook(
                    fab, ws, vname, load_json(resolve(out, ventry["path"])), lh, lh_ids[lh]
                )
                vjob = fab.run_notebook(
                    ws,
                    vid,
                    poll_s=settings.FABRIC_JOB_POLL_S,
                    timeout_s=settings.FABRIC_JOB_TIMEOUT_S,
                )
                ok = vjob.get("status") == "Completed"
                for iid in ventry.get("items", []):
                    if iid in items:
                        (items[iid]["actions"] if ok else items[iid]["reasons"]).append(
                            f"View notebook {vname} run {'succeeded' if ok else 'failed: ' + json.dumps(vjob.get('failureReason'))[:300]}"
                        )
                        if ok and items[iid]["outcome"] == PART:
                            items[iid]["outcome"] = TEST
                if ok:
                    summary["views_created"].append(vname)
            except FabricError as e:
                for iid in ventry.get("items", []):
                    if iid in items:
                        items[iid]["reasons"].append(f"View notebook {vname} failed: {e.message}")
    ledger["data_copied"] = datetime.now().isoformat(timespec="seconds")
    ledger["data_copy"] = summary
    save_json(out / "ledger.json", ledger)
    return summary
