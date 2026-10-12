"""Source/target validation notebooks and import of their results. Spec §3.8."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from app.conversion.common import MIG, PART, load_json, save_json
from app.conversion.notebook import build_ipynb

__all__ = ["build_validation", "import_validation"]

METRICS_FN = """def metrics(df):
    cols = sorted(df.columns)
    row = df.agg(F.count(F.lit(1)).alias("n"),
                 F.sum(F.xxhash64(*[F.col(c).cast("string") for c in cols]).cast("decimal(38,0)")).alias("h")).first()
    return {"rows": int(row["n"]), "checksum": None if row["h"] is None else str(row["h"]),
            "columns": [[f.name, f.dataType.simpleString()] for f in df.schema.fields]}"""

FABRIC_VALIDATION_LOOP = """src = json.loads(notebookutils.fs.head(METRICS_PATH, 50000000))
results = []
for t in MANIFEST:
    s = src.get(t["source"])
    if not s or "error" in s:
        results.append({"id": t["id"], "status": "NO_SOURCE", "note": (s or {}).get("error", "no source metrics")})
        continue
    try:
        m = metrics(spark.table(t["target"]))
    except Exception as e:
        results.append({"id": t["id"], "status": "MISSING", "note": str(e)[:200]})
        continue
    issues = []
    if s["rows"] != m["rows"]:
        issues.append(f"rows {s['rows']} != {m['rows']}")
    elif s["checksum"] != m["checksum"]:
        issues.append("checksum differs (inspect values; float/timestamp formatting can differ)")
    sc, tc = {c.lower(): ty for c, ty in s["columns"]}, {c.lower(): ty for c, ty in m["columns"]}
    if set(sc) != set(tc):
        issues.append(f"columns differ: missing {sorted(set(sc) - set(tc))} extra {sorted(set(tc) - set(sc))}")
    types = [c for c in sc if c in tc and sc[c] != tc[c]]
    note = "; ".join(issues) + (f" | type changes: {types}" if types else "")
    results.append({"id": t["id"], "status": "PASS" if not issues else "FAIL", "note": note, "rows_source": s["rows"], "rows_target": m["rows"]})
for r in results:
    print(r["status"], r["id"], r.get("rows_source"), r.get("rows_target"), r["note"])
notebookutils.fs.put(RESULTS_PATH, json.dumps(results), True)
bad = [r for r in results if r["status"] != "PASS"]
print(f"{len(results) - len(bad)} passed, {len(bad)} need attention")"""


def code_cell(src: str) -> dict[str, Any]:
    return dict(kind="code", src=src)


def build_validation(
    adir: Path,
    cfg: dict[str, Any],
    manifest: list[dict[str, Any]],
    art: list[dict[str, Any]],
    catalogs: list[str],
) -> None:
    if not manifest:
        return
    vdir = adir / "validation"
    vdir.mkdir(parents=True, exist_ok=True)
    staging = (
        cfg["staging_storage"].rstrip("/") + "/validation/source_metrics.json"
        if cfg["staging_storage"]
        else ""
    )
    src_manifest = [{"id": m["id"], "source": m["source"], "kind": m["kind"]} for m in manifest]
    dbx = (
        "# Databricks notebook source\n# MAGIC %md\n# MAGIC # Source metrics for migration validation (run in DATABRICKS)\n"
        "# MAGIC Row count, per-table checksum and schema. Download `source_metrics.json` and upload it to the Fabric lakehouse under Files/validation/.\n\n# COMMAND ----------\n\n"
        "import json\nfrom pyspark.sql import functions as F\nMANIFEST = "
        + json.dumps(src_manifest, indent=2)
        + "\nOUT = "
        + json.dumps(staging)
        + "\n\n# COMMAND ----------\n\n"
        + METRICS_FN
        + "\n\nout = {}\nfor t in MANIFEST:\n    try:\n        out[t['source']] = metrics(spark.table(t['source']))\n    except Exception as e:\n        out[t['source']] = {'error': str(e)}\n"
        "text = json.dumps(out, indent=2)\nprint(text)\nif OUT:\n    dbutils.fs.put(OUT, text, True)\n    print('written to', OUT)\n"
    )
    (vdir / "databricks_source_metrics.py").write_text(dbx, "utf-8")
    dflt = cfg["lakehouse_default"]
    tgt = [
        {
            "id": m["id"],
            "source": m["source"],
            "target": (
                m["target"] if m["lakehouse"] == dflt else f"{m['lakehouse']}.{m['target']}"
            ),
            "kind": m["kind"],
        }
        for m in manifest
    ]
    cells: list[dict[str, Any]] = [
        dict(
            kind="md",
            src="# Migration validation (run in FABRIC)\nCompares source metrics from Databricks with the migrated tables: row count, checksum, column names. Writes `Files/validation/results.json`; upload it to the migration service with `POST /api/v1/migrations/{id}/validation`.",
        ),
        dict(
            kind="code",
            tags=["parameters"],
            src='METRICS_PATH = "Files/validation/source_metrics.json"\nRESULTS_PATH = "Files/validation/results.json"',
        ),
        code_cell(
            "import json\nfrom pyspark.sql import functions as F\nMANIFEST = "
            + json.dumps(tgt, indent=2)
            + "\n"
            + METRICS_FN
        ),
        code_cell(FABRIC_VALIDATION_LOOP),
    ]
    p = adir / "notebooks" / "validate_migration.ipynb"
    save_json(p, build_ipynb(cells))
    art.append(
        {
            "kind": "notebook",
            "name": "validate_migration",
            "path": str(p),
            "lakehouse": dflt,
            "items": [],
        }
    )


def import_validation(out: str | Path, results: list[dict[str, Any]]) -> dict[str, int]:
    """Passed tables become Migrated, failures Partial; everything else is noted on the item."""
    out = Path(out)
    led = load_json(out / "ledger.json")
    byid = {i["id"]: i for i in led["items"]}
    n = {"PASS": 0, "FAIL": 0, "other": 0}  # nosec B105
    for r in results:
        it = byid.get(r.get("id", ""))
        if not it:
            continue
        it["validation"] = {k: r.get(k) for k in ("status", "note", "rows_source", "rows_target")}
        if r.get("status") == "PASS":
            it["outcome"] = MIG
            it["actions"].append(
                f"Validated: row count ({r.get('rows_target')}), checksum and columns match the source"
            )
            n["PASS"] += 1
        elif r.get("status") == "FAIL":
            it["outcome"] = PART
            it["reasons"].append("Validation failed: " + str(r.get("note")))
            n["FAIL"] += 1
        else:
            it["reasons"].append(f"Validation {r.get('status')}: {r.get('note')}")
            n["other"] += 1
    led["validated"] = datetime.now().isoformat(timespec="seconds")
    save_json(out / "ledger.json", led)
    return n
