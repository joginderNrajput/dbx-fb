"""Convert every planned ledger item into Fabric artifacts. Spec §3.7.

Ported from ``convert_all`` in the reference tool. Two deliberate changes (ADR 0003):
source code comes from an injected ``source_lookup`` (offline uploads and/or the Databricks
API), and dbt project folders must live inside the uploaded sources (``dbt_root``).
"""

from __future__ import annotations

import csv
import json
import re
import shutil
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from app.conversion.code import Ctx, convert_code
from app.conversion.common import (
    ENGINE_VERSION,
    MIG,
    NOT,
    PART,
    TEST,
    load_json,
    norm_path,
    safe,
    save_json,
)
from app.conversion.jobs import convert_job
from app.conversion.notebook import (
    Cell,
    build_ipynb,
    convert_cells,
    notes_from_ctx,
    outcome_from_ctx,
    parse_source,
)
from app.migration.ai_tasks import write_ai_task
from app.migration.dbt import convert_dbt
from app.migration.planner import is_default_managed, parse_abfss
from app.migration.runbook import write_runbook
from app.migration.validation import build_validation, code_cell

__all__ = ["CellFixer", "SourceLookup", "catalogs_for", "convert_all", "tsql_type"]

SourceLookup = Callable[[dict[str, Any]], tuple[str, str] | None]
CellFixer = Callable[[list[Cell], Ctx], int]

TESTS = {
    "Notebook": "Run with representative parameters; compare outputs/row counts with the Databricks run",
    "Data Pipeline": "Trigger a run; confirm activity order, parameters, retries and the schedule time",
    "shortcut": "Query the shortcut table; compare row counts and schema with the source",
    "copy": "Run the copy notebook; confirm every table reports OK and spot-check data",
    "view": "Run the view and compare results with Databricks for sample filters",
}

SQL_WORDS = {
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "AND",
    "OR",
    "NOT",
    "NULL",
    "TRUE",
    "FALSE",
    "IS",
    "IN",
    "AS",
    "BETWEEN",
    "LIKE",
    "DISTINCT",
    "INTERVAL",
    "SELECT",
    "FROM",
    "WHERE",
    "DIV",
    "MOD",
    "INT",
    "INTEGER",
    "BIGINT",
    "SMALLINT",
    "TINYINT",
    "STRING",
    "DOUBLE",
    "FLOAT",
    "DECIMAL",
    "DATE",
    "TIMESTAMP",
    "BOOLEAN",
    "CURRENT_DATE",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
}

MANUAL_NOTES = {
    "DLT Pipeline": "No DLT in Fabric: redesign as notebooks + Materialized Lake Views, orchestrated by a Data Pipeline",
    "Dashboard": "Rebuild as a Power BI report on a Fabric semantic model (Power BI Modeling MCP can help)",
    "Alert": "Recreate in Fabric Activator, or a pipeline with Teams/Outlook notification",
    "ML Model": "Re-register the model artifacts in the Fabric MLflow registry",
    "Serving Endpoint": "No native Fabric model serving: host on Azure ML / AI Foundry",
    "Repo": "Connect the Git repository to the Fabric workspace (Git integration)",
    "Credential": "Create a workspace identity / service principal and Fabric connections; secrets are not exportable",
    "Function": "Python/table-valued function, or signature missing (re-run the assessor v4): recreate as a PySpark UDF in a notebook or a Warehouse T-SQL function",
    # not in the reference tool (live discovery only, ADR 0003)
    "Workspace File": "Workspace file: copied to the lakehouse Files/workspace_files folder when data copy runs; otherwise upload it to the lakehouse Files section or the notebook's built-in resources",
}

CONN_REASON = "adls_connection_id not set: create an ADLS Gen2 connection in Fabric and put its id in mapping.json (shortcut spec is ready)"


def tsql_type(t: str | None) -> str:
    t = (t or "").lower().strip()
    m = re.match(r"decimal\((\d+),\s*(\d+)\)", t)
    if m:
        return f"DECIMAL({m.group(1)},{m.group(2)})"
    return {
        "int": "INT",
        "integer": "INT",
        "bigint": "BIGINT",
        "smallint": "SMALLINT",
        "tinyint": "SMALLINT",
        "double": "FLOAT",
        "float": "REAL",
        "string": "VARCHAR(8000)",
        "boolean": "BIT",
        "date": "DATE",
        "timestamp": "DATETIME2(6)",
        "decimal": "DECIMAL(18,0)",
    }.get(t, t.upper() + " /* verify type */")


def copy_notebook(lh: str, tables: list[dict[str, Any]]) -> list[Cell]:
    cfg_cell = "TABLES = " + json.dumps(tables, indent=2)
    run = '''results = []
for t in TABLES:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {t['schema']}")
    df = spark.read.format("delta").load(t["source"])
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if t["partitions"]:
        w = w.partitionBy(*t["partitions"])
    target = f"{t['schema']}.{t['table']}"
    w.saveAsTable(target)
    src_n, tgt_n = df.count(), spark.table(target).count()
    results.append((target, src_n, tgt_n, "OK" if src_n == tgt_n else "MISMATCH"))
for r in results:
    print(r)
bad = [r for r in results if r[3] != "OK"]
assert not bad, f"Row count mismatch: {bad}"'''
    return [
        dict(
            kind="md",
            src=f"# Physical copy into lakehouse `{lh}`\nAttach `{lh}` as default lakehouse. Reads each source Delta path (needs storage access) and validates row counts.",
        ),
        code_cell(cfg_cell),
        code_cell(run),
    ]


def _abfss(loc: str) -> tuple[str, str]:
    ph = parse_abfss(loc)
    if ph is None:
        raise ValueError(f"storage location {loc!r} is not an ADLS Gen2 (abfss://) path")
    return ph


def catalogs_for(assess: dict[str, Any], cfg: dict[str, Any]) -> list[str]:
    """Catalog names whose 3-part references get rewritten.

    The reference tool only knew catalogs from the inventory. Offline notebook uploads have no
    table inventory, so catalogs named in ``catalog_to_lakehouse`` count too (ADR 0003).
    """
    cats = {
        a["name"].split(".")[0]
        for a in assess["assets"]
        if a["type"] in ("Table/View", "Volume") and "." in a["name"]
    }
    return sorted(cats | set(cfg.get("catalog_to_lakehouse", {})))


def _dbt_folder(root: Path | None, rel: str | None) -> Path | None:
    """dbt folders are resolved inside the uploaded sources only; a server never reads arbitrary paths."""
    if not root or not rel:
        return None
    p = (root / rel).resolve()
    r = root.resolve()
    return p if (p == r or r in p.parents) and p.is_dir() else None


def convert_all(
    out: str | Path,
    assess: dict[str, Any],
    cfg: dict[str, Any],
    *,
    source_lookup: SourceLookup,
    cell_fixer: CellFixer | None = None,
    dbt_root: Path | None = None,
) -> dict[str, Any]:
    out = Path(out)
    ledger = load_json(out / "ledger.json")
    assets = {f"{a['type']}:{a['name']}": a for a in assess["assets"]}
    items = ledger["items"]
    catalogs = catalogs_for(assess, cfg)
    art: list[dict[str, Any]] = []
    tasks_dir = out / "ai_tasks"
    shutil.rmtree(out / "artifacts", ignore_errors=True)
    shutil.rmtree(tasks_dir, ignore_errors=True)
    adir = out / "artifacts"
    agg: dict[str, Any] = {
        "shortcuts": [],
        "copy": {},
        "views": {},
        "wh_tables": [],
        "wh_views": [],
        "queries": [],
        "env": [],
        "kv": [],
        "perm": [],
        "copy_specs": [],
        "export": [],
        "export_vol": [],
        "funcs": [],
        "wh_funcs": [],
        "validate": [],
    }

    def fail(it: dict[str, Any], reason: str, outcome: str = NOT) -> None:
        it["outcome"] = outcome
        it["reasons"].append(reason)

    for it in items:
        a = assets.get(
            it["id"], {"type": it["type"], "name": it["name"], "meta": {}, "location": ""}
        )
        m, st = a.get("meta", {}), it["strategy"]
        it.update(actions=[], reasons=[], tests=[], artifacts=[], deployment="n/a")
        try:
            if it["type"] == "Notebook":
                src = source_lookup(a)
                if not src:
                    fail(
                        it,
                        "Source code not available: upload the exported notebook (offline) or provide Databricks credentials",
                    )
                    continue
                cells = parse_source(*src)
                ctx = Ctx(cfg, catalogs)
                oc = convert_cells(cells, ctx, norm_path(a["location"]))
                if cell_fixer and ctx.todos:
                    n = cell_fixer(oc, ctx)
                    if n:
                        rem = sum(1 for c in oc if "TODO[dbx2fabric:" in c.get("src", ""))
                        it["actions"].append(f"AI-converted {n} cell(s) - review required")
                        if not rem:
                            ctx.todos = []
                nb = build_ipynb(
                    oc,
                    f"**Migrated from Databricks:** `{a['location']}` by dbx2fabric {ENGINE_VERSION}",
                )
                p = adir / "notebooks" / f"{it['target_name']}.ipynb"
                save_json(p, nb)
                lh = ctx.lakehouses[0] if ctx.lakehouses else cfg["lakehouse_default"]
                art.append(
                    {
                        "kind": "notebook",
                        "name": it["target_name"],
                        "path": str(p),
                        "lakehouse": lh,
                        "items": [it["id"]],
                    }
                )
                it["artifacts"].append(str(p))
                it["outcome"] = outcome_from_ctx(ctx)
                notes_from_ctx(it, ctx)
                it["tests"].append(TESTS["Notebook"])
                if ctx.params:
                    it["tests"].append(
                        "Check the parameter cell defaults: " + ", ".join(ctx.params)
                    )
                if ctx.secrets:
                    it["tests"].append(
                        "Create secrets in Key Vault: "
                        + ", ".join(sorted(k for _, k in ctx.secrets))
                    )
                it["deployment"] = "generated"
            elif it["type"] == "Job":
                pipe, sched, notes, omitted = convert_job(a, cfg)
                if not pipe:
                    fail(it, "No notebook tasks to convert; " + "; ".join(omitted or ["empty job"]))
                    continue
                p = adir / "pipelines" / f"{it['target_name']}.json"
                save_json(p, pipe)
                it["artifacts"].append(str(p))
                entry: dict[str, Any] = {
                    "kind": "pipeline",
                    "name": it["target_name"],
                    "path": str(p),
                    "items": [it["id"]],
                }
                if sched:
                    sp = adir / "pipelines" / f"{it['target_name']}.schedule.json"
                    save_json(sp, sched)
                    entry["schedule"] = str(sp)
                    it["actions"].append(f"schedule converted ({sched['configuration']['type']})")
                art.append(entry)
                it["actions"].append(
                    f"{len(pipe['properties']['activities'])} notebook activities generated"
                )
                it["reasons"] += notes + [f"Task not migrated - {o}" for o in omitted]
                it["outcome"] = PART if omitted else TEST
                it["tests"].append(TESTS["Data Pipeline"])
                it["deployment"] = "generated"
            elif it["type"] == "dbt Project":
                d = _dbt_folder(dbt_root, cfg["dbt_projects"].get(it["name"]))
                if not d:
                    fail(
                        it,
                        f"dbt project folder not provided: upload it with the sources and set mapping dbt_projects['{it['name']}'] to its folder",
                    )
                    continue
                path, issues = convert_dbt(it["name"], d, cfg, out)
                it["artifacts"].append(path)
                it["actions"].append(
                    f"profiles.yml retargeted to {'dbt-fabricspark (Lakehouse)' if cfg['dbt_target'] == 'lakehouse' else 'dbt-fabric (Warehouse)'}"
                )
                it["reasons"] += issues + [
                    "Create a dbt job item in Fabric (Data Factory) from this folder; dbt job support is preview/GA-rolling - check current status"
                ]
                it["outcome"] = PART if issues else TEST
                it["tests"].append(
                    "dbt build in Fabric; compare model row counts and test results with Databricks"
                )
                it["deployment"] = "generated"
            elif it["type"] == "Table/View":
                cat, sch, tbl = (a["name"].split(".") + ["", "", ""])[:3]
                lh = cfg["catalog_to_lakehouse"].get(cat, cfg["lakehouse_default"])
                wh = it["target_type"] == "Warehouse"
                if st == "manual":
                    fail(
                        it,
                        "Materialized/streaming/foreign table, or storage location unavailable (non-ADLS or assessment lacks metadata)",
                    )
                elif st == "view":
                    ctx = Ctx(cfg, catalogs)
                    body = convert_code(m.get("view_definition") or "", "sql", ctx)
                    if not m.get("view_definition"):
                        fail(it, "View definition missing in assessment")
                        continue
                    if wh:
                        agg["wh_views"].append((it["id"], sch, tbl, body, lh))
                    else:
                        agg["views"].setdefault(lh, []).append((it["id"], sch, tbl, body))
                    it["outcome"] = PART if ctx.todos else TEST
                    notes_from_ctx(it, ctx)
                    it["reasons"].append(
                        "Spark SQL dialect: verify results"
                        + (" and translate to T-SQL for Warehouse" if wh else "")
                    )
                    it["tests"].append(TESTS["view"])
                    if not wh:
                        agg["validate"].append(
                            {
                                "id": it["id"],
                                "source": a["name"],
                                "target": f"{sch}.{tbl}",
                                "lakehouse": lh,
                                "kind": "view",
                            }
                        )
                    it["deployment"] = "generated"
                else:
                    loc = m["storage_location"]
                    fmt = (m.get("format") or "").upper()
                    staged = is_default_managed(loc) or (
                        st == "copy" and not cfg["allow_direct_managed_copy"]
                    )
                    spath = (
                        f"{cfg['staging_storage'].rstrip('/')}/tables/{sch}/{tbl}"
                        if cfg["staging_storage"]
                        else ""
                    )
                    if staged and not spath:
                        fail(
                            it,
                            "Fabric cannot read Databricks/UC-managed storage directly. Set mapping.json staging_storage (an ADLS Gen2 container you own), re-run convert, then run databricks/export_to_staging.py in Databricks",
                        )
                        continue
                    if staged:
                        agg["export"].append(
                            {
                                "source": a["name"],
                                "path": spath,
                                "partitions": m.get("partition_columns", []),
                            }
                        )
                        loc = spath
                        it["actions"].append(
                            "Staged: Databricks export notebook writes a Delta copy to your ADLS container"
                        )
                    host, sub = _abfss(loc)
                    if st == "shortcut" and not staged:
                        agg["shortcuts"].append(
                            {
                                "item": it["id"],
                                "lakehouse": lh,
                                "path": "Tables/" + sch if fmt == "DELTA" else "Files/" + sch,
                                "name": tbl,
                                "target": {
                                    "adlsGen2": {
                                        "location": host,
                                        "subpath": sub,
                                        "connectionId": cfg["adls_connection_id"],
                                    }
                                },
                            }
                        )
                        it["outcome"] = TEST if cfg["adls_connection_id"] else PART
                        if not cfg["adls_connection_id"]:
                            it["reasons"].append(CONN_REASON)
                        it["actions"].append("OneLake shortcut (logical migration, no data copy)")
                        if fmt != "DELTA":
                            it["reasons"].append(
                                f"{fmt} data exposed under Files/: convert to a Delta table with a notebook"
                            )
                        it["tests"].append(TESTS["shortcut"])
                    else:
                        agg["copy"].setdefault(lh, []).append(
                            {
                                "item": it["id"],
                                "source": loc,
                                "schema": sch,
                                "table": tbl,
                                "partitions": m.get("partition_columns", []),
                            }
                        )
                        it["outcome"] = TEST
                        it["actions"].append(
                            "Physical copy notebook (reads Delta path, writes Lakehouse table, validates row counts)"
                        )
                        it["tests"].append(TESTS["copy"])
                    if fmt == "DELTA" or staged:
                        agg["validate"].append(
                            {
                                "id": it["id"],
                                "source": a["name"],
                                "target": f"{sch}.{tbl}",
                                "lakehouse": lh,
                                "kind": "table",
                            }
                        )
                    if wh:
                        agg["wh_tables"].append((sch, tbl, lh))
                        it["actions"].append(
                            f"CTAS into Warehouse {cfg['warehouse_name']} (load_tables.sql)"
                        )
                    it["deployment"] = "generated"
            elif st == "queries":
                ctx = Ctx(cfg, catalogs)
                body = convert_code(m.get("query_text") or "", "sql", ctx)
                agg["queries"].append((it["name"], body))
                it["outcome"] = PART if ctx.todos else TEST
                notes_from_ctx(it, ctx)
                it["reasons"].append(
                    "Spark SQL dialect: verify in Spark (notebook) or translate for Warehouse"
                )
                it["tests"].append("Run the cell; compare results with Databricks")
                it["deployment"] = "generated"
            elif st == "environment":
                if it["type"] == "Library":
                    kind, name = m.get("kind"), m.get("name")
                    if kind == "pypi":
                        agg["env"].append(name)
                        it["outcome"] = TEST
                        it["actions"].append("added to environment.yml")
                        it["tests"].append(
                            "Import the package in a notebook attached to the Environment"
                        )
                        it["deployment"] = "generated"
                    else:
                        fail(
                            it,
                            f"{kind} library: upload the artifact to the Fabric Environment manually"
                            + (" (Maven must be downloaded as a jar)" if kind == "maven" else ""),
                        )
                else:
                    it["outcome"] = TEST
                    it["actions"].append(
                        "Spark settings noted; create a Fabric Environment and pool of similar size"
                    )
                    it["reasons"].append(
                        f"Runtime {m.get('spark_version')} -> choose a Fabric runtime (Spark 3.5+); custom spark_conf: {list(m.get('spark_conf') or {})[:5]}"
                    )
                    it["tests"].append("Attach the Environment to a migrated notebook and run it")
                    it["deployment"] = "generated"
            elif st == "infra":
                it["outcome"] = MIG
                it["actions"].append(
                    f"Fabric Warehouse '{cfg['warehouse_name']}' (compute-equivalent; items created on deploy)"
                )
            elif st == "mount":
                if a["name"] in cfg["mount_map"]:
                    it["outcome"] = TEST
                    it["actions"].append(
                        f"{a['name']} -> {cfg['mount_map'][a['name']]} (paths rewritten in notebooks)"
                    )
                    it["tests"].append(
                        "Confirm the target folder exists in the lakehouse Files section and holds the data"
                    )
                else:
                    fail(it, "No mount_map entry: add it to mapping.json")
            elif st == "extloc":
                if cfg["adls_connection_id"]:
                    it["outcome"] = TEST
                    it["actions"].append("Used by shortcuts via adls_connection_id")
                    it["tests"].append(
                        "Confirm the Fabric connection can read " + str(m.get("url"))
                    )
                else:
                    fail(
                        it,
                        "Create an ADLS Gen2 connection in Fabric and set adls_connection_id in mapping.json",
                    )
            elif st == "volume":
                loc = m.get("storage_location") or ""
                cat, sch, vol = (a["name"].split(".") + ["", "", ""])[:3]
                lh = cfg["catalog_to_lakehouse"].get(cat, cfg["lakehouse_default"])
                if not parse_abfss(loc):
                    fail(it, "Volume storage location is not ADLS Gen2: copy files into OneLake")
                    continue
                if is_default_managed(loc):
                    if not cfg["staging_storage"]:
                        fail(
                            it,
                            "Volume is in Databricks-managed storage that Fabric cannot read. Set mapping.json staging_storage, re-run convert, then run databricks/export_to_staging.py",
                        )
                        continue
                    loc = f"{cfg['staging_storage'].rstrip('/')}/volumes/{sch}_{vol}"
                    agg["export_vol"].append({"source": f"/Volumes/{cat}/{sch}/{vol}", "path": loc})
                    it["actions"].append(
                        "Staged: Databricks export notebook copies the files to your ADLS container"
                    )
                host, sub = _abfss(loc)
                agg["shortcuts"].append(
                    {
                        "item": it["id"],
                        "lakehouse": lh,
                        "path": "Files",
                        "name": f"{sch}_{vol}",
                        "target": {
                            "adlsGen2": {
                                "location": host,
                                "subpath": sub,
                                "connectionId": cfg["adls_connection_id"],
                            }
                        },
                    }
                )
                it["outcome"] = TEST if cfg["adls_connection_id"] else PART
                if not cfg["adls_connection_id"]:
                    it["reasons"].append(CONN_REASON)
                it["actions"].append("Files shortcut")
                it["tests"].append(TESTS["shortcut"])
                it["deployment"] = "generated"
            elif st == "connection":
                ctype = (m.get("connection_type") or "").upper()
                agg["copy_specs"].append(
                    {
                        "connection": a["name"],
                        "type": ctype,
                        "target": it["target_type"],
                        "target_name": it["target_name"],
                        "tool": "Copy job"
                        if it["target_type"] == "SQL database"
                        else "Copy Data (pipeline)",
                        "note": "create Fabric connection, then list source tables",
                    }
                )
                fail(
                    it,
                    "Credentials cannot be exported: create the Fabric connection manually; spec written to ingestion/copy_specs.json",
                )
            elif st == "function":
                params, ret, body = m.get("input_params"), m.get("return_type"), m.get("definition")
                if (
                    m.get("language") != "SQL"
                    or m.get("is_table")
                    or params is None
                    or not ret
                    or not body
                ):
                    fail(it, MANUAL_NOTES["Function"])
                    continue
                if not params:
                    # no parameters captured, but the body uses names that are not SQL keywords or function calls
                    names = {
                        w.upper()
                        for w in re.findall(
                            r"\b([A-Za-z_]\w*)\b(?!\s*\()", re.sub(r"'[^']*'", "", body)
                        )
                    }
                    if names - SQL_WORDS:
                        fail(
                            it,
                            "Function body uses "
                            + ", ".join(sorted(names - SQL_WORDS)).lower()
                            + " but no parameters were captured: re-run the assessor or recreate the signature by hand",
                        )
                        continue
                cat, sch, fn = (a["name"].split(".") + ["", "", ""])[:3]
                ctx = Ctx(cfg, catalogs)
                body = convert_code(body, "sql", ctx)
                if sch.lower() in [x.lower() for x in cfg["warehouse_schemas"]]:
                    tb = body
                    for pr in params:
                        tb = re.sub(r"\b" + re.escape(pr["name"]) + r"\b", "@" + pr["name"], tb)
                    agg["wh_funcs"].append(
                        f"CREATE OR ALTER FUNCTION [{sch}].[{fn}]({', '.join('@' + pr['name'] + ' ' + tsql_type(pr['type']) for pr in params)})\nRETURNS {tsql_type(ret)}\nAS\nBEGIN\n    RETURN ({tb});\nEND;\nGO"
                    )
                    it["reasons"].append(
                        "T-SQL draft in warehouse/load_tables.sql: verify types and that the Spark SQL body is valid T-SQL"
                    )
                    it["actions"].append("Scalar SQL function -> Warehouse T-SQL function (draft)")
                else:
                    agg["funcs"].append(
                        (
                            fn,
                            f"CREATE OR REPLACE TEMPORARY FUNCTION {fn}({', '.join(pr['name'] + ' ' + pr['type'] for pr in params)})\nRETURNS {ret}\nRETURN {body}",
                        )
                    )
                    it["reasons"].append(
                        "Spark temporary function (session scoped): run the Migrated_Functions notebook (or %run it) before notebooks that call it"
                    )
                    it["actions"].append("Scalar SQL function -> Spark temporary function")
                notes_from_ctx(it, ctx)
                it["outcome"] = PART if ctx.todos else TEST
                it["tests"].append(f"Call {fn}() with sample inputs and compare with Databricks")
                it["deployment"] = "generated"
            elif it["type"] == "Secret Scope":
                for k in m.get("keys", []):
                    agg["kv"].append(
                        (
                            m.get("scope"),
                            k,
                            cfg["keyvault_urls"].get(m.get("scope"), cfg["keyvault_url_default"]),
                        )
                    )
                fail(
                    it,
                    "Secret values cannot be exported: create the secrets in Key Vault (names in security/keyvault_secrets.csv)",
                )
            elif it["type"] == "Permission":
                for g in m.get("grants", []):
                    agg["perm"].append(
                        (m.get("securable"), g.get("principal"), ";".join(g.get("privileges", [])))
                    )
                fail(
                    it,
                    "Map principals to Fabric workspace roles / OneLake security (matrix in security/permissions_matrix.csv)",
                )
            else:
                fail(
                    it,
                    MANUAL_NOTES.get(
                        it["type"], f"No automated path for {it['type']}: manual migration"
                    ),
                )
        except Exception as e:  # one broken asset must not stop the run; it is reported on the item
            fail(it, f"Conversion error: {e}")
        finally:
            if it["outcome"] in (NOT, PART):
                write_ai_task(it, a, tasks_dir, cfg)

    # aggregated artifacts
    for lh, tabs in agg["copy"].items():
        p = adir / "notebooks" / f"copy_tables_{safe(lh)}.ipynb"
        save_json(
            p,
            build_ipynb(
                copy_notebook(lh, [{k: v for k, v in t.items() if k != "item"} for t in tabs])
            ),
        )
        art.append(
            {
                "kind": "notebook",
                "name": f"copy_tables_{safe(lh)}",
                "path": str(p),
                "lakehouse": lh,
                "items": [t["item"] for t in tabs],
                "generated_only": True,
            }
        )
    for lh, views in agg["views"].items():
        cells = [dict(kind="md", src=f"# Views for lakehouse `{lh}`")] + [
            code_cell(f'spark.sql("""CREATE OR REPLACE VIEW {sch}.{v} AS\n{body}""")')
            for _, sch, v, body in views
        ]
        p = adir / "notebooks" / f"create_views_{safe(lh)}.ipynb"
        save_json(p, build_ipynb(cells))
        art.append(
            {
                "kind": "notebook",
                "name": f"create_views_{safe(lh)}",
                "path": str(p),
                "lakehouse": lh,
                "items": [v[0] for v in views],
            }
        )
    if agg["export"] or agg["export_vol"]:
        txt = (
            "# Databricks notebook source\n# MAGIC %md\n# MAGIC # Export managed tables / volumes to your ADLS Gen2 staging container (run in DATABRICKS)\n"
            "# MAGIC Fabric cannot read Databricks-managed storage. This copies data to `staging_storage`; Fabric then copies or shortcuts from there.\n\n# COMMAND ----------\n\n"
            "TABLES = "
            + json.dumps(agg["export"], indent=2)
            + "\nVOLUMES = "
            + json.dumps(agg["export_vol"], indent=2)
            + "\n\n# COMMAND ----------\n\n"
            "for t in TABLES:\n    w = spark.table(t['source']).write.format('delta').mode('overwrite').option('overwriteSchema', 'true')\n"
            "    if t['partitions']:\n        w = w.partitionBy(*t['partitions'])\n    w.save(t['path'])\n    print('exported', t['source'], '->', t['path'])\n"
            "for v in VOLUMES:\n    dbutils.fs.cp(v['source'], v['path'], recurse=True)\n    print('copied', v['source'], '->', v['path'])\n"
        )
        p = adir / "databricks" / "export_to_staging.py"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(txt, "utf-8")
        art.append(
            {
                "kind": "databricks_notebook",
                "name": "databricks/export_to_staging.py",
                "path": str(p),
                "items": [],
            }
        )
    if agg["funcs"]:
        cells = [dict(kind="md", src="# Migrated SQL functions (Spark temporary functions)")] + [
            code_cell("%%sql\n" + d) for _, d in agg["funcs"]
        ]
        p = adir / "notebooks" / "Migrated_Functions.ipynb"
        save_json(p, build_ipynb(cells))
        art.append(
            {
                "kind": "notebook",
                "name": "Migrated_Functions",
                "path": str(p),
                "lakehouse": cfg["lakehouse_default"],
                "items": [
                    i["id"]
                    for i in items
                    if i["type"] == "Function"
                    and i["outcome"] in (TEST, PART)
                    and "Spark temporary" in " ".join(i["actions"])
                ],
            }
        )
    if agg["queries"]:
        cells = [dict(kind="md", src="# Migrated SQL queries")]
        for n_, body in agg["queries"]:
            cells += [dict(kind="md", src=f"## {n_}"), code_cell("%%sql\n" + body)]
        p = adir / "notebooks" / "Migrated_SQL_Queries.ipynb"
        save_json(p, build_ipynb(cells))
        art.append(
            {
                "kind": "notebook",
                "name": "Migrated_SQL_Queries",
                "path": str(p),
                "lakehouse": cfg["lakehouse_default"],
                "items": [],
            }
        )
    if agg["wh_tables"] or agg["wh_views"] or agg["wh_funcs"]:
        sql = [
            f"-- Run in Fabric Warehouse '{cfg['warehouse_name']}'. Data must already be in the lakehouse (copy/shortcut first)."
        ]
        for sch in sorted(
            {s for s, _, _ in agg["wh_tables"]}
            | {v[1] for v in agg["wh_views"]}
            | set(re.findall(r"FUNCTION \[(\w+)\]", "\n".join(agg["wh_funcs"])))
        ):
            # Text of a generated script for the operator to review and run; nothing is executed here.
            sql.append(
                f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{sch}') EXEC('CREATE SCHEMA [{sch}]');\nGO"  # nosec B608
            )
        for sch, tbl, lh in agg["wh_tables"]:
            sql.append(
                f"DROP TABLE IF EXISTS [{sch}].[{tbl}];\nCREATE TABLE [{sch}].[{tbl}] AS SELECT * FROM [{lh}].[{sch}].[{tbl}];\nGO"  # nosec B608
            )
        sql += agg["wh_funcs"]
        for _, sch, v, body, _lh in agg["wh_views"]:
            sql.append(
                f"-- Spark SQL body: translate to T-SQL\nCREATE OR ALTER VIEW [{sch}].[{v}] AS\n{body};\nGO"
            )
        p = adir / "warehouse" / "load_tables.sql"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("\n\n".join(sql), "utf-8")
        art.append(
            {"kind": "script", "name": "warehouse/load_tables.sql", "path": str(p), "items": []}
        )
    if agg["shortcuts"]:
        p = adir / "lakehouse" / "shortcuts.json"
        save_json(p, agg["shortcuts"])
        art.append(
            {
                "kind": "shortcuts",
                "name": "shortcuts",
                "path": str(p),
                "items": [s["item"] for s in agg["shortcuts"]],
            }
        )
    if agg["env"]:
        p = adir / "environment" / "environment.yml"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(
            "dependencies:\n  - pip:\n" + "".join(f"    - {x}\n" for x in sorted(set(agg["env"]))),
            "utf-8",
        )
        art.append({"kind": "environment", "name": "environment.yml", "path": str(p), "items": []})
    for name, rows, hdr in (
        ("keyvault_secrets.csv", agg["kv"], ["scope", "secret_name", "key_vault_url"]),
        ("permissions_matrix.csv", agg["perm"], ["securable", "principal", "privileges"]),
    ):
        if rows:
            p = adir / "security" / name
            p.parent.mkdir(parents=True, exist_ok=True)
            with open(p, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(hdr)
                w.writerows(rows)
            art.append({"kind": "csv", "name": name, "path": str(p), "items": []})
    if agg["copy_specs"]:
        p = adir / "ingestion" / "copy_specs.json"
        save_json(p, agg["copy_specs"])
        art.append({"kind": "spec", "name": "copy_specs.json", "path": str(p), "items": []})
    build_validation(adir, cfg, agg["validate"], art, catalogs)
    byid = {i["id"]: i for i in items}
    for entry in art:
        for iid in entry.get("items", []):
            if iid in byid and entry["path"] not in byid[iid]["artifacts"]:
                byid[iid]["artifacts"].append(entry["path"])
    ledger.update(artifacts=art, converted=datetime.now().isoformat(timespec="seconds"))
    save_json(out / "ledger.json", ledger)
    write_runbook(out, cfg, ledger)
    return ledger
