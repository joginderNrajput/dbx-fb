#!/usr/bin/env python3
"""
dbx2fabric_migrate.py - Databricks -> Microsoft Fabric migration tool (stdlib only).

Pipeline:  init -> plan -> convert -> [deploy] -> report        (or: run)
Input   :  assessment.json produced by dbx2fabric_assess.py (v3, includes source metadata)
Output  :  migration_out/  ledger.json, artifacts/, ai_tasks/, post_migration_report.html/.csv

Modes
  Offline  (default)  generates Fabric artifacts only: notebooks (.ipynb), pipeline JSON, shortcut specs,
                      SQL scripts, environment.yml, dbt project. Nothing is sent to Fabric.
  Online   (--deploy) creates/updates items through the Fabric REST API (token from FABRIC_TOKEN or `az login`).
  AI       (--ai)     optional: sends only cells that still contain TODO markers to the Anthropic API
                      (ANTHROPIC_API_KEY). Without it, work orders in ai_tasks/ can be given to any AI client.
  MCP      (mcp)      runs this tool as an MCP server (stdio) for Claude Code, Codex, GitHub Copilot, etc.

Target rules
  OLTP database -> Fabric SQL database | OLAP database -> Warehouse | other analytics/files -> Lakehouse
  Code -> Notebooks | Orchestration -> Data Pipeline | Ingestion -> Copy Data / Copy Job (specs emitted)
"""
import argparse, base64, csv, html, json, os, posixpath, re, shutil, subprocess, sys, time
import urllib.error, urllib.parse, urllib.request
from datetime import datetime, timedelta
from pathlib import Path

VERSION = "0.3.0"
MIG, TEST, PART, NOT = "migrated", "needs_testing", "partial", "not_migrated"
LABEL = {MIG: "Migrated", TEST: "Migrated - needs testing", PART: "Partially migrated - manual rework", NOT: "Not migrated"}


def log(*a):
    print(*a, file=sys.stderr, flush=True)


def safe(n):
    return re.sub(r"[^A-Za-z0-9_\-]+", "_", n or "").strip("_")[:100] or "item"


def ident(n):
    n = re.sub(r"\W+", "_", n or "")
    return n if re.match(r"[A-Za-z_]", n) else "_" + n


def load_json(p):
    return json.loads(Path(p).read_text("utf-8"))


def save_json(p, o):
    Path(p).parent.mkdir(parents=True, exist_ok=True)
    Path(p).write_text(json.dumps(o, indent=2, ensure_ascii=False), "utf-8")


def norm_path(p, base=""):
    p = (p or "").strip().strip("\"'")
    if p.startswith("/Workspace/"):
        p = p[len("/Workspace"):]
    if p and not p.startswith("/") and base.startswith("/"):
        p = posixpath.normpath(posixpath.join(posixpath.dirname(base), p))
    return p


def nb_name(p):
    return safe(norm_path(p).rstrip("/").split("/")[-1])


# ---------------------------------------------------------------------------
# CONFIG (mapping.json) - the "Map" step; edit it, then re-run.
# ---------------------------------------------------------------------------
DEFAULT_MAPPING = {
    "workspace_id": "",
    "lakehouse_default": "lh_main",
    "catalog_to_lakehouse": {},
    "warehouse_name": "wh_main",
    "warehouse_schemas": ["gold", "dw", "mart", "reporting"],   # OLAP schemas -> Fabric Warehouse
    "sqldb_name": "sqldb_main",
    "sqldb_connection_types": ["POSTGRESQL", "MYSQL", "SQLSERVER", "ORACLE", "SQLDW"],  # OLTP sources -> SQL database
    "keyvault_url_default": "https://<your-vault>.vault.azure.net/",
    "keyvault_urls": {},          # secret scope -> vault url
    "mount_map": {},              # "/mnt/raw" -> "Files/raw"
    "adls_connection_id": "",     # Fabric connection id used by OneLake shortcuts
    "timezone": "UTC",
    "dbt_target": "warehouse",    # warehouse | lakehouse
    "dbt_projects": {},           # job name -> local dbt project folder
    "staging_storage": "",        # abfss://container@account.dfs.core.windows.net/dbx_export (ADLS Gen2 you own)
    "allow_direct_managed_copy": False,  # read UC managed tables straight from their storage (only if you own it and Fabric can reach it)
}


def load_cfg(path=None):
    cfg = json.loads(json.dumps(DEFAULT_MAPPING))
    if path and Path(path).exists():
        cfg.update(load_json(path))
    return cfg


def init_mapping(assess, path):
    cfg = load_cfg(None)
    assets = assess["assets"]
    cats = sorted({a["name"].split(".")[0] for a in assets if a["type"] in ("Table/View", "Volume") and "." in a["name"]})
    cfg["catalog_to_lakehouse"] = {c: f"lh_{safe(c)}" for c in cats}
    if cats:
        cfg["lakehouse_default"] = f"lh_{safe(cats[0])}"
    cfg["mount_map"] = {a["name"]: "Files/" + a["name"].rstrip("/").split("/")[-1] for a in assets if a["type"] == "DBFS Mount"}
    cfg["keyvault_urls"] = {a["name"]: cfg["keyvault_url_default"] for a in assets if a["type"] == "Secret Scope"}
    cfg["dbt_projects"] = {a["name"]: "" for a in assets if a["type"] == "Job" and any("dbt_task" in t for t in a["meta"].get("settings", {}).get("tasks", []))}
    save_json(path, cfg)
    return cfg


# ---------------------------------------------------------------------------
# CODE CONVERSION
# ---------------------------------------------------------------------------
class Ctx:
    def __init__(self, cfg, catalogs):
        self.cfg, self.catalogs = cfg, catalogs
        self.changes, self.todos, self.warns = {}, [], {}
        self.params, self.deps, self.lakehouses, self.secrets = {}, set(), [], set()
        self.wh_refs = set()

    def change(self, label, n=1):
        self.changes[label] = self.changes.get(label, 0) + n

    def use_lh(self, lh):
        if lh not in self.lakehouses:
            self.lakehouses.append(lh)


SECRET_RE = r"dbutils\.secrets\.get\(\s*(?:scope\s*=\s*)?([\"'])(.+?)\1\s*,\s*(?:key\s*=\s*)?([\"'])(.+?)\3\s*\)"
WIDGET_DEF_RE = r"^([ \t]*)dbutils\.widgets\.(?:text|dropdown|combobox|multiselect)\(\s*(?:name\s*=\s*)?([\"'])(.+?)\2\s*,\s*(?:defaultValue\s*=\s*)?([\"'])(.*?)\4.*\)[ \t]*$"
WIDGET_GET_RE = r"dbutils\.widgets\.(?:get|getArgument)\(\s*(?:name\s*=\s*)?([\"'])(.+?)\1\s*(?:,[^)]*)?\)"
WIDGET_RM_RE = r"^([ \t]*)dbutils\.widgets\.(?:remove|removeAll)\(.*\)[ \t]*$"
NB_RUN_RE = r"dbutils\.notebook\.run\(\s*([\"'])(.+?)\1"

# first matching rule per line gets a TODO marker above it (specific -> generic)
TODO_RULES = [
    ("DBU-MOUNT", r"dbutils\.fs\.(mount|unmount|mounts|refreshMounts|updateMount)\b", "Mounts are not supported: use OneLake shortcuts or abfss paths"),
    ("DBU-TASKVALUES", r"dbutils\.jobs\.taskValues", "Use notebookutils.notebook.exit values and pipeline expressions"),
    ("DBU-CONTEXT", r"entry_point|getContext\(\)", "Use notebookutils.runtime.context"),
    ("DBU-REMAINING", r"dbutils\.", "Unconverted dbutils call: replace with notebookutils or redesign"),
    ("ADLS-KEY", r"fs\.azure\.(account\.key|sas\.|account\.oauth)", "Storage key/SAS/OAuth settings in code: use Entra ID / workspace identity, OneLake shortcuts or abfss paths, and remove the key"),
    ("DBFS-PATH", r"dbfs:/|/dbfs/|[\"']/mnt/", "Unmapped DBFS/mount path: add it to mapping.json mount_map or copy the data to OneLake"),
    ("AUTOLOADER", r"cloudFiles", "No Auto Loader: use Structured Streaming file source, pipeline triggers or Eventstream"),
    ("DLT", r"import dlt\b|@dlt\.|dlt\.(read|table|view)|CREATE\s+(OR\s+REFRESH\s+)?(STREAMING|LIVE)\s+(TABLE|VIEW)", "No DLT in Fabric: redesign as Materialized Lake Views / Spark jobs"),
    ("COPY-INTO", r"\bCOPY\s+INTO\b", "Replace with a pipeline Copy activity / Copy job or Spark read + write"),
    ("AI-FUNCTIONS", r"\b(ai_query|ai_classify|ai_extract|ai_gen|ai_summarize|vector_search|http_request)\s*\(", "Databricks AI functions: use Fabric AI functions or Azure OpenAI"),
    ("FEATURE-STORE", r"databricks\.feature_store|FeatureEngineeringClient|FeatureStoreClient", "No Feature Store: rebuild as Delta tables + MLflow"),
    ("DBX-SDK", r"from databricks\.(sdk|sql|connect)|import databricks_cli|databricks-connect", "Replace with Fabric REST APIs / semantic-link / notebookutils"),
    ("DELTA-SHARING", r"delta_sharing|CREATE\s+SHARE", "Use OneLake shortcuts / external data sharing"),
    ("SQL-PROC", r"\bCREATE\s+(OR\s+REPLACE\s+)?PROCEDURE\b", "Rewrite as T-SQL stored procedure in Fabric Warehouse or notebook logic"),
    ("MLFLOW-DBX", r"databricks-uc|set_registry_uri\([\"']databricks", "Point MLflow at the Fabric workspace registry"),
    ("GRANTS", r"^\s*(GRANT|REVOKE|DENY)\b", "Re-implement with Fabric workspace roles / OneLake security / SQL permissions"),
    ("SQL-WIDGET", r"CREATE\s+WIDGET", "Replace with a Fabric parameter cell"),
]
TODO_RULES = [(i, re.compile(rx, re.I | re.M), a) for i, rx, a in TODO_RULES]

# need testing, but converted or compatible
TEST_RULES = [
    ("STREAMING", r"readStream|writeStream|foreachBatch", "Structured Streaming: re-validate checkpoints, triggers, sinks"),
    ("DELTA-CDF", r"table_changes\(|readChangeFeed|enableChangeDataFeed", "Change Data Feed: verify support in your Fabric runtime"),
    ("LIQUID-CLUSTER", r"\bCLUSTER\s+BY\b", "Liquid clustering: verify support in your Fabric runtime"),
    ("GENERATED-COLS", r"GENERATED\s+(ALWAYS|BY\s+DEFAULT)\s+AS", "Identity/generated columns: verify Delta protocol support"),
    ("ARROW", r"pyarrow|useArrow|arrow\.pyspark|import pandas|toPandas\(", "Arrow/pandas paths: pyarrow ships with Fabric Spark runtimes; verify version and config"),
    ("ZORDER", r"\bZORDER\s+BY\b", "Prefer V-Order + OPTIMIZE in Fabric"),
    ("TIME-TRAVEL", r"VERSION\s+AS\s+OF|TIMESTAMP\s+AS\s+OF|versionAsOf|timestampAsOf", "Time travel: confirm Delta log retention after the data copy"),
    ("SQL-UDF", r"\bCREATE\s+(OR\s+REPLACE\s+)?(TEMP(ORARY)?\s+)?FUNCTION\b", "SQL UDF: recreate per notebook session"),
    ("JDBC-ODBC", r"jdbc:|format\([\"']jdbc|pyodbc|\bodbc\b", "JDBC/ODBC: recreate as Fabric connection / gateway; secrets in Key Vault"),
    ("PIP-INSTALL", r"%pip install", "Move libraries to a Fabric Environment for repeatability"),
    ("MLFLOW", r"import mlflow", "MLflow is native in Fabric: check experiment and model names"),
    ("MLFLOW-EXPERIMENT-PATH", r"set_experiment\(\s*[\"']/", "Fabric experiments are named items: use a plain name, not a workspace path"),
    ("CONSTRAINTS", r"ADD\s+CONSTRAINT|\bCHECK\s*\(", "Constraints: verify behaviour on the Fabric Delta writer"),
]
TEST_RULES = [(i, re.compile(rx, re.I | re.M), a) for i, rx, a in TEST_RULES]
COMMENT = {"python": "#", "sql": "--", "scala": "//", "r": "#"}


def convert_code(src, lang, ctx):
    cfg, s, cm = ctx.cfg, src, COMMENT.get(lang, "#")

    def sub(label, rx, repl, flags=0):
        nonlocal s
        s, n = re.subn(rx, repl, s, flags=flags)
        if n:
            ctx.change(label, n)

    if lang in ("python", "scala"):
        if lang == "python":
            def wdef(m):
                ctx.params[ident(m.group(3))] = m.group(5)
                return f"{m.group(1)}# [dbx2fabric] widget '{m.group(3)}' -> parameter cell"

            def wget(m):
                n = ident(m.group(2))
                ctx.params.setdefault(n, "")
                return n
            sub("dbutils.widgets -> parameter cell", WIDGET_DEF_RE, wdef, re.M)
            sub("dbutils.widgets.get -> parameter variable", WIDGET_GET_RE, wget)
            sub("dbutils.widgets.remove removed", WIDGET_RM_RE, lambda m: f"{m.group(1)}# [dbx2fabric] widgets.remove not needed", re.M)

        sub("self-assignment removed (value comes from the parameter cell)", r"^([ \t]*)(\w+)[ \t]*=[ \t]*\2[ \t]*$", lambda m: f"{m.group(1)}# [dbx2fabric] {m.group(2)} comes from the parameter cell", re.M)
        sub("mlflow.set_registry_uri(databricks) removed (Fabric uses the workspace registry)", r"^[ \t]*mlflow\.set_registry_uri\([\"']databricks[^\n]*$", lambda m: "# [dbx2fabric] removed: " + m.group(0).strip(), re.M)

        def sec(m):
            scope, key = m.group(2), m.group(4)
            ctx.secrets.add((scope, key))
            vault = cfg["keyvault_urls"].get(scope, cfg["keyvault_url_default"])
            return f'notebookutils.credentials.getSecret("{vault}", "{key}")'
        sub("dbutils.secrets.get -> notebookutils.credentials.getSecret", SECRET_RE, sec)

        def nrun(m):
            ctx.deps.add(nb_name(m.group(2)))
            return f'notebookutils.notebook.run("{nb_name(m.group(2))}"'
        sub("dbutils.notebook.run -> notebookutils.notebook.run", NB_RUN_RE, nrun)
        sub("dbutils.notebook.exit -> notebookutils.notebook.exit", r"dbutils\.notebook\.exit\(", "notebookutils.notebook.exit(")
        sub("dbutils.fs -> notebookutils.fs", r"dbutils\.fs\.(ls|cp|mv|rm|mkdirs|put|head|append)\b", r"notebookutils.fs.\1")
        sub("dbutils.library.restartPython -> notebookutils.session.restartPython", r"dbutils\.library\.restartPython\(\)", "notebookutils.session.restartPython()")

    for mnt in sorted(cfg["mount_map"], key=len, reverse=True):
        dest, m_ = cfg["mount_map"][mnt].rstrip("/"), mnt.rstrip("/")
        sub(f"mount {m_} -> {dest}", re.escape("dbfs:" + m_) + r"(?![\w])", lambda m, d=dest: d)
        sub(f"mount {m_} -> {dest}", r"(?<![\w/.:\-])" + re.escape(m_) + r"(?![\w])", lambda m, d=dest: d)

    for cat in ctx.catalogs:
        lh = cfg["catalog_to_lakehouse"].get(cat, cfg["lakehouse_default"])
        rx = r"(?<![\w.`])`?" + re.escape(cat) + r"`?\.`?(\w+)`?\.`?(\w+)`?"
        whs = [x.lower() for x in cfg["warehouse_schemas"]]

        def cat_repl(m, lh=lh):
            if m.group(1).lower() in whs:
                ctx.wh_refs.add(f"{m.group(1)}.{m.group(2)}")
            return f"{lh}.{m.group(1)}.{m.group(2)}"
        new, n = re.subn(rx, cat_repl, s)
        if n:
            s = new
            ctx.change(f"catalog '{cat}' -> lakehouse '{lh}' (3-part names)", n)
            ctx.use_lh(lh)

    sub("Databricks-only Spark config removed", r"^[ \t]*spark\.conf\.set\(\s*[\"']spark\.databricks\.[^\n]*$", lambda m: cm + " [dbx2fabric] removed: " + m.group(0).strip(), re.M)
    sub("Databricks-only SET removed", r"^[ \t]*SET\s+spark\.databricks\.[^\n]*$", lambda m: cm + " [dbx2fabric] removed: " + m.group(0).strip(), re.M)

    out = []
    for line in s.split("\n"):
        stripped = line.strip()
        if stripped and not stripped.startswith((cm, "[dbx2fabric]")):
            for rid, rx, advice in TODO_RULES:
                if rx.search(line):
                    indent = line[:len(line) - len(line.lstrip())]
                    out.append(f"{indent}{cm} TODO[dbx2fabric:{rid}] {advice}")
                    ctx.todos.append(dict(rule=rid, advice=advice, line=stripped[:120]))
                    break
        out.append(line)
    s = "\n".join(out)
    if ctx.wh_refs:
        ctx.warns["WH-LANDING"] = (len(ctx.wh_refs), "Uses Warehouse-mapped tables (" + ", ".join(sorted(ctx.wh_refs)[:3]) + "): Spark reads/writes them in the Lakehouse; publish to the Warehouse with load_tables.sql after this notebook runs")
    for rid, rx, advice in TEST_RULES:
        n = len(rx.findall(src))
        if n:
            ctx.warns[rid] = (ctx.warns.get(rid, (0, ""))[0] + n, advice)
    return s


HDR_RE = re.compile(r"^(#|--|//)\s*Databricks notebook source\s*$")
MAGIC_RE = re.compile(r"^(#|--|//)\s?MAGIC\b")


def parse_source(text, lang):
    """Databricks source format (.py/.sql/.scala/.r) or .ipynb -> [{kind, lang, src}]"""
    lang = (lang or "PYTHON").lower()
    text = text.lstrip("\ufeff").replace("\r\n", "\n")
    cells = []
    if text.lstrip().startswith("{") and '"cells"' in text[:2000]:
        for c in json.loads(text).get("cells", []):
            cells.append(_classify("".join(c.get("source", [])), lang, md=c.get("cell_type") == "markdown"))
        return cells
    text = re.sub(r"^# /// script\n((?:#.*\n)*?)# ///\n?", lambda m: "" if "tool.databricks" in m.group(1) else m.group(0), text, flags=re.M)
    lines = [ln for ln in text.split("\n") if not HDR_RE.match(ln.strip())]
    chunks, chunk = [], []
    for ln in lines:
        if re.match(r"^(#|--|//)\s*COMMAND\s*-+\s*$", ln):
            chunks.append(chunk)
            chunk = []
        else:
            chunk.append(ln)
    chunks.append(chunk)
    for ch in chunks:
        segs = []                       # consecutive MAGIC lines vs plain lines become separate cells
        for ln in ch:
            is_m = bool(MAGIC_RE.match(ln))
            if not segs or segs[-1][0] != is_m:
                segs.append((is_m, []))
            segs[-1][1].append(re.sub(r"^(#|--|//)\s?MAGIC ?", "", ln) if is_m else ln)
        for is_m, ls in segs:
            while ls and not ls[0].strip():
                ls = ls[1:]
            while ls and not ls[-1].strip():
                ls = ls[:-1]
            if ls:
                cells.append(_classify("\n".join(ls), lang))
    return cells


def _classify(src, default, md=False):
    if md:
        return dict(kind="md", lang="md", src=src)
    m = re.match(r"\s*%(\w+)\b[ \t]*\n?", src)
    if m and m.group(1) in ("sql", "md", "python", "scala", "r", "sh", "run", "pip", "fs"):
        k = m.group(1)
        if k == "md":
            return dict(kind="md", lang="md", src=src[m.end():])
        if k in ("run", "pip"):
            return dict(kind="code", lang=k, src=src.strip())
        return dict(kind="code", lang=k, src=src[m.end():])
    return dict(kind="code", lang=default if default in ("python", "sql", "scala", "r") else "python", src=src)


def convert_cells(cells, ctx, base_path=""):
    out = []
    for c in cells:
        if c["kind"] == "md":
            out.append(dict(kind="md", src=c["src"]))
            continue
        lang, src = c["lang"], c["src"]
        if lang == "run":
            m = re.match(r"%run\s+(\S+)", src)
            name = nb_name(norm_path(m.group(1), base_path)) if m else ""
            ctx.deps.add(name)
            ctx.change("%run path -> notebook name")
            out.append(dict(kind="code", src=f"%run {name}"))
        elif lang in ("sh", "fs"):
            ctx.todos.append(dict(rule="MAGIC-SH-FS", advice="Rewrite shell/%fs with notebookutils or Python", line=src[:80]))
            out.append(dict(kind="code", src="# TODO[dbx2fabric:MAGIC-SH-FS] Rewrite shell/%fs with notebookutils or Python\n" + "\n".join("# " + l for l in src.split("\n"))))
        elif lang == "pip":
            ctx.warns["PIP-INSTALL"] = (ctx.warns.get("PIP-INSTALL", (0, ""))[0] + 1, "Move libraries to a Fabric Environment for repeatability")
            out.append(dict(kind="code", src=src))
        else:
            code = convert_code(src, lang, ctx)
            if re.search(r"^(#|--|//)\s?MAGIC\b", code, re.M):
                cm = COMMENT.get(lang, "#")
                ctx.todos.append(dict(rule="MAGIC-LEFTOVER", advice="Unconverted MAGIC cell", line=code[:80]))
                code = f"{cm} TODO[dbx2fabric:MAGIC-LEFTOVER] Unconverted MAGIC line(s): convert manually\n" + code
            prefix = {"sql": "%%sql\n", "scala": "%%spark\n", "r": "%%sparkr\n"}.get(lang, "")
            out.append(dict(kind="code", src=prefix + code))
    if ctx.params:
        out.insert(0, dict(kind="code", tags=["parameters"], src="\n".join(f"{k} = {json.dumps(v)}" for k, v in ctx.params.items())))
    return out


def build_ipynb(cells, header=""):
    nbc = []
    if header:
        nbc.append({"cell_type": "markdown", "metadata": {}, "source": header.splitlines(True)})
    for c in cells:
        if c["kind"] == "md":
            nbc.append({"cell_type": "markdown", "metadata": {}, "source": c["src"].splitlines(True)})
        else:
            meta = {"tags": c["tags"]} if c.get("tags") else {}
            nbc.append({"cell_type": "code", "execution_count": None, "metadata": meta, "outputs": [], "source": c["src"].splitlines(True)})
    return {"nbformat": 4, "nbformat_minor": 5, "cells": nbc, "metadata": {
        "kernel_info": {"name": "synapse_pyspark"},
        "kernelspec": {"name": "synapse_pyspark", "display_name": "Synapse PySpark"},
        "language_info": {"name": "python"},
        "microsoft": {"language": "python", "language_group": "synapse_pyspark"}}}


def outcome_from_ctx(ctx):
    if ctx.todos:
        return PART
    return TEST if (ctx.changes or ctx.warns) else MIG


def notes_from_ctx(item, ctx):
    for k, n in ctx.changes.items():
        item["actions"].append(f"{k} ({n}x)")
    for t in ctx.todos:
        item["reasons"].append(f"TODO {t['rule']}: {t['advice']}  [{t['line']}]")
    for k, (n, adv) in ctx.warns.items():
        item["reasons"].append(f"Test {k} ({n}x): {adv}")


# ---------------------------------------------------------------------------
# SOURCE ACCESS (offline folder or live Databricks)
# ---------------------------------------------------------------------------
class Dbx:
    def __init__(self, host, token):
        self.host, self.h = host.rstrip("/"), {"Authorization": f"Bearer {token}"}

    def get(self, ep, **p):
        url = self.host + ep + ("?" + urllib.parse.urlencode(p) if p else "")
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=self.h), timeout=90) as r:
                return json.loads(r.read() or b"{}")
        except Exception as e:
            return {"_error": str(e)}


EXT_LANG = {".py": "PYTHON", ".sql": "SQL", ".scala": "SCALA", ".r": "R", ".ipynb": "PYTHON"}


def get_source(a, args, dbx):
    path = a["meta"].get("source_path") or a["location"]
    if args.source_dir:
        root = Path(args.source_dir)
        rel = path.lstrip("/")
        cands = [root / (rel + e) for e in ("", ".py", ".sql", ".scala", ".r", ".ipynb")]
        cands += [p for p in root.rglob("*") if p.is_file() and p.stem == Path(rel).name.split(".")[0] and p.suffix.lower() in EXT_LANG]
        for p in cands:
            if p.is_file():
                return p.read_text("utf-8", "ignore"), a["meta"].get("language") or EXT_LANG.get(p.suffix.lower(), "PYTHON")
    if dbx:
        r = dbx.get("/api/2.0/workspace/export", path=path, format="SOURCE")
        if "content" in r:
            return base64.b64decode(r["content"]).decode("utf-8", "ignore"), a["meta"].get("language") or "PYTHON"
    return None


# ---------------------------------------------------------------------------
# JOBS -> DATA PIPELINES
# ---------------------------------------------------------------------------
WIN_TZ = {"Asia/Kolkata": "India Standard Time", "UTC": "UTC", "America/New_York": "Eastern Standard Time",
          "America/Chicago": "Central Standard Time", "America/Los_Angeles": "Pacific Standard Time",
          "Europe/London": "GMT Standard Time", "Europe/Berlin": "W. Europe Standard Time"}
DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
DOW3 = {d[:3].upper(): d for d in DAYS}
Q_NUM = {"1": "Sunday", "2": "Monday", "3": "Tuesday", "4": "Wednesday", "5": "Thursday", "6": "Friday", "7": "Saturday"}


def _dow(expr):
    out = []
    for part in expr.upper().split(","):
        if "-" in part:
            a, b = part.split("-")
            names = [DOW3.get(a[:3]) or Q_NUM.get(a), DOW3.get(b[:3]) or Q_NUM.get(b)]
            if None in names:
                return None
            i, j = DAYS.index(names[0]), DAYS.index(names[1])
            out += DAYS[i:j + 1] if i <= j else DAYS[i:] + DAYS[:j + 1]
        else:
            d = DOW3.get(part[:3]) or Q_NUM.get(part)
            if not d:
                return None
            out.append(d)
    return sorted(set(out), key=DAYS.index)


def quartz_to_fabric(expr):
    p = (expr or "").split()
    if len(p) < 6:
        return None, "cron not parseable"
    _, mi, hr, dom, mon, dow = p[:6]
    anyd = lambda x: x in ("*", "?")
    if mon != "*" or not anyd(dom) and not anyd(dow):
        return None, "monthly/complex cron needs a manual schedule"
    m = re.fullmatch(r"(?:0/|\*/)(\d+)", mi)
    if m and hr == "*" and anyd(dom) and anyd(dow):
        return {"type": "Cron", "interval": int(m.group(1))}, ""
    m = re.fullmatch(r"(?:0/|\*/)(\d+)", hr)
    if mi.isdigit() and m and anyd(dom) and anyd(dow):
        return {"type": "Cron", "interval": int(m.group(1)) * 60}, "hourly interval approximated (start offset not kept)"
    if mi.isdigit() and re.fullmatch(r"\d+(,\d+)*", hr):
        times = [f"{int(h):02d}:{int(mi):02d}" for h in hr.split(",")]
        if anyd(dom) and anyd(dow):
            return {"type": "Daily", "times": times}, ""
        if anyd(dom):
            days = _dow(dow)
            if days:
                return {"type": "Weekly", "weekdays": days, "times": times}, ""
    return None, "cron pattern not supported by automatic conversion"


def fab_timeout(sec):
    sec = int(sec or 0) or 12 * 3600
    d, r = divmod(sec, 86400)
    return f"{d}.{r // 3600:02d}:{r % 3600 // 60:02d}:{r % 60:02d}"


SKIP_TASK = {"pipeline_task": "DLT pipeline: no Fabric equivalent (redesign)", "dbt_task": "dbt task: handled as separate dbt Project item",
             "sql_task": "SQL task: recreate as Script/Warehouse activity", "spark_python_task": "Spark Python task: create a Spark Job Definition",
             "python_wheel_task": "Wheel task: Spark Job Definition + Environment", "spark_jar_task": "JAR task: Spark Job Definition",
             "spark_submit_task": "spark-submit task: rewrite as notebook / Spark Job Definition", "run_job_task": "Run Job task: wire an Invoke/Execute Pipeline activity manually",
             "condition_task": "Condition task: add an If Condition activity manually", "for_each_task": "For-each task: add a ForEach activity manually"}


def convert_job(a, cfg):
    s = a["meta"].get("settings", {})
    acts, kept, notes, omitted = [], set(), [], []
    tasks = s.get("tasks", [])
    for t in tasks:
        if "notebook_task" in t:
            kept.add(t["task_key"])
    for t in tasks:
        k = t["task_key"]
        if k not in kept:
            kind = next((x for x in SKIP_TASK if x in t), "unknown")
            omitted.append(f"{k}: {SKIP_TASK.get(kind, 'unsupported task type')}")
            continue
        nt = t["notebook_task"]
        dep, lost = [], []
        for d in t.get("depends_on", []):
            (dep if d["task_key"] in kept else lost).append(d["task_key"])
        if lost:
            notes.append(f"{k}: dependency on omitted task(s) {lost} was dropped")
        tp = {"notebookId": f"@@NB:{nb_name(nt.get('notebook_path', ''))}@@", "workspaceId": "@@WS@@"}
        if nt.get("base_parameters"):
            tp["parameters"] = {pk: {"value": str(pv), "type": "string"} for pk, pv in nt["base_parameters"].items()}
        acts.append({"name": k, "type": "TridentNotebook",
                     "dependsOn": [{"activity": d, "dependencyConditions": ["Succeeded"]} for d in dep],
                     "policy": {"timeout": fab_timeout(t.get("timeout_seconds")), "retry": int(t.get("max_retries") or 0),
                                "retryIntervalInSeconds": max(30, int((t.get("min_retry_interval_millis") or 30000) / 1000)),
                                "secureInput": False, "secureOutput": False},
                     "typeProperties": tp})
    pipe = {"properties": {"description": f"Migrated from Databricks job '{a['name']}'", "activities": acts}} if acts else None
    if s.get("continuous"):
        notes.append("continuous job: Fabric pipelines are not continuous; use a streaming notebook or frequent schedule")
    if s.get("email_notifications") or s.get("webhook_notifications"):
        notes.append("notifications: add Teams/Outlook activities on the failure path")
    if s.get("job_clusters") or any(t.get("libraries") for t in tasks):
        notes.append("cluster/library settings: attach a Fabric Environment to the notebooks")
    sched = None
    if s.get("schedule") and pipe:
        conf, note = quartz_to_fabric(s["schedule"].get("quartz_cron_expression"))
        if note:
            notes.append("schedule: " + note)
        if conf:
            tz = WIN_TZ.get(s["schedule"].get("timezone_id"), cfg["timezone"])
            start = datetime.now().replace(microsecond=0, second=0)
            conf.update({"startDateTime": start.strftime("%Y-%m-%dT%H:%M:%S"),
                         "endDateTime": (start + timedelta(days=3650)).strftime("%Y-%m-%dT%H:%M:%S"), "localTimeZoneId": tz})
            sched = {"enabled": s["schedule"].get("pause_status") != "PAUSED", "configuration": conf}
    return pipe, sched, notes, omitted


# ---------------------------------------------------------------------------
# TABLE / DATA HELPERS
# ---------------------------------------------------------------------------
def parse_abfss(loc):
    m = re.match(r"abfss?://([^@/]+)@([^/]+)(/.*)?$", loc or "")
    if m:
        return f"https://{m.group(2)}", f"/{m.group(1)}{m.group(3) or ''}"
    m = re.match(r"https://([^/]+\.dfs\.core\.windows\.net)(/.*)$", loc or "")
    return (f"https://{m.group(1)}", m.group(2)) if m else None


def route_table(a, cfg):
    cat, sch, _ = (a["name"].split(".") + ["", "", ""])[:3]
    m = a.get("meta", {})
    tt, loc = m.get("table_type"), m.get("storage_location") or ""
    lh = cfg["catalog_to_lakehouse"].get(cat, cfg["lakehouse_default"])
    tgt = ("Warehouse", cfg["warehouse_name"]) if sch.lower() in [x.lower() for x in cfg["warehouse_schemas"]] else ("Lakehouse", lh)
    if tt == "VIEW":
        return tgt + ("view",)
    if tt in ("MATERIALIZED_VIEW", "STREAMING_TABLE", "FOREIGN") or not tt:
        return tgt + ("manual",)
    if parse_abfss(loc):
        return tgt + ("shortcut" if tt == "EXTERNAL" else "copy",)
    return tgt + ("manual",)


def is_default_managed(loc):
    """Databricks-managed (default) UC storage: not reachable from outside Databricks."""
    if "__unitystorage" in (loc or ""):
        return True
    ph = parse_abfss(loc)
    return bool(ph and ph[0].split("//")[1].startswith("dbstorage"))


def tsql_type(t):
    t = (t or "").lower().strip()
    m = re.match(r"decimal\((\d+),\s*(\d+)\)", t)
    if m:
        return f"DECIMAL({m.group(1)},{m.group(2)})"
    return {"int": "INT", "integer": "INT", "bigint": "BIGINT", "smallint": "SMALLINT", "tinyint": "SMALLINT", "double": "FLOAT", "float": "REAL",
            "string": "VARCHAR(8000)", "boolean": "BIT", "date": "DATE", "timestamp": "DATETIME2(6)", "decimal": "DECIMAL(18,0)"}.get(t, t.upper() + " /* verify type */")


CONN_REASON = "adls_connection_id not set: create an ADLS Gen2 connection in Fabric and put its id in mapping.json (shortcut spec is ready)"


def resolve(out, p):
    """Artifact paths in ledger.json may come from another machine: re-anchor them under the output folder."""
    q = Path(p)
    if q.exists():
        return q
    parts = re.split(r"[\\/]", str(p))
    for key in ("artifacts", "ai_tasks"):
        if key in parts:
            return Path(out).joinpath(*parts[parts.index(key):])
    return q


def code_cell(src):
    return dict(kind="code", src=src)


def copy_notebook(lh, tables):
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
    return [dict(kind="md", src=f"# Physical copy into lakehouse `{lh}`\nAttach `{lh}` as default lakehouse. Reads each source Delta path (needs storage access) and validates row counts."),
            code_cell(cfg_cell), code_cell(run)]


# ---------------------------------------------------------------------------
# dbt
# ---------------------------------------------------------------------------
DBT_FLAGS = [(r"file_format\s*[=:]\s*['\"]?delta", "file_format=delta is Databricks-specific"),
             (r"liquid_clustered_by|databricks_compute|\+?location_root|\+?catalog\b", "Databricks-specific dbt config"),
             (r"dbt[-_]databricks", "dbt-databricks adapter reference"),
             (r"\bZORDER\b|\bOPTIMIZE\b", "Databricks maintenance SQL"),
             (r"\b\w+\.\w+\.\w+\b(?=\s*(?:\n|$|\)))", "possible 3-part (Unity Catalog) name")]


def convert_dbt(job_name, src_dir, cfg, outdir):
    src, dst = Path(src_dir), Path(outdir) / "artifacts" / "dbt" / safe(job_name)
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("target", "dbt_packages", "logs", ".git"))
    issues = []
    for f in list(dst.rglob("*.sql")) + list(dst.rglob("*.yml")):
        if f.name == "profiles.yml":
            continue
        text = f.read_text("utf-8", "ignore")
        for rx, msg in DBT_FLAGS:
            if re.search(rx, text, re.I | re.M):
                issues.append(f"{f.relative_to(dst)}: {msg}")
    old = dst / "profiles.yml"
    if old.exists():
        old.rename(dst / "profiles.databricks.yml")
    prof = "default"
    pj = dst / "dbt_project.yml"
    if pj.exists():
        m = re.search(r"^profile:\s*['\"]?([\w\-]+)", pj.read_text("utf-8"), re.M)
        prof = m.group(1) if m else prof
    if cfg["dbt_target"] == "lakehouse":
        out = f"{prof}:\n  target: fabric\n  outputs:\n    fabric:\n      type: fabricspark   # dbt-fabricspark (Livy)\n      method: livy\n      authentication: CLI\n      endpoint: https://api.fabric.microsoft.com/v1\n      workspaceid: {cfg['workspace_id'] or '<workspace-id>'}\n      lakehouseid: <lakehouse-id>\n      lakehouse: {cfg['lakehouse_default']}\n      schema: dbt\n"
    else:
        out = f"{prof}:\n  target: fabric\n  outputs:\n    fabric:\n      type: fabric   # dbt-fabric (Fabric Data Warehouse)\n      driver: 'ODBC Driver 18 for SQL Server'\n      server: <warehouse-sql-endpoint>\n      database: {cfg['warehouse_name']}\n      schema: dbt\n      authentication: CLI\n"
    (dst / "profiles.yml").write_text(out, "utf-8")
    return str(dst), sorted(set(issues))


# ---------------------------------------------------------------------------
# PLAN
# ---------------------------------------------------------------------------
def new_item(a, ttype, tname, strategy):
    return {"id": f"{a['type']}:{a['name']}", "type": a["type"], "name": a["name"], "target_type": ttype, "target_name": tname,
            "strategy": strategy, "outcome": "planned", "deployment": "pending", "reasons": [], "actions": [], "tests": [],
            "artifacts": [], "target_id": None, "assessment_score": a.get("score"), "assessment_status": a.get("bucket")}


ROUTES = {
    "DLT Pipeline": ("Materialized Lake Views / Notebooks", "manual"), "Cluster": ("Environment", "environment"),
    "Library": ("Environment", "environment"), "SQL Warehouse": ("Warehouse", "infra"), "Secret Scope": ("Azure Key Vault", "manual"),
    "Repo": ("Git integration", "manual"), "Dashboard": ("Power BI report", "manual"), "Alert": ("Activator", "manual"),
    "ML Model": ("Fabric ML model", "manual"), "Serving Endpoint": ("Azure ML / AI Foundry", "manual"),
    "Credential": ("Workspace identity / connection", "manual"), "Permission": ("Fabric roles / OneLake security", "manual"),
    "Function": ("Warehouse function / notebook", "function"), "SQL Query": ("Notebook", "queries"),
    "DBFS Mount": ("OneLake shortcut / Files path", "mount"), "External Location": ("ADLS connection", "extloc"),
    "Volume": ("Lakehouse", "volume"), "Connection": ("Fabric connection / SQL database", "connection"),
}


def plan(assess, cfg):
    items = []
    for a in assess["assets"]:
        t = a["type"]
        if t == "Notebook":
            items.append(new_item(a, "Notebook", safe(a["name"]), "convert"))
        elif t == "Job":
            items.append(new_item(a, "Data Pipeline", "pl_" + safe(a["name"]), "convert"))
            if any("dbt_task" in x for x in a["meta"].get("settings", {}).get("tasks", [])):
                items.append(new_item({"type": "dbt Project", "name": a["name"]}, "dbt job", safe(a["name"]), "dbt"))
        elif t == "Table/View":
            items.append(new_item(a, *route_table(a, cfg)))
        elif t in ROUTES:
            tt, st = ROUTES[t]
            tname = ""
            if t == "SQL Warehouse":
                tname = cfg["warehouse_name"]
            elif t == "Connection" and (a["meta"].get("connection_type") or "").upper() in cfg["sqldb_connection_types"]:
                tt, tname = "SQL database", cfg["sqldb_name"]
            items.append(new_item(a, tt, tname, st))
        else:
            items.append(new_item(a, "-", "-", "manual"))
    return items


# ---------------------------------------------------------------------------
# CONVERT
# ---------------------------------------------------------------------------
TESTS = {
    "Notebook": "Run with representative parameters; compare outputs/row counts with the Databricks run",
    "Data Pipeline": "Trigger a run; confirm activity order, parameters, retries and the schedule time",
    "shortcut": "Query the shortcut table; compare row counts and schema with the source",
    "copy": "Run the copy notebook; confirm every table reports OK and spot-check data",
    "view": "Run the view and compare results with Databricks for sample filters",
}


def ai_complete(prompt):
    key = os.getenv("ANTHROPIC_API_KEY")
    if not key:
        raise RuntimeError("ANTHROPIC_API_KEY not set")
    body = json.dumps({"model": os.getenv("DBX2FABRIC_AI_MODEL", "claude-sonnet-4-6"), "max_tokens": 4000,
                       "messages": [{"role": "user", "content": prompt}]}).encode()
    req = urllib.request.Request("https://api.anthropic.com/v1/messages", data=body, headers={
        "x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return "".join(b.get("text", "") for b in json.loads(r.read())["content"])


AI_PROMPT = """You are migrating a Databricks notebook cell to Microsoft Fabric.
Rules: code stays in Fabric notebooks (PySpark/Spark SQL); use notebookutils instead of dbutils; data lives in Lakehouse (Delta),
OLAP warehouse tables in Fabric Warehouse, OLTP in Fabric SQL database; orchestration is a Data Pipeline; ingestion is Copy Data / Copy job.
Resolve every TODO[dbx2fabric:...] marker in the cell below. Keep behaviour identical. If something has no Fabric equivalent, implement the closest
pattern and explain in a comment. Return ONLY the corrected cell code inside one ``` fence.

TODO markers: {todos}

CELL:
```
{cell}
```"""


def ai_fix_cells(cells, ctx):
    fixed = 0
    for c in cells:
        if c["kind"] == "code" and "TODO[dbx2fabric:" in c["src"]:
            try:
                txt = ai_complete(AI_PROMPT.format(todos=", ".join(sorted({t["rule"] for t in ctx.todos})), cell=c["src"]))
                m = re.search(r"```(?:\w+)?\n(.*?)```", txt, re.S)
                if m:
                    c["src"] = m.group(1).rstrip("\n")
                    fixed += 1
            except Exception as e:
                log("  AI step failed:", e)
    return fixed


def convert_all(out, assess, cfg, args):
    out = Path(out)
    ledger = load_json(out / "ledger.json")
    assets = {f"{a['type']}:{a['name']}": a for a in assess["assets"]}
    items = ledger["items"]
    catalogs = sorted({a["name"].split(".")[0] for a in assess["assets"] if a["type"] in ("Table/View", "Volume") and "." in a["name"]})
    dbx = Dbx(args.host, args.token) if getattr(args, "host", None) and getattr(args, "token", None) else None
    art, tasks_dir = [], out / "ai_tasks"
    shutil.rmtree(out / "artifacts", ignore_errors=True)
    shutil.rmtree(tasks_dir, ignore_errors=True)
    adir = out / "artifacts"
    agg = {"shortcuts": [], "copy": {}, "views": {}, "wh_tables": [], "wh_views": [], "queries": [], "env": [], "kv": [], "perm": [], "copy_specs": [],
           "export": [], "export_vol": [], "funcs": [], "wh_funcs": [], "validate": []}

    def fail(it, reason, outcome=NOT):
        it["outcome"] = outcome
        it["reasons"].append(reason)

    for it in items:
        a = assets.get(it["id"], {"type": it["type"], "name": it["name"], "meta": {}, "location": ""})
        m, st = a.get("meta", {}), it["strategy"]
        it.update(actions=[], reasons=[], tests=[], artifacts=[], deployment="n/a")
        try:
            if it["type"] == "Notebook":
                src = get_source(a, args, dbx)
                if not src:
                    fail(it, "Source code not available: pass --source-dir (exported notebooks) or Databricks --host/--token")
                    continue
                cells = parse_source(*src)
                ctx = Ctx(cfg, catalogs)
                oc = convert_cells(cells, ctx, norm_path(a["location"]))
                if getattr(args, "ai", False) and ctx.todos:
                    n = ai_fix_cells(oc, ctx)
                    if n:
                        rem = sum(1 for c in oc if "TODO[dbx2fabric:" in c.get("src", ""))
                        it["actions"].append(f"AI-converted {n} cell(s) - review required")
                        if not rem:
                            ctx.todos = []
                nb = build_ipynb(oc, f"**Migrated from Databricks:** `{a['location']}` by dbx2fabric {VERSION}")
                p = adir / "notebooks" / f"{it['target_name']}.ipynb"
                save_json(p, nb)
                lh = ctx.lakehouses[0] if ctx.lakehouses else cfg["lakehouse_default"]
                art.append({"kind": "notebook", "name": it["target_name"], "path": str(p), "lakehouse": lh, "items": [it["id"]]})
                it["artifacts"].append(str(p))
                it["outcome"] = outcome_from_ctx(ctx)
                if it["outcome"] == TEST and any("AI-converted" in x for x in it["actions"]):
                    it["outcome"] = TEST
                notes_from_ctx(it, ctx)
                it["tests"].append(TESTS["Notebook"])
                if ctx.params:
                    it["tests"].append("Check the parameter cell defaults: " + ", ".join(ctx.params))
                if ctx.secrets:
                    it["tests"].append("Create secrets in Key Vault: " + ", ".join(sorted(k for _, k in ctx.secrets)))
                it["deployment"] = "generated"
            elif it["type"] == "Job":
                pipe, sched, notes, omitted = convert_job(a, cfg)
                if not pipe:
                    fail(it, "No notebook tasks to convert; " + "; ".join(omitted or ["empty job"]))
                    continue
                p = adir / "pipelines" / f"{it['target_name']}.json"
                save_json(p, pipe)
                it["artifacts"].append(str(p))
                entry = {"kind": "pipeline", "name": it["target_name"], "path": str(p), "items": [it["id"]]}
                if sched:
                    sp = adir / "pipelines" / f"{it['target_name']}.schedule.json"
                    save_json(sp, sched)
                    entry["schedule"] = str(sp)
                    it["actions"].append(f"schedule converted ({sched['configuration']['type']})")
                art.append(entry)
                it["actions"].append(f"{len(pipe['properties']['activities'])} notebook activities generated")
                it["reasons"] += notes + [f"Task not migrated - {o}" for o in omitted]
                it["outcome"] = PART if omitted else TEST
                it["tests"].append(TESTS["Data Pipeline"])
                it["deployment"] = "generated"
            elif it["type"] == "dbt Project":
                d = cfg["dbt_projects"].get(it["name"])
                if not d or not Path(d).exists():
                    fail(it, f"dbt project folder not provided: set mapping.json dbt_projects['{it['name']}']")
                    continue
                path, issues = convert_dbt(it["name"], d, cfg, out)
                it["artifacts"].append(path)
                it["actions"].append(f"profiles.yml retargeted to {'dbt-fabricspark (Lakehouse)' if cfg['dbt_target'] == 'lakehouse' else 'dbt-fabric (Warehouse)'}")
                it["reasons"] += issues + ["Create a dbt job item in Fabric (Data Factory) from this folder; dbt job support is preview/GA-rolling - check current status"]
                it["outcome"] = PART if issues else TEST
                it["tests"].append("dbt build in Fabric; compare model row counts and test results with Databricks")
                it["deployment"] = "generated"
            elif it["type"] == "Table/View":
                cat, sch, tbl = (a["name"].split(".") + ["", "", ""])[:3]
                lh = cfg["catalog_to_lakehouse"].get(cat, cfg["lakehouse_default"])
                wh = it["target_type"] == "Warehouse"
                if st == "manual":
                    fail(it, "Materialized/streaming/foreign table, or storage location unavailable (non-ADLS or assessment lacks metadata)")
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
                    it["reasons"].append("Spark SQL dialect: verify results" + (" and translate to T-SQL for Warehouse" if wh else ""))
                    it["tests"].append(TESTS["view"])
                    if not wh:
                        agg["validate"].append({"id": it["id"], "source": a["name"], "target": f"{sch}.{tbl}", "lakehouse": lh, "kind": "view"})
                    it["deployment"] = "generated"
                else:
                    loc = m["storage_location"]
                    fmt = (m.get("format") or "").upper()
                    staged = is_default_managed(loc) or (st == "copy" and not cfg["allow_direct_managed_copy"])
                    spath = f"{cfg['staging_storage'].rstrip('/')}/tables/{sch}/{tbl}" if cfg["staging_storage"] else ""
                    if staged and not spath:
                        fail(it, "Fabric cannot read Databricks/UC-managed storage directly. Set mapping.json staging_storage (an ADLS Gen2 container you own), re-run convert, then run databricks/export_to_staging.py in Databricks")
                        continue
                    if staged:
                        agg["export"].append({"source": a["name"], "path": spath, "partitions": m.get("partition_columns", [])})
                        loc = spath
                        it["actions"].append("Staged: Databricks export notebook writes a Delta copy to your ADLS container")
                    host, sub = parse_abfss(loc)
                    if st == "shortcut" and not staged:
                        agg["shortcuts"].append({"item": it["id"], "lakehouse": lh, "path": "Tables/" + sch if fmt == "DELTA" else "Files/" + sch, "name": tbl,
                                                 "target": {"adlsGen2": {"location": host, "subpath": sub, "connectionId": cfg["adls_connection_id"]}}})
                        it["outcome"] = TEST if cfg["adls_connection_id"] else PART
                        if not cfg["adls_connection_id"]:
                            it["reasons"].append(CONN_REASON)
                        it["actions"].append("OneLake shortcut (logical migration, no data copy)")
                        if fmt != "DELTA":
                            it["reasons"].append(f"{fmt} data exposed under Files/: convert to a Delta table with a notebook")
                        it["tests"].append(TESTS["shortcut"])
                    else:
                        agg["copy"].setdefault(lh, []).append({"item": it["id"], "source": loc, "schema": sch, "table": tbl, "partitions": m.get("partition_columns", [])})
                        it["outcome"] = TEST
                        it["actions"].append("Physical copy notebook (reads Delta path, writes Lakehouse table, validates row counts)")
                        it["tests"].append(TESTS["copy"])
                    if fmt == "DELTA" or staged:
                        agg["validate"].append({"id": it["id"], "source": a["name"], "target": f"{sch}.{tbl}", "lakehouse": lh, "kind": "table"})
                    if wh:
                        agg["wh_tables"].append((sch, tbl, lh))
                        it["actions"].append(f"CTAS into Warehouse {cfg['warehouse_name']} (load_tables.sql)")
                    it["deployment"] = "generated"
            elif st == "queries":
                ctx = Ctx(cfg, catalogs)
                body = convert_code(m.get("query_text") or "", "sql", ctx)
                agg["queries"].append((it["name"], body))
                it["outcome"] = PART if ctx.todos else TEST
                notes_from_ctx(it, ctx)
                it["reasons"].append("Spark SQL dialect: verify in Spark (notebook) or translate for Warehouse")
                it["tests"].append("Run the cell; compare results with Databricks")
                it["deployment"] = "generated"
            elif st == "environment":
                if it["type"] == "Library":
                    kind, name = m.get("kind"), m.get("name")
                    if kind == "pypi":
                        agg["env"].append(name)
                        it["outcome"] = TEST
                        it["actions"].append("added to environment.yml")
                        it["tests"].append("Import the package in a notebook attached to the Environment")
                        it["deployment"] = "generated"
                    else:
                        fail(it, f"{kind} library: upload the artifact to the Fabric Environment manually" + (" (Maven must be downloaded as a jar)" if kind == "maven" else ""))
                else:
                    it["outcome"] = TEST
                    it["actions"].append("Spark settings noted; create a Fabric Environment and pool of similar size")
                    it["reasons"].append(f"Runtime {m.get('spark_version')} -> choose a Fabric runtime (Spark 3.5+); custom spark_conf: {list((m.get('spark_conf') or {}))[:5]}")
                    it["tests"].append("Attach the Environment to a migrated notebook and run it")
                    it["deployment"] = "generated"
            elif st == "infra":
                it["outcome"] = MIG
                it["actions"].append(f"Fabric Warehouse '{cfg['warehouse_name']}' (compute-equivalent; items created on deploy)")
            elif st == "mount":
                if a["name"] in cfg["mount_map"]:
                    it["outcome"] = TEST
                    it["actions"].append(f"{a['name']} -> {cfg['mount_map'][a['name']]} (paths rewritten in notebooks)")
                    it["tests"].append("Confirm the target folder exists in the lakehouse Files section and holds the data")
                else:
                    fail(it, "No mount_map entry: add it to mapping.json")
            elif st == "extloc":
                if cfg["adls_connection_id"]:
                    it["outcome"] = TEST
                    it["actions"].append("Used by shortcuts via adls_connection_id")
                    it["tests"].append("Confirm the Fabric connection can read " + str(m.get("url")))
                else:
                    fail(it, "Create an ADLS Gen2 connection in Fabric and set adls_connection_id in mapping.json")
            elif st == "volume":
                loc = m.get("storage_location") or ""
                cat, sch, vol = (a["name"].split(".") + ["", "", ""])[:3]
                lh = cfg["catalog_to_lakehouse"].get(cat, cfg["lakehouse_default"])
                if not parse_abfss(loc):
                    fail(it, "Volume storage location is not ADLS Gen2: copy files into OneLake")
                    continue
                if is_default_managed(loc):
                    if not cfg["staging_storage"]:
                        fail(it, "Volume is in Databricks-managed storage that Fabric cannot read. Set mapping.json staging_storage, re-run convert, then run databricks/export_to_staging.py")
                        continue
                    loc = f"{cfg['staging_storage'].rstrip('/')}/volumes/{sch}_{vol}"
                    agg["export_vol"].append({"source": f"/Volumes/{cat}/{sch}/{vol}", "path": loc})
                    it["actions"].append("Staged: Databricks export notebook copies the files to your ADLS container")
                host, sub = parse_abfss(loc)
                agg["shortcuts"].append({"item": it["id"], "lakehouse": lh, "path": "Files", "name": f"{sch}_{vol}",
                                         "target": {"adlsGen2": {"location": host, "subpath": sub, "connectionId": cfg["adls_connection_id"]}}})
                it["outcome"] = TEST if cfg["adls_connection_id"] else PART
                if not cfg["adls_connection_id"]:
                    it["reasons"].append(CONN_REASON)
                it["actions"].append("Files shortcut")
                it["tests"].append(TESTS["shortcut"])
                it["deployment"] = "generated"
            elif st == "connection":
                ctype = (m.get("connection_type") or "").upper()
                agg["copy_specs"].append({"connection": a["name"], "type": ctype, "target": it["target_type"], "target_name": it["target_name"],
                                          "tool": "Copy job" if it["target_type"] == "SQL database" else "Copy Data (pipeline)",
                                          "note": "create Fabric connection, then list source tables"})
                fail(it, "Credentials cannot be exported: create the Fabric connection manually; spec written to ingestion/copy_specs.json")
            elif st == "function":
                params, ret, body = m.get("input_params"), m.get("return_type"), m.get("definition")
                if m.get("language") != "SQL" or m.get("is_table") or params is None or not ret or not body:
                    fail(it, MANUAL_NOTES["Function"])
                    continue
                if not params:
                    # no parameters captured, but the body uses names that are not SQL keywords or function calls
                    names = {w.upper() for w in re.findall(r"\b([A-Za-z_]\w*)\b(?!\s*\()", re.sub(r"'[^']*'", "", body))}
                    if names - SQL_WORDS:
                        fail(it, "Function body uses " + ", ".join(sorted(names - SQL_WORDS)).lower() + " but no parameters were captured: re-run the assessor or recreate the signature by hand")
                        continue
                cat, sch, fn = (a["name"].split(".") + ["", "", ""])[:3]
                ctx = Ctx(cfg, catalogs)
                body = convert_code(body, "sql", ctx)
                if sch.lower() in [x.lower() for x in cfg["warehouse_schemas"]]:
                    tb = body
                    for pr in params:
                        tb = re.sub(r"\b" + re.escape(pr["name"]) + r"\b", "@" + pr["name"], tb)
                    agg["wh_funcs"].append(f"CREATE OR ALTER FUNCTION [{sch}].[{fn}]({', '.join('@' + pr['name'] + ' ' + tsql_type(pr['type']) for pr in params)})\nRETURNS {tsql_type(ret)}\nAS\nBEGIN\n    RETURN ({tb});\nEND;\nGO")
                    it["reasons"].append("T-SQL draft in warehouse/load_tables.sql: verify types and that the Spark SQL body is valid T-SQL")
                    it["actions"].append("Scalar SQL function -> Warehouse T-SQL function (draft)")
                else:
                    agg["funcs"].append((fn, f"CREATE OR REPLACE TEMPORARY FUNCTION {fn}({', '.join(pr['name'] + ' ' + pr['type'] for pr in params)})\nRETURNS {ret}\nRETURN {body}"))
                    it["reasons"].append("Spark temporary function (session scoped): run the Migrated_Functions notebook (or %run it) before notebooks that call it")
                    it["actions"].append("Scalar SQL function -> Spark temporary function")
                notes_from_ctx(it, ctx)
                it["outcome"] = PART if ctx.todos else TEST
                it["tests"].append(f"Call {fn}() with sample inputs and compare with Databricks")
                it["deployment"] = "generated"
            elif it["type"] == "Secret Scope":
                for k in m.get("keys", []):
                    agg["kv"].append((m.get("scope"), k, cfg["keyvault_urls"].get(m.get("scope"), cfg["keyvault_url_default"])))
                fail(it, "Secret values cannot be exported: create the secrets in Key Vault (names in security/keyvault_secrets.csv)")
            elif it["type"] == "Permission":
                for g in m.get("grants", []):
                    agg["perm"].append((m.get("securable"), g.get("principal"), ";".join(g.get("privileges", []))))
                fail(it, "Map principals to Fabric workspace roles / OneLake security (matrix in security/permissions_matrix.csv)")
            else:
                fail(it, MANUAL_NOTES.get(it["type"], f"No automated path for {it['type']}: manual migration"))
        except Exception as e:
            fail(it, f"Conversion error: {e}")
        finally:
            if it["outcome"] in (NOT, PART):
                write_ai_task(it, a, tasks_dir, cfg)

    # aggregated artifacts
    for lh, tabs in agg["copy"].items():
        p = adir / "notebooks" / f"copy_tables_{safe(lh)}.ipynb"
        save_json(p, build_ipynb(copy_notebook(lh, [{k: v for k, v in t.items() if k != "item"} for t in tabs])))
        art.append({"kind": "notebook", "name": f"copy_tables_{safe(lh)}", "path": str(p), "lakehouse": lh, "items": [t["item"] for t in tabs], "generated_only": True})
    for lh, views in agg["views"].items():
        cells = [dict(kind="md", src=f"# Views for lakehouse `{lh}`")] + [code_cell(f'spark.sql("""CREATE OR REPLACE VIEW {sch}.{v} AS\n{body}""")') for _, sch, v, body in views]
        p = adir / "notebooks" / f"create_views_{safe(lh)}.ipynb"
        save_json(p, build_ipynb(cells))
        art.append({"kind": "notebook", "name": f"create_views_{safe(lh)}", "path": str(p), "lakehouse": lh, "items": [v[0] for v in views]})
    if agg["export"] or agg["export_vol"]:
        txt = ("# Databricks notebook source\n# MAGIC %md\n# MAGIC # Export managed tables / volumes to your ADLS Gen2 staging container (run in DATABRICKS)\n"
               "# MAGIC Fabric cannot read Databricks-managed storage. This copies data to `staging_storage`; Fabric then copies or shortcuts from there.\n\n# COMMAND ----------\n\n"
               "TABLES = " + json.dumps(agg["export"], indent=2) + "\nVOLUMES = " + json.dumps(agg["export_vol"], indent=2) + "\n\n# COMMAND ----------\n\n"
               "for t in TABLES:\n    w = spark.table(t['source']).write.format('delta').mode('overwrite').option('overwriteSchema', 'true')\n"
               "    if t['partitions']:\n        w = w.partitionBy(*t['partitions'])\n    w.save(t['path'])\n    print('exported', t['source'], '->', t['path'])\n"
               "for v in VOLUMES:\n    dbutils.fs.cp(v['source'], v['path'], recurse=True)\n    print('copied', v['source'], '->', v['path'])\n")
        p = adir / "databricks" / "export_to_staging.py"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(txt, "utf-8")
        art.append({"kind": "databricks_notebook", "name": "databricks/export_to_staging.py", "path": str(p), "items": []})
    if agg["funcs"]:
        cells = [dict(kind="md", src="# Migrated SQL functions (Spark temporary functions)")] + [code_cell("%%sql\n" + d) for _, d in agg["funcs"]]
        p = adir / "notebooks" / "Migrated_Functions.ipynb"
        save_json(p, build_ipynb(cells))
        art.append({"kind": "notebook", "name": "Migrated_Functions", "path": str(p), "lakehouse": cfg["lakehouse_default"],
                    "items": [i["id"] for i in items if i["type"] == "Function" and i["outcome"] in (TEST, PART) and "Spark temporary" in " ".join(i["actions"])]})
    if agg["queries"]:
        cells = [dict(kind="md", src="# Migrated SQL queries")]
        for n, body in agg["queries"]:
            cells += [dict(kind="md", src=f"## {n}"), code_cell("%%sql\n" + body)]
        p = adir / "notebooks" / "Migrated_SQL_Queries.ipynb"
        save_json(p, build_ipynb(cells))
        art.append({"kind": "notebook", "name": "Migrated_SQL_Queries", "path": str(p), "lakehouse": cfg["lakehouse_default"], "items": []})
    if agg["wh_tables"] or agg["wh_views"] or agg["wh_funcs"]:
        sql = [f"-- Run in Fabric Warehouse '{cfg['warehouse_name']}'. Data must already be in the lakehouse (copy/shortcut first)."]
        for sch in sorted({s for s, _, _ in agg["wh_tables"]} | {v[1] for v in agg["wh_views"]} | set(re.findall(r"FUNCTION \[(\w+)\]", "\n".join(agg["wh_funcs"])))):
            sql.append(f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{sch}') EXEC('CREATE SCHEMA [{sch}]');\nGO")
        for sch, tbl, lh in agg["wh_tables"]:
            sql.append(f"DROP TABLE IF EXISTS [{sch}].[{tbl}];\nCREATE TABLE [{sch}].[{tbl}] AS SELECT * FROM [{lh}].[{sch}].[{tbl}];\nGO")
        sql += agg["wh_funcs"]
        for _, sch, v, body, lh in agg["wh_views"]:
            sql.append(f"-- Spark SQL body: translate to T-SQL\nCREATE OR ALTER VIEW [{sch}].[{v}] AS\n{body};\nGO")
        p = adir / "warehouse" / "load_tables.sql"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("\n\n".join(sql), "utf-8")
        art.append({"kind": "script", "name": "warehouse/load_tables.sql", "path": str(p), "items": []})
    if agg["shortcuts"]:
        p = adir / "lakehouse" / "shortcuts.json"
        save_json(p, agg["shortcuts"])
        art.append({"kind": "shortcuts", "name": "shortcuts", "path": str(p), "items": [s["item"] for s in agg["shortcuts"]]})
    if agg["env"]:
        p = adir / "environment" / "environment.yml"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("dependencies:\n  - pip:\n" + "".join(f"    - {x}\n" for x in sorted(set(agg["env"]))), "utf-8")
        art.append({"kind": "environment", "name": "environment.yml", "path": str(p), "items": []})
    for name, rows, hdr in (("keyvault_secrets.csv", agg["kv"], ["scope", "secret_name", "key_vault_url"]),
                            ("permissions_matrix.csv", agg["perm"], ["securable", "principal", "privileges"])):
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
    for e in art:
        for iid in e.get("items", []):
            if iid in byid and e["path"] not in byid[iid]["artifacts"]:
                byid[iid]["artifacts"].append(e["path"])
    ledger.update(artifacts=art, converted=datetime.now().isoformat(timespec="seconds"))
    save_json(out / "ledger.json", ledger)
    write_runbook(out, cfg, ledger)
    return ledger


SQL_WORDS = {"CASE", "WHEN", "THEN", "ELSE", "END", "AND", "OR", "NOT", "NULL", "TRUE", "FALSE", "IS", "IN", "AS",
             "BETWEEN", "LIKE", "DISTINCT", "INTERVAL", "SELECT", "FROM", "WHERE", "DIV", "MOD",
             "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "STRING", "DOUBLE", "FLOAT", "DECIMAL", "DATE", "TIMESTAMP", "BOOLEAN",
             "CURRENT_DATE", "CURRENT_TIMESTAMP", "CURRENT_USER"}

MANUAL_NOTES = {
    "DLT Pipeline": "No DLT in Fabric: redesign as notebooks + Materialized Lake Views, orchestrated by a Data Pipeline",
    "Dashboard": "Rebuild as a Power BI report on a Fabric semantic model (Power BI Modeling MCP can help)",
    "Alert": "Recreate in Fabric Activator, or a pipeline with Teams/Outlook notification",
    "ML Model": "Re-register the model artifacts in the Fabric MLflow registry",
    "Serving Endpoint": "No native Fabric model serving: host on Azure ML / AI Foundry",
    "Repo": "Connect the Git repository to the Fabric workspace (Git integration)",
    "Credential": "Create a workspace identity / service principal and Fabric connections; secrets are not exportable",
    "Function": "Python/table-valued function, or signature missing (re-run the assessor v4): recreate as a PySpark UDF in a notebook or a Warehouse T-SQL function",
}


METRICS_FN = """def metrics(df):
    cols = sorted(df.columns)
    row = df.agg(F.count(F.lit(1)).alias("n"),
                 F.sum(F.xxhash64(*[F.col(c).cast("string") for c in cols]).cast("decimal(38,0)")).alias("h")).first()
    return {"rows": int(row["n"]), "checksum": None if row["h"] is None else str(row["h"]),
            "columns": [[f.name, f.dataType.simpleString()] for f in df.schema.fields]}"""

FABRIC_VALIDATION_LOOP = '''src = json.loads(notebookutils.fs.head(METRICS_PATH, 50000000))
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
print(f"{len(results) - len(bad)} passed, {len(bad)} need attention")'''


def build_validation(adir, cfg, manifest, art, catalogs):
    if not manifest:
        return
    vdir = adir / "validation"
    vdir.mkdir(parents=True, exist_ok=True)
    staging = cfg["staging_storage"].rstrip("/") + "/validation/source_metrics.json" if cfg["staging_storage"] else ""
    src_manifest = [{"id": m["id"], "source": m["source"], "kind": m["kind"]} for m in manifest]
    dbx = ("# Databricks notebook source\n# MAGIC %md\n# MAGIC # Source metrics for migration validation (run in DATABRICKS)\n"
           "# MAGIC Row count, per-table checksum and schema. Download `source_metrics.json` and upload it to the Fabric lakehouse under Files/validation/.\n\n# COMMAND ----------\n\n"
           "import json\nfrom pyspark.sql import functions as F\nMANIFEST = " + json.dumps(src_manifest, indent=2) + "\nOUT = " + json.dumps(staging) + "\n\n# COMMAND ----------\n\n"
           + METRICS_FN + "\n\nout = {}\nfor t in MANIFEST:\n    try:\n        out[t['source']] = metrics(spark.table(t['source']))\n    except Exception as e:\n        out[t['source']] = {'error': str(e)}\n"
           "text = json.dumps(out, indent=2)\nprint(text)\nif OUT:\n    dbutils.fs.put(OUT, text, True)\n    print('written to', OUT)\n")
    (vdir / "databricks_source_metrics.py").write_text(dbx, "utf-8")
    dflt = cfg["lakehouse_default"]
    tgt = [{"id": m["id"], "source": m["source"], "target": (m["target"] if m["lakehouse"] == dflt else f"{m['lakehouse']}.{m['target']}"), "kind": m["kind"]} for m in manifest]
    cells = [dict(kind="md", src="# Migration validation (run in FABRIC)\nCompares source metrics from Databricks with the migrated tables: row count, checksum, column names. Writes `Files/validation/results.json`; import it with `dbx2fabric_migrate.py validate-import`."),
             dict(kind="code", tags=["parameters"], src='METRICS_PATH = "Files/validation/source_metrics.json"\nRESULTS_PATH = "Files/validation/results.json"'),
             code_cell("import json\nfrom pyspark.sql import functions as F\nMANIFEST = " + json.dumps(tgt, indent=2) + "\n" + METRICS_FN),
             code_cell(FABRIC_VALIDATION_LOOP)]
    p = adir / "notebooks" / "validate_migration.ipynb"
    save_json(p, build_ipynb(cells))
    art.append({"kind": "notebook", "name": "validate_migration", "path": str(p), "lakehouse": dflt, "items": []})


def import_validation(out, results_path):
    out = Path(out)
    led = load_json(out / "ledger.json")
    res = load_json(results_path)
    byid = {i["id"]: i for i in led["items"]}
    n = {"PASS": 0, "FAIL": 0, "other": 0}
    for r in res:
        it = byid.get(r["id"])
        if not it:
            continue
        it["validation"] = {k: r.get(k) for k in ("status", "note", "rows_source", "rows_target")}
        if r["status"] == "PASS":
            it["outcome"] = MIG
            it["actions"].append(f"Validated: row count ({r.get('rows_target')}), checksum and columns match the source")
            n["PASS"] += 1
        elif r["status"] == "FAIL":
            it["outcome"] = PART
            it["reasons"].append("Validation failed: " + str(r.get("note")))
            n["FAIL"] += 1
        else:
            it["reasons"].append(f"Validation {r['status']}: {r.get('note')}")
            n["other"] += 1
    led["validated"] = datetime.now().isoformat(timespec="seconds")
    save_json(out / "ledger.json", led)
    return n


def write_runbook(out, cfg, ledger):
    arts = ledger.get("artifacts", [])
    kinds = lambda k: [e["name"] for e in arts if e["kind"] == k]
    export = any(e["name"].startswith("databricks/export") for e in arts)
    blocked = sum(1 for it in ledger.get("items", []) if any("staging_storage" in r for r in it.get("reasons", [])))
    validation = any(e["name"] == "validate_migration" for e in arts)
    copies = [n for n in kinds("notebook") if n.startswith("copy_tables_")]
    steps = ["# Runbook: moving this migration into Fabric", "",
             "## 0. Prerequisites",
             "- A Fabric workspace on a paid capacity or trial; you are Contributor or above; note the workspace id (the GUID in the workspace URL).",
             "- Tenant setting 'Users can create Fabric items' is on. Sign in with `az login` (or set FABRIC_TOKEN).",
             "- Create an Azure Key Vault and give the Fabric users/notebooks read access to secrets.", "",
             "## 1. Fill in mapping.json",
             "- `workspace_id`, `keyvault_urls`, `mount_map`, `warehouse_schemas`, lakehouse names.",
             "- `adls_connection_id`: create an ADLS Gen2 connection in Fabric (Settings > Manage connections) and paste its id. Needed for shortcuts.",
             "- `staging_storage`: an ADLS Gen2 container you own. Needed for managed tables and volumes." + (
                 "" if export else f" **Required for this run: {blocked} managed tables/volumes are blocked until it is set.**" if blocked
                 else " (not needed for this run)"), ""]
    n = 2
    if export:
        steps += [f"## {n}. In DATABRICKS: stage managed data",
                  "- Run `artifacts/databricks/export_to_staging.py` as a notebook. Give the Fabric identity read access to the staging container.", ""]
        n += 1
    steps += [f"## {n}. Re-run conversion",
              "`python3 dbx2fabric_migrate.py convert --assessment assessment_out/assessment.json --out migration_out --source-dir ./exported_notebooks`", "",
              f"## {n + 1}. Smoke test the deployment (notebooks only)",
              "`python3 dbx2fabric_migrate.py preflight --out migration_out` first: fix every ERROR; read the warnings.",
              "`python3 dbx2fabric_migrate.py deploy --out migration_out --workspace-id <id> --only notebook`",
              "- Open one notebook in Fabric: check the default lakehouse is attached, the parameter cell is recognised and `lakehouse.schema.table` names resolve. If anything is off, fix mapping.json and re-run before deploying everything.", "",
              f"## {n + 2}. Full deployment",
              "`python3 dbx2fabric_migrate.py deploy --out migration_out --workspace-id <id>`",
              "Runs `preflight` first and stops on errors. Creates lakehouses, Warehouse/SQL database items, notebooks, shortcuts, pipelines and schedules. Safe to re-run: existing items are updated.", "",
              f"## {n + 3}. Manual steps after deployment",
              "- Create the secrets listed in `artifacts/security/keyvault_secrets.csv` in Key Vault." if kinds("csv") else "- No secrets to recreate.",
              f"- Run the copy notebooks once, in order: {', '.join(copies) or 'none'}. Each checks its own row counts.",
              "- In the Warehouse SQL editor run `artifacts/warehouse/load_tables.sql` (after the lakehouse tables exist and the notebooks that write gold tables have run)." if "warehouse/load_tables.sql" in kinds("script") else "- No Warehouse scripts.",
              "- Create a Fabric Environment from `artifacts/environment/environment.yml` and attach it to notebooks that need the libraries." if kinds("environment") else "- No environment changes.",
              "- Give users access (see `artifacts/security/permissions_matrix.csv`) using workspace roles and OneLake security.",
              "- Enable pipeline schedules only after the first successful manual run.", "",
              f"## {n + 4}. Validate",
              *([] if validation or not blocked else ["- The validation notebooks are generated only once tables can be copied (set `staging_storage` and re-run convert). The steps below apply after that."]),
              "1. In DATABRICKS run `artifacts/validation/databricks_source_metrics.py`; download `source_metrics.json`.",
              "2. Upload it to the lakehouse under Files/validation/.",
              "3. In FABRIC run the `validate_migration` notebook. It writes Files/validation/results.json; download it.",
              "4. `python3 dbx2fabric_migrate.py validate-import --out migration_out --results results.json` - passed tables move to Migrated, failures to Partial.", "",
              f"## {n + 5}. Go-live",
              "- Every 'Needs testing' item has been run once; 'Partially migrated' and 'Not migrated' items are done or consciously deferred.",
              "- Run both platforms in parallel for one full cycle and compare outputs before switching consumers.", ""]
    (Path(out) / "RUNBOOK.md").write_text("\n".join(steps), "utf-8")


def write_ai_task(it, a, tasks_dir, cfg):
    tasks_dir.mkdir(parents=True, exist_ok=True)
    body = f"""# AI work order: {it['id']}
Outcome so far: **{LABEL.get(it['outcome'])}**   Target: **{it['target_type']} / {it['target_name']}**

## Why this needs you
{chr(10).join('- ' + r for r in it['reasons']) or '- manual migration'}

## Source details
```json
{json.dumps(a.get('meta', {}), indent=2, default=str)[:6000]}
```
Converted artifacts: {', '.join(it['artifacts']) or 'none'}

## Fabric target rules (follow these)
- OLTP database -> Fabric SQL database; OLAP database -> Warehouse; all other analytics / files -> Lakehouse
- Code -> Fabric Notebooks (use notebookutils, never dbutils)
- Ingestion -> Data Pipeline Copy Data or Copy job; orchestration -> Data Pipeline
- dbt -> Fabric dbt job (adapters: dbt-fabric for Warehouse, dbt-fabricspark for Lakehouse); Arrow/pyarrow is available in Fabric Spark runtimes

## Suggested tools
- Fabric MCP (API specs, item definitions) to build the item definition; Power BI Modeling MCP for semantic models / dashboards
- This tool's MCP: `get_ai_task`, then `record_result` when you finish

## Done when
- The item exists in Fabric (or the artifact is complete), is tested against the Databricks source, and `record_result` has been called with outcome `needs_testing` or `migrated` and a note.
"""
    (tasks_dir / f"{safe(it['id'])}.md").write_text(body, "utf-8")


# ---------------------------------------------------------------------------
# DEPLOY (online)
# ---------------------------------------------------------------------------
class FabricError(Exception):
    pass


def get_token():
    t = os.getenv("FABRIC_TOKEN")
    if t:
        return t
    try:
        return subprocess.check_output(["az", "account", "get-access-token", "--resource", "https://api.fabric.microsoft.com",
                                        "--query", "accessToken", "-o", "tsv"], text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        raise FabricError("No token: set FABRIC_TOKEN or run `az login`")


class Fabric:
    def __init__(self, token, base=None):
        self.base = (base or os.getenv("FABRIC_API_BASE") or "https://api.fabric.microsoft.com/v1").rstrip("/")
        self.h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        self._items = {}

    def req(self, method, path, body=None, full=False):
        url = path if full else self.base + path
        for i in range(4):
            r = urllib.request.Request(url, method=method, headers=self.h, data=json.dumps(body).encode() if body is not None else None)
            try:
                with urllib.request.urlopen(r, timeout=120) as resp:
                    raw = resp.read()
                    return resp.status, dict(resp.headers), (json.loads(raw) if raw else None) or {}
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    time.sleep(int(e.headers.get("Retry-After", 5)))
                    continue
                raise FabricError(f"{method} {path} -> {e.code}: {e.read().decode('utf-8', 'ignore')[:300]}")
        raise FabricError(f"{method} {path}: throttled")

    def wait(self, headers):
        loc = headers.get("Location") or headers.get("location")
        for _ in range(150):
            time.sleep(float(headers.get("Retry-After", 2)) if _ == 0 else 2)
            _, h, js = self.req("GET", loc, full=True)
            if js.get("status") in ("Succeeded", "Completed"):
                return js
            if js.get("status") == "Failed":
                raise FabricError(f"operation failed: {js}")
        raise FabricError("operation timed out")

    def find(self, ws, typ, name):
        if ws not in self._items:
            items, tok = [], None
            while True:
                _, _, js = self.req("GET", f"/workspaces/{ws}/items" + (f"?continuationToken={urllib.parse.quote(tok)}" if tok else ""))
                items += js.get("value", [])
                tok = js.get("continuationToken")
                if not tok:
                    break
            self._items[ws] = items
        return next((i["id"] for i in self._items[ws] if i["type"] == typ and i["displayName"] == name), None)

    def ensure(self, ws, endpoint, typ, name, extra=None, definition=None):
        iid = self.find(ws, typ, name)
        if iid:
            if definition:
                st, h, _ = self.req("POST", f"/workspaces/{ws}/items/{iid}/updateDefinition", {"definition": definition})
                if st == 202:
                    self.wait(h)
            return iid, "updated" if definition else "existing"
        body = {"displayName": name, **(extra or {})}
        if definition:
            body["definition"] = definition
        st, h, js = self.req("POST", f"/workspaces/{ws}/{endpoint}", body)
        if st == 202:
            self.wait(h)
        self._items.pop(ws, None)
        iid = js.get("id") or self.find(ws, typ, name)
        if not iid:
            raise FabricError(f"{typ} '{name}' created but id not found")
        return iid, "created"


def b64(obj):
    return base64.b64encode(json.dumps(obj).encode() if not isinstance(obj, str) else obj.encode()).decode()


def preflight(out, cfg):
    """Static checks on the generated artifacts before anything is sent to Fabric -> [(level, where, message)]"""
    out = Path(out)
    led = load_json(out / "ledger.json")
    issues = []
    add = lambda lvl, where, msg: issues.append((lvl, where, msg))
    for k in ("lakehouse_default", "warehouse_name"):
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", cfg[k]):
            add("error", "mapping.json", f"{k} '{cfg[k]}' must start with a letter and use letters, digits, underscores")
    for c, lh in cfg["catalog_to_lakehouse"].items():
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", lh):
            add("error", "mapping.json", f"lakehouse name '{lh}' (catalog {c}) must use letters, digits, underscores")
    names, nbs = {}, set()
    for e in led.get("artifacts", []):
        if e["kind"] in ("notebook", "pipeline"):
            names.setdefault((e["kind"], e["name"]), 0)
            names[(e["kind"], e["name"])] += 1
        if e["kind"] == "notebook":
            nbs.add(e["name"])
    for (k, n), c in names.items():
        if c > 1:
            add("error", n, f"duplicate {k} name (two source assets map to the same Fabric name)")
    for e in led.get("artifacts", []):
        p = resolve(out, e["path"])
        if e["kind"] in ("notebook", "pipeline", "shortcuts") and not p.exists():
            add("error", e["name"], f"artifact file missing: {p}")
            continue
        if e["kind"] == "notebook":
            try:
                nb = json.loads(p.read_text("utf-8"))
                code = "\n".join("".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code")
            except Exception as ex:
                add("error", e["name"], f"not a valid notebook: {ex}")
                continue
            if re.search(r"^\s*(#|--|//)\s?MAGIC\b", code, re.M):
                add("error", e["name"], "unconverted MAGIC lines left in a code cell")
            if re.search(r"Databricks notebook source", code):
                add("warn", e["name"], "Databricks header comment left in a cell")
            n = code.count("TODO[dbx2fabric:")
            if n:
                add("warn", e["name"], f"{n} unresolved TODO marker(s): deploys, but those cells need rework")
            if re.search(r"\bdbutils\.", code):
                add("warn", e["name"], "dbutils calls remain (see TODO markers): they fail in Fabric")
            if "<your-vault>" in code:
                add("warn", e["name"], "Key Vault URL placeholder: set keyvault_url_default / keyvault_urls in mapping.json and re-run convert")
        elif e["kind"] == "pipeline":
            try:
                acts = json.loads(p.read_text("utf-8"))["properties"]["activities"]
            except Exception as ex:
                add("error", e["name"], f"not a valid pipeline definition: {ex}")
                continue
            an = {a["name"] for a in acts}
            if len(an) != len(acts):
                add("error", e["name"], "duplicate activity names")
            for a in acts:
                for d in a.get("dependsOn", []):
                    if d["activity"] not in an:
                        add("error", e["name"], f"activity {a['name']} depends on unknown activity {d['activity']}")
                nb = re.findall(r"@@NB:([^@]+)@@", json.dumps(a))
                for x in nb:
                    if x not in nbs:
                        add("warn", e["name"], f"activity {a['name']} calls notebook '{x}' which is not part of this migration: it must already exist in the workspace")
        elif e["kind"] == "shortcuts" and not cfg["adls_connection_id"]:
            add("warn", e["name"], "adls_connection_id is empty: shortcuts will not be created")
    blocked = sum(1 for it in led["items"] if any("staging_storage" in r for r in it.get("reasons", [])))
    if blocked:
        add("warn", "mapping.json", f"{blocked} item(s) are blocked until staging_storage is set")
    return issues


def deploy(out, cfg, args):
    out = Path(out)
    ledger = load_json(out / "ledger.json")
    errs = [i for i in preflight(out, cfg) if i[0] == "error"]
    if errs and not getattr(args, "force", False):
        raise FabricError("preflight found errors (run `preflight` to see all; --force to deploy anyway): " + "; ".join(f"{w}: {m}" for _, w, m in errs[:3]))
    ws = args.workspace_id or cfg["workspace_id"]
    if not ws:
        raise FabricError("workspace id required (--workspace-id or mapping.json)")
    fab = Fabric(get_token())
    only = {x for x in (getattr(args, "only", None) or "").split(",") if x}
    want = lambda k: not only or k in only
    ids, items = {}, {i["id"]: i for i in ledger["items"]}
    arts = ledger.get("artifacts", [])

    def mark(e, ok, msg=""):
        for iid in e.get("items", []):
            it = items.get(iid)
            if it:
                it["deployment"] = "deployed" if ok else "failed"
                if not ok:
                    it["outcome"] = NOT
                    it["reasons"].append("Deployment failed: " + msg)
                elif msg:
                    it["actions"].append(msg)

    lhs = {e["lakehouse"] for e in arts if e.get("lakehouse")} | set(cfg["catalog_to_lakehouse"].values()) | {cfg["lakehouse_default"]}
    for lh in sorted(lhs):
        ids["LH:" + lh], how = fab.ensure(ws, "lakehouses", "Lakehouse", lh, {"creationPayload": {"enableSchemas": True}})
        log(f"lakehouse {lh}: {how}")
    if any(i["target_type"] == "Warehouse" for i in items.values()):
        ids["WH"], how = fab.ensure(ws, "warehouses", "Warehouse", cfg["warehouse_name"])
        log(f"warehouse {cfg['warehouse_name']}: {how}")
        for it in items.values():
            if it["type"] == "SQL Warehouse":
                it["deployment"], it["target_id"] = "deployed", ids["WH"]
    if any(i["target_type"] == "SQL database" for i in items.values()):
        ids["SQLDB"], how = fab.ensure(ws, "sqlDatabases", "SQLDatabase", cfg["sqldb_name"])
        log(f"sql database {cfg['sqldb_name']}: {how}")
    for e in [x for x in arts if x["kind"] == "notebook" and want("notebook")]:
        try:
            nb = load_json(resolve(out, e["path"]))
            lh = e.get("lakehouse") or cfg["lakehouse_default"]
            nb["metadata"]["dependencies"] = {"lakehouse": {"default_lakehouse": ids["LH:" + lh], "default_lakehouse_name": lh, "default_lakehouse_workspace_id": ws}}
            definition = {"format": "ipynb", "parts": [{"path": "notebook-content.ipynb", "payload": b64(nb), "payloadType": "InlineBase64"}]}
            nid, how = fab.ensure(ws, "notebooks", "Notebook", e["name"], definition=definition)
            ids["NB:" + e["name"]] = nid
            for iid in e["items"]:
                if iid in items:
                    items[iid]["target_id"] = nid
            mark(e, True)
            log(f"notebook {e['name']}: {how}")
        except FabricError as ex:
            mark(e, False, str(ex))
            log(f"notebook {e['name']}: FAILED {ex}")
    for e in [x for x in arts if x["kind"] == "shortcuts" and want("shortcuts")]:
        for sc in load_json(resolve(out, e["path"])):
            it = items.get(sc["item"])
            sc["target"]["adlsGen2"]["connectionId"] = sc["target"]["adlsGen2"]["connectionId"] or cfg["adls_connection_id"]
            try:
                if not sc["target"]["adlsGen2"]["connectionId"]:
                    raise FabricError("adls_connection_id not set in mapping.json")
                body = {k: v for k, v in sc.items() if k in ("path", "name", "target")}
                try:
                    fab.req("POST", f"/workspaces/{ws}/items/{ids['LH:' + sc['lakehouse']]}/shortcuts", body)
                except FabricError as ex:
                    if " 409" not in str(ex):
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
                    it["reasons"].append("Shortcut failed: " + str(ex))
    for e in [x for x in arts if x["kind"] == "pipeline" and want("pipeline")]:
        try:
            text = resolve(out, e["path"]).read_text("utf-8").replace("@@WS@@", ws)
            for nm in set(re.findall(r"@@NB:([^@]+)@@", text)):
                if "NB:" + nm not in ids and fab.find(ws, "Notebook", nm):
                    ids["NB:" + nm] = fab.find(ws, "Notebook", nm)
            miss = [m for m in re.findall(r"@@NB:([^@]+)@@", text) if "NB:" + m not in ids]
            if miss:
                raise FabricError(f"notebook(s) not deployed: {sorted(set(miss))}")
            text = re.sub(r"@@NB:([^@]+)@@", lambda m: ids["NB:" + m.group(1)], text)
            definition = {"parts": [{"path": "pipeline-content.json", "payload": b64(text), "payloadType": "InlineBase64"}]}
            pid, how = fab.ensure(ws, "dataPipelines", "DataPipeline", e["name"], definition=definition)
            for iid in e["items"]:
                if iid in items:
                    items[iid]["target_id"] = pid
            msg = ""
            if e.get("schedule"):
                try:
                    fab.req("POST", f"/workspaces/{ws}/items/{pid}/jobs/Pipeline/schedules", load_json(resolve(out, e["schedule"])))
                    msg = "schedule created"
                except FabricError as ex:
                    for iid in e["items"]:
                        items[iid]["reasons"].append("Schedule not created: " + str(ex))
            mark(e, True, msg)
            log(f"pipeline {e['name']}: {how}")
        except FabricError as ex:
            mark(e, False, str(ex))
            log(f"pipeline {e['name']}: FAILED {ex}")
    ledger["deployed"] = datetime.now().isoformat(timespec="seconds")
    save_json(out / "ledger.json", ledger)
    return ledger


# ---------------------------------------------------------------------------
# REPORT
# ---------------------------------------------------------------------------
def summarize_ledger(ledger):
    its = ledger["items"]
    cnt = {k: sum(1 for i in its if i["outcome"] == k) for k in (MIG, TEST, PART, NOT)}
    by_type = {}
    for i in its:
        by_type.setdefault(i["type"], {MIG: 0, TEST: 0, PART: 0, NOT: 0})[i["outcome"]] = by_type.setdefault(i["type"], {MIG: 0, TEST: 0, PART: 0, NOT: 0}).get(i["outcome"], 0) + 1
    return {"total": len(its), **cnt, "deployed": sum(1 for i in its if i["deployment"] == "deployed"),
            "generated_only": sum(1 for i in its if i["deployment"] == "generated"), "failed": sum(1 for i in its if i["deployment"] == "failed"), "by_type": by_type}


def write_report(out):
    out = Path(out)
    ledger = load_json(out / "ledger.json")
    S = summarize_ledger(ledger)
    e = html.escape
    color = {MIG: "#1a7f37", TEST: "#b7791f", PART: "#d9730d", NOT: "#c0392b"}

    def table(outcome, cols):
        rows = [i for i in ledger["items"] if i["outcome"] == outcome]
        if not rows:
            return "<p><i>none</i></p>"
        body = "".join("<tr>" + "".join(f"<td>{c(i)}</td>" for c in cols) + "</tr>" for i in rows)
        return "<table><tr>" + "".join(f"<th>{h}" for h in HDR[outcome]) + f"</tr>{body}</table>"
    ul = lambda xs: "<ul>" + "".join(f"<li>{e(x)}</li>" for x in xs) + "</ul>" if xs else ""
    base = [lambda i: e(i["type"]), lambda i: e(i["name"]), lambda i: e(f"{i['target_type']} / {i['target_name']}" if i["target_name"] else i["target_type"]), lambda i: e(i["deployment"])]
    HDR = {MIG: ["Type", "Name", "Target", "Deployment", "What was done"], TEST: ["Type", "Name", "Target", "Deployment", "What was done", "Test before go-live"],
           PART: ["Type", "Name", "Target", "Deployment", "Still needs manual rework", "Test / next step"], NOT: ["Type", "Name", "Target", "Deployment", "Why not migrated", "Next action"]}
    sections = {
        MIG: table(MIG, base + [lambda i: ul(i["actions"])]),
        TEST: table(TEST, base + [lambda i: ul(i["actions"] + i["reasons"]), lambda i: ul(i["tests"])]),
        PART: table(PART, base + [lambda i: ul(i["reasons"]), lambda i: ul(i["tests"] + ["AI work order: ai_tasks/" + safe(i["id"]) + ".md"])]),
        NOT: table(NOT, base + [lambda i: ul(i["reasons"]), lambda i: ul(["AI work order: ai_tasks/" + safe(i["id"]) + ".md"])])}
    cards = "".join(f"<span class=c style='color:{color[k]}'><b>{S[k]}</b>{LABEL[k]}</span>" for k in (MIG, TEST, PART, NOT))
    trows = "".join(f"<tr><td>{e(t)}<td>{sum(v.values())}" + "".join(f"<td>{v[k]}" for k in (MIG, TEST, PART, NOT)) + "</tr>" for t, v in sorted(S["by_type"].items()))
    doc = f"""<!doctype html><meta charset=utf-8><title>Databricks to Fabric - Post-Migration Report</title>
<style>body{{font:14px system-ui;margin:32px;color:#222}}table{{border-collapse:collapse;margin:12px 0;width:100%}}td,th{{border:1px solid #ddd;padding:6px 9px;text-align:left;vertical-align:top}}
th{{background:#f3f4f6}}.c{{display:inline-block;margin:0 14px 14px 0;padding:14px 20px;border-radius:10px;background:#f3f4f6}}.c b{{font-size:28px;display:block}}ul{{margin:0;padding-left:18px}}</style>
<h1>Databricks &rarr; Fabric Post-Migration Report</h1><p>Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} by dbx2fabric_migrate {VERSION}</p>
<div>{cards}<span class=c><b>{S['deployed']}</b>Deployed to Fabric</span><span class=c><b>{S['generated_only']}</b>Generated, not deployed</span><span class=c><b>{S['failed']}</b>Deployment failed</span></div>
<p><b>Deployment column:</b> "deployed" means the generated artifact now exists in Fabric (a notebook, pipeline or shortcut). Data copies, view creation and Warehouse scripts still have to be run; see RUNBOOK.md. Nothing counts as verified until it is executed in Fabric. "Migrated" = no behaviour to test (infrastructure or code with nothing to convert). Run the tests listed for every other item.</p>
<h2>By asset type</h2><table><tr><th>Type<th>Total<th>{LABEL[MIG]}<th>{LABEL[TEST]}<th>{LABEL[PART]}<th>{LABEL[NOT]}</tr>{trows}</table>
<h2>Not migrated</h2>{sections[NOT]}<h2>Partially migrated - manual rework</h2>{sections[PART]}
<h2>Migrated - needs testing</h2>{sections[TEST]}<h2>Migrated</h2>{sections[MIG]}"""
    (out / "post_migration_report.html").write_text(doc, "utf-8")
    with open(out / "post_migration_report.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["type", "name", "outcome", "deployment", "target_type", "target_name", "target_id", "actions", "reasons", "tests"])
        for i in ledger["items"]:
            w.writerow([i["type"], i["name"], LABEL[i["outcome"]] if i["outcome"] in LABEL else i["outcome"], i["deployment"], i["target_type"], i["target_name"],
                        i.get("target_id") or "", " | ".join(i["actions"]), " | ".join(i["reasons"]), " | ".join(i["tests"])])
    return S


# ---------------------------------------------------------------------------
# WORKFLOW
# ---------------------------------------------------------------------------
def do_plan(assessment, mapping, out):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    assess = load_json(assessment)
    cfg = load_cfg(mapping)
    ledger = {"version": VERSION, "created": datetime.now().isoformat(timespec="seconds"), "source_assessment": str(assessment), "items": plan(assess, cfg)}
    save_json(out / "ledger.json", ledger)
    return ledger


def do_run(assessment, mapping, out, args):
    out = Path(out)
    if mapping and not Path(mapping).exists():
        init_mapping(load_json(assessment), mapping)
        log("created", mapping, "- review it (lakehouse names, warehouse schemas, mounts, Key Vault URLs) and re-run for best results")
    do_plan(assessment, mapping, out)
    convert_all(out, load_json(assessment), load_cfg(mapping), args)
    if getattr(args, "deploy", False):
        deploy(out, load_cfg(mapping), args)
    return write_report(out)


# ---------------------------------------------------------------------------
# MCP SERVER (stdio, JSON-RPC 2.0)
# ---------------------------------------------------------------------------
class A:  # args shim for tool calls
    def __init__(self, **k):
        self.host = self.token = self.source_dir = self.workspace_id = None
        self.deploy = self.ai = False
        self.only = None
        self.__dict__.update(k)


def _paths(a):
    out = a.get("out", "migration_out")
    return a.get("assessment", "assessment_out/assessment.json"), a.get("mapping", str(Path(out) / "mapping.json")), out


def t_summary(a):
    asm = load_json(a.get("assessment", "assessment_out/assessment.json"))
    return asm["summary"]


def t_init(a):
    asm, mp, out = _paths(a)
    return init_mapping(load_json(asm), mp)


def t_plan(a):
    asm, mp, out = _paths(a)
    return summarize_ledger(do_plan(asm, mp, out))


def t_convert_nb(a):
    cfg = load_cfg(a.get("mapping"))
    text = a.get("source") or Path(a["path"]).read_text("utf-8")
    ctx = Ctx(cfg, a.get("catalogs", []))
    cells = convert_cells(parse_source(text, a.get("language", "PYTHON")), ctx)
    return {"outcome": outcome_from_ctx(ctx), "changes": ctx.changes, "todos": ctx.todos, "warnings": {k: v[1] for k, v in ctx.warns.items()},
            "notebook": build_ipynb(cells)}


def t_convert_job(a):
    pipe, sched, notes, omitted = convert_job({"name": a.get("name", "job"), "meta": {"settings": a["settings"]}}, load_cfg(a.get("mapping")))
    return {"pipeline": pipe, "schedule": sched, "notes": notes, "omitted_tasks": omitted}


def t_run(a):
    asm, mp, out = _paths(a)
    return do_run(asm, mp, out, A(**{k: v for k, v in a.items() if k in ("host", "token", "source_dir", "workspace_id", "deploy", "ai")}))


def t_status(a):
    led = load_json(Path(a.get("out", "migration_out")) / "ledger.json")
    s = summarize_ledger(led)
    s["not_migrated_items"] = [{"id": i["id"], "why": i["reasons"][:2]} for i in led["items"] if i["outcome"] == NOT][:50]
    return s


def t_import_validation(a):
    out = a.get("out", "migration_out")
    n = import_validation(out, a["results"])
    write_report(out)
    return n


def t_preflight(a):
    out = a.get("out", "migration_out")
    return [{"level": l, "where": w, "message": m} for l, w, m in preflight(out, load_cfg(a.get("mapping") or str(Path(out) / "mapping.json")))]


def t_report(a):
    return write_report(a.get("out", "migration_out"))


def t_ai_task(a):
    p = Path(a.get("out", "migration_out")) / "ai_tasks" / f"{safe(a['id'])}.md"
    return p.read_text("utf-8") if p.exists() else f"No work order for {a['id']}"


def t_record(a):
    p = Path(a.get("out", "migration_out")) / "ledger.json"
    led = load_json(p)
    if a["outcome"] not in (MIG, TEST, PART, NOT):
        raise ValueError(f"outcome must be one of {MIG}, {TEST}, {PART}, {NOT}")
    for i in led["items"]:
        if i["id"] == a["id"]:
            i["outcome"] = a["outcome"]
            i["actions"].append(f"[manual/AI] {a.get('note', '')}")
            if a.get("target_id"):
                i["target_id"] = a["target_id"]
            if a["outcome"] != NOT:
                i["deployment"] = "deployed"
            save_json(p, led)
            write_report(a.get("out", "migration_out"))
            return {"updated": a["id"], "outcome": a["outcome"]}
    raise ValueError("item not found: " + a["id"])


S_STR = lambda d: {"type": "string", "description": d}
TOOLS = {
    "summarize_assessment": ("Summarize the pre-migration assessment (readiness score, status counts, effort).", {"assessment": S_STR("path to assessment.json")}, t_summary),
    "init_mapping": ("Create mapping.json (lakehouses, warehouse schemas, mounts, Key Vault) from the assessment.", {"assessment": S_STR("assessment.json"), "out": S_STR("output dir")}, t_init),
    "plan_migration": ("Create the migration ledger: every asset gets a Fabric target (SQL database / Warehouse / Lakehouse / Notebook / Pipeline).", {"assessment": S_STR("assessment.json"), "mapping": S_STR("mapping.json"), "out": S_STR("output dir")}, t_plan),
    "convert_notebook": ("Convert one Databricks notebook (source text or file path) to a Fabric .ipynb; returns changes, TODO blockers, warnings.", {"source": S_STR("notebook source text"), "path": S_STR("file path (alternative)"), "language": S_STR("PYTHON|SQL|SCALA|R"), "mapping": S_STR("mapping.json")}, t_convert_nb),
    "convert_job": ("Convert a Databricks job settings JSON into a Fabric Data Pipeline definition + schedule.", {"settings": {"type": "object", "description": "job settings"}, "name": S_STR("job name"), "mapping": S_STR("mapping.json")}, t_convert_job),
    "run_migration": ("Plan + convert + report (add deploy=true and workspace_id to push to Fabric).", {"assessment": S_STR("assessment.json"), "mapping": S_STR("mapping.json"), "out": S_STR("output dir"), "source_dir": S_STR("exported notebooks"), "deploy": {"type": "boolean"}, "workspace_id": S_STR("Fabric workspace id"), "ai": {"type": "boolean"}}, t_run),
    "migration_status": ("Counts per outcome plus the list of not-migrated items.", {"out": S_STR("output dir")}, t_status),
    "import_validation": ("Import Fabric validation results.json: passed tables become Migrated, failures Partial.", {"results": S_STR("path to results.json"), "out": S_STR("output dir")}, t_import_validation),
    "preflight": ("Static checks on generated artifacts before deploying to Fabric (errors block deploy).", {"out": S_STR("output dir"), "mapping": S_STR("mapping.json")}, t_preflight),
    "generate_report": ("Regenerate the post-migration report (HTML + CSV).", {"out": S_STR("output dir")}, t_report),
    "get_ai_task": ("Get the work order for an item that needs manual/AI completion.", {"id": S_STR("ledger item id, e.g. 'Dashboard:Sales'"), "out": S_STR("output dir")}, t_ai_task),
    "record_result": ("Record the result of manual/AI work on an item so the report stays accurate.", {"id": S_STR("item id"), "outcome": S_STR("migrated|needs_testing|partial|not_migrated"), "note": S_STR("what was done"), "target_id": S_STR("Fabric item id"), "out": S_STR("output dir")}, t_record),
}


def mcp_serve():
    def send(o):
        sys.stdout.write(json.dumps(o) + "\n")
        sys.stdout.flush()
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except Exception:
            continue
        mid, method, params = msg.get("id"), msg.get("method"), msg.get("params") or {}
        if method == "initialize":
            send({"jsonrpc": "2.0", "id": mid, "result": {"protocolVersion": params.get("protocolVersion", "2024-11-05"), "capabilities": {"tools": {}},
                                                           "serverInfo": {"name": "dbx2fabric", "version": VERSION}}})
        elif method == "tools/list":
            send({"jsonrpc": "2.0", "id": mid, "result": {"tools": [{"name": n, "description": d, "inputSchema": {"type": "object", "properties": p}} for n, (d, p, _) in TOOLS.items()]}})
        elif method == "tools/call":
            name = params.get("name")
            try:
                res = TOOLS[name][2](params.get("arguments") or {})
                text = res if isinstance(res, str) else json.dumps(res, indent=2, default=str)
                send({"jsonrpc": "2.0", "id": mid, "result": {"content": [{"type": "text", "text": text}]}})
            except Exception as e:
                send({"jsonrpc": "2.0", "id": mid, "result": {"content": [{"type": "text", "text": f"Error: {e}"}], "isError": True}})
        elif method == "ping":
            send({"jsonrpc": "2.0", "id": mid, "result": {}})
        elif mid is not None:
            send({"jsonrpc": "2.0", "id": mid, "error": {"code": -32601, "message": f"unknown method {method}"}})


def mcp_config():
    me = str(Path(__file__).resolve())
    servers = {"dbx2fabric": {"command": "python3", "args": [me, "mcp"]},
               "fabric": {"command": "npx", "args": ["-y", "@microsoft/fabric-mcp@latest", "server", "start", "--mode", "all"]},
               "powerbi": {"command": "npx", "args": ["-y", "@microsoft/powerbi-modeling-mcp@latest", "--start"]}}
    print("# Claude Desktop / Cursor style  (claude_desktop_config.json)")
    print(json.dumps({"mcpServers": servers}, indent=2))
    print("\n# Claude Code\n" + f"claude mcp add dbx2fabric -- python3 {me} mcp")
    print("claude mcp add fabric -- npx -y @microsoft/fabric-mcp@latest server start --mode all")
    print("claude mcp add powerbi -- npx -y @microsoft/powerbi-modeling-mcp@latest --start")
    print("\n# GitHub Copilot in VS Code  (.vscode/mcp.json)")
    print(json.dumps({"servers": {k: dict(v, type="stdio") for k, v in servers.items()}}, indent=2))
    print("\n# Codex CLI  (~/.codex/config.toml)")
    for k, v in servers.items():
        print(f"[mcp_servers.{k}]\ncommand = \"{v['command']}\"\nargs = {json.dumps(v['args'])}\n")


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Databricks -> Fabric migration tool")
    ap.add_argument("command", choices=["init", "plan", "convert", "preflight", "deploy", "report", "run", "validate-import", "mcp", "mcp-config"])
    ap.add_argument("--assessment", default="assessment_out/assessment.json")
    ap.add_argument("--out", default="migration_out")
    ap.add_argument("--mapping", help="mapping.json (default: <out>/mapping.json)")
    ap.add_argument("--source-dir", help="folder with exported Databricks notebooks (offline)")
    ap.add_argument("--host", default=os.getenv("DATABRICKS_HOST"))
    ap.add_argument("--token", default=os.getenv("DATABRICKS_TOKEN"))
    ap.add_argument("--workspace-id", help="Fabric workspace id (for --deploy)")
    ap.add_argument("--only", help="deploy only: notebook,shortcuts,pipeline (lakehouse/warehouse items are always ensured)")
    ap.add_argument("--results", help="validate-import: results.json produced by the validate_migration notebook")
    ap.add_argument("--force", action="store_true", help="deploy even if preflight reports errors")
    ap.add_argument("--deploy", action="store_true", help="push generated items to Fabric via REST")
    ap.add_argument("--ai", action="store_true", help="AI-complete cells that still have TODO markers (needs ANTHROPIC_API_KEY)")
    a = ap.parse_args()
    if a.command == "mcp":
        return mcp_serve()
    if a.command == "mcp-config":
        return mcp_config()
    a.mapping = a.mapping or str(Path(a.out) / "mapping.json")
    if a.command == "init":
        init_mapping(load_json(a.assessment), a.mapping)
        log("wrote", a.mapping)
    elif a.command == "plan":
        S = summarize_ledger(do_plan(a.assessment, a.mapping, a.out))
        log("planned", S["total"], "items")
    elif a.command == "convert":
        convert_all(a.out, load_json(a.assessment), load_cfg(a.mapping), a)
        S = write_report(a.out)
    elif a.command == "preflight":
        iss = preflight(a.out, load_cfg(a.mapping))
        for lvl, where, msg in iss:
            log(f"{lvl.upper():5} {where}: {msg}")
        log(f"{sum(1 for i in iss if i[0] == 'error')} error(s), {sum(1 for i in iss if i[0] == 'warn')} warning(s)")
        sys.exit(1 if any(i[0] == "error" for i in iss) else 0)
    elif a.command == "deploy":
        deploy(a.out, load_cfg(a.mapping), a)
        S = write_report(a.out)
    elif a.command == "report":
        S = write_report(a.out)
    elif a.command == "validate-import":
        log("validation imported:", import_validation(a.out, a.results))
        S = write_report(a.out)
    else:
        S = do_run(a.assessment, a.mapping, a.out, a)
    if a.command not in ("init", "plan"):
        log(f"\n{S['total']} items | Migrated {S[MIG]} | Needs testing {S[TEST]} | Partial {S[PART]} | Not migrated {S[NOT]} | "
            f"deployed {S['deployed']} / generated {S['generated_only']} / failed {S['failed']}\nReport: {Path(a.out) / 'post_migration_report.html'}")


if __name__ == "__main__":
    main()
