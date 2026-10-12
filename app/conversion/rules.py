"""Conversion rules: dbutils rewrites, TODO blockers and needs-testing warnings. Spec §3.2.

Order matters in TODO_RULES: the first matching rule per line wins (specific -> generic).
"""

from __future__ import annotations

import re

__all__ = [
    "COMMENT",
    "NB_RUN_RE",
    "SECRET_RE",
    "TEST_RULES",
    "TODO_RULES",
    "WIDGET_DEF_RE",
    "WIDGET_GET_RE",
    "WIDGET_RM_RE",
]

SECRET_RE = r"dbutils\.secrets\.get\(\s*(?:scope\s*=\s*)?([\"'])(.+?)\1\s*,\s*(?:key\s*=\s*)?([\"'])(.+?)\3\s*\)"  # nosec B105
WIDGET_DEF_RE = r"^([ \t]*)dbutils\.widgets\.(?:text|dropdown|combobox|multiselect)\(\s*(?:name\s*=\s*)?([\"'])(.+?)\2\s*,\s*(?:defaultValue\s*=\s*)?([\"'])(.*?)\4.*\)[ \t]*$"
WIDGET_GET_RE = (
    r"dbutils\.widgets\.(?:get|getArgument)\(\s*(?:name\s*=\s*)?([\"'])(.+?)\1\s*(?:,[^)]*)?\)"
)
WIDGET_RM_RE = r"^([ \t]*)dbutils\.widgets\.(?:remove|removeAll)\(.*\)[ \t]*$"
NB_RUN_RE = r"dbutils\.notebook\.run\(\s*([\"'])(.+?)\1"

_TODO_RULES = [
    (
        "DBU-MOUNT",
        r"dbutils\.fs\.(mount|unmount|mounts|refreshMounts|updateMount)\b",
        "Mounts are not supported: use OneLake shortcuts or abfss paths",
    ),
    (
        "DBU-TASKVALUES",
        r"dbutils\.jobs\.taskValues",
        "Use notebookutils.notebook.exit values and pipeline expressions",
    ),
    ("DBU-CONTEXT", r"entry_point|getContext\(\)", "Use notebookutils.runtime.context"),
    (
        "DBU-REMAINING",
        r"dbutils\.",
        "Unconverted dbutils call: replace with notebookutils or redesign",
    ),
    (
        "ADLS-KEY",
        r"fs\.azure\.(account\.key|sas\.|account\.oauth)",
        "Storage key/SAS/OAuth settings in code: use Entra ID / workspace identity, OneLake shortcuts or abfss paths, and remove the key",
    ),
    (
        "DBFS-PATH",
        r"dbfs:/|/dbfs/|[\"']/mnt/",
        "Unmapped DBFS/mount path: add it to mapping.json mount_map or copy the data to OneLake",
    ),
    (
        "AUTOLOADER",
        r"cloudFiles",
        "No Auto Loader: use Structured Streaming file source, pipeline triggers or Eventstream",
    ),
    (
        "DLT",
        r"import dlt\b|@dlt\.|dlt\.(read|table|view)|CREATE\s+(OR\s+REFRESH\s+)?(STREAMING|LIVE)\s+(TABLE|VIEW)",
        "No DLT in Fabric: redesign as Materialized Lake Views / Spark jobs",
    ),
    (
        "COPY-INTO",
        r"\bCOPY\s+INTO\b",
        "Replace with a pipeline Copy activity / Copy job or Spark read + write",
    ),
    (
        "AI-FUNCTIONS",
        r"\b(ai_query|ai_classify|ai_extract|ai_gen|ai_summarize|vector_search|http_request)\s*\(",
        "Databricks AI functions: use Fabric AI functions or Azure OpenAI",
    ),
    (
        "FEATURE-STORE",
        r"databricks\.feature_store|FeatureEngineeringClient|FeatureStoreClient",
        "No Feature Store: rebuild as Delta tables + MLflow",
    ),
    (
        "DBX-SDK",
        r"from databricks\.(sdk|sql|connect)|import databricks_cli|databricks-connect",
        "Replace with Fabric REST APIs / semantic-link / notebookutils",
    ),
    (
        "DELTA-SHARING",
        r"delta_sharing|CREATE\s+SHARE",
        "Use OneLake shortcuts / external data sharing",
    ),
    (
        "SQL-PROC",
        r"\bCREATE\s+(OR\s+REPLACE\s+)?PROCEDURE\b",
        "Rewrite as T-SQL stored procedure in Fabric Warehouse or notebook logic",
    ),
    (
        "MLFLOW-DBX",
        r"databricks-uc|set_registry_uri\([\"']databricks",
        "Point MLflow at the Fabric workspace registry",
    ),
    (
        "GRANTS",
        r"^\s*(GRANT|REVOKE|DENY)\b",
        "Re-implement with Fabric workspace roles / OneLake security / SQL permissions",
    ),
    ("SQL-WIDGET", r"CREATE\s+WIDGET", "Replace with a Fabric parameter cell"),
]
TODO_RULES: list[tuple[str, re.Pattern[str], str]] = [
    (i, re.compile(rx, re.I | re.M), a) for i, rx, a in _TODO_RULES
]

# need testing, but converted or compatible
_TEST_RULES = [
    (
        "STREAMING",
        r"readStream|writeStream|foreachBatch",
        "Structured Streaming: re-validate checkpoints, triggers, sinks",
    ),
    (
        "DELTA-CDF",
        r"table_changes\(|readChangeFeed|enableChangeDataFeed",
        "Change Data Feed: verify support in your Fabric runtime",
    ),
    (
        "LIQUID-CLUSTER",
        r"\bCLUSTER\s+BY\b",
        "Liquid clustering: verify support in your Fabric runtime",
    ),
    (
        "GENERATED-COLS",
        r"GENERATED\s+(ALWAYS|BY\s+DEFAULT)\s+AS",
        "Identity/generated columns: verify Delta protocol support",
    ),
    (
        "ARROW",
        r"pyarrow|useArrow|arrow\.pyspark|import pandas|toPandas\(",
        "Arrow/pandas paths: pyarrow ships with Fabric Spark runtimes; verify version and config",
    ),
    ("ZORDER", r"\bZORDER\s+BY\b", "Prefer V-Order + OPTIMIZE in Fabric"),
    (
        "TIME-TRAVEL",
        r"VERSION\s+AS\s+OF|TIMESTAMP\s+AS\s+OF|versionAsOf|timestampAsOf",
        "Time travel: confirm Delta log retention after the data copy",
    ),
    (
        "SQL-UDF",
        r"\bCREATE\s+(OR\s+REPLACE\s+)?(TEMP(ORARY)?\s+)?FUNCTION\b",
        "SQL UDF: recreate per notebook session",
    ),
    (
        "JDBC-ODBC",
        r"jdbc:|format\([\"']jdbc|pyodbc|\bodbc\b",
        "JDBC/ODBC: recreate as Fabric connection / gateway; secrets in Key Vault",
    ),
    ("PIP-INSTALL", r"%pip install", "Move libraries to a Fabric Environment for repeatability"),
    ("MLFLOW", r"import mlflow", "MLflow is native in Fabric: check experiment and model names"),
    (
        "MLFLOW-EXPERIMENT-PATH",
        r"set_experiment\(\s*[\"']/",
        "Fabric experiments are named items: use a plain name, not a workspace path",
    ),
    (
        "CONSTRAINTS",
        r"ADD\s+CONSTRAINT|\bCHECK\s*\(",
        "Constraints: verify behaviour on the Fabric Delta writer",
    ),
]
TEST_RULES: list[tuple[str, re.Pattern[str], str]] = [
    (i, re.compile(rx, re.I | re.M), a) for i, rx, a in _TEST_RULES
]
COMMENT = {"python": "#", "sql": "--", "scala": "//", "r": "#"}
