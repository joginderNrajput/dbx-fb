from __future__ import annotations

from app.conversion.code import Ctx, convert_code
from app.migration.mapping import load_cfg


def ctx(**cfg_over: object) -> Ctx:
    cfg = load_cfg(None)
    cfg.update(cfg_over)
    return Ctx(cfg, ["main"])


def test_secrets_become_key_vault_lookups() -> None:
    c = ctx(keyvault_urls={"kv": "https://v.vault.azure.net/"})
    out = convert_code('pw = dbutils.secrets.get(scope="kv", key="pw")', "python", c)
    assert out == 'pw = notebookutils.credentials.getSecret("https://v.vault.azure.net/", "pw")'
    assert c.secrets == {("kv", "pw")}


def test_widgets_become_parameters() -> None:
    c = ctx()
    out = convert_code(
        'dbutils.widgets.text("d", "2024")\nx = dbutils.widgets.get("d")', "python", c
    )
    assert "parameter cell" in out and "x = d" in out
    assert c.params == {"d": "2024"}


def test_catalog_three_part_names_and_warehouse_refs() -> None:
    c = ctx(catalog_to_lakehouse={"main": "lh_main"})
    out = convert_code("spark.table('main.gold.sales')", "python", c)
    assert "lh_main.gold.sales" in out
    assert c.lakehouses == ["lh_main"] and "WH-LANDING" in c.warns


def test_mount_paths_rewritten() -> None:
    c = ctx(mount_map={"/mnt/raw": "Files/raw"})
    out = convert_code(
        'spark.read.load("/mnt/raw/x") ; spark.read.load("dbfs:/mnt/raw/y")', "python", c
    )
    assert '"Files/raw/x"' in out and '"Files/raw/y"' in out


def test_blockers_get_todo_markers_first_rule_wins() -> None:
    c = ctx()
    out = convert_code('dbutils.fs.mount("a", "/mnt/b")', "python", c)
    assert out.splitlines()[0].strip().startswith("# TODO[dbx2fabric:DBU-MOUNT]")
    assert [t["rule"] for t in c.todos] == ["DBU-MOUNT"]


def test_sql_comment_style_and_databricks_set_removed() -> None:
    c = ctx()
    out = convert_code("SET spark.databricks.x = 1;\nCOPY INTO t FROM 'p'", "sql", c)
    assert out.startswith("-- [dbx2fabric] removed: SET spark.databricks.x")
    assert "-- TODO[dbx2fabric:COPY-INTO]" in out


def test_needs_testing_warnings_counted() -> None:
    c = ctx()
    convert_code("df = spark.readStream.load(p)\nq = df.writeStream.start()", "python", c)
    assert c.warns["STREAMING"][0] == 2
