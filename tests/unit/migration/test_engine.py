from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from app.conversion.common import MIG, NOT, PART, TEST
from app.migration.engine import catalogs_for, convert_all
from app.migration.ledger import do_plan, record_result, summarize_ledger
from app.migration.mapping import init_mapping
from app.migration.preflight import preflight
from app.migration.report import write_report
from app.migration.validation import import_validation
from app.sources.lookup import SourceResolver
from tests.conftest import FIXTURES


def run(
    assessment: dict[str, Any], tmp_path: Path, **cfg_over: Any
) -> tuple[dict[str, Any], dict[str, Any], Path]:
    cfg = init_mapping(assessment)
    cfg.update(cfg_over)
    out = tmp_path / "out"
    do_plan(assessment, cfg, out)
    src = tmp_path / "src"
    shutil.copytree(FIXTURES / "notebooks", src)
    shutil.copytree(FIXTURES / "dbt", src / "dbt")
    led = convert_all(out, assessment, cfg, source_lookup=SourceResolver(src, None), dbt_root=src)
    return led, cfg, out


def outcome(led: dict[str, Any], item_id: str) -> str:
    return str(next(i for i in led["items"] if i["id"] == item_id)["outcome"])


def test_outcomes_per_asset_type(assessment: dict[str, Any], tmp_path: Path) -> None:
    led, _, out = run(assessment, tmp_path)
    assert outcome(led, "Notebook:load_sales") == PART  # mount + autoloader + %sh blockers
    assert outcome(led, "Notebook:publish") == TEST
    assert outcome(led, "Notebook:missing_nb") == NOT
    assert outcome(led, "Job:nightly_sales") == PART  # SQL task omitted
    assert outcome(led, "Job:sql_only") == NOT
    assert (
        outcome(led, "Table/View:main.bronze.sales_raw") == PART
    )  # shortcut without connection id
    assert outcome(led, "Table/View:main.gold.daily_sales") == NOT  # managed, no staging storage
    assert outcome(led, "SQL Warehouse:Shared WH") == MIG
    assert outcome(led, "Secret Scope:kv-sales") == NOT
    assert outcome(led, "dbt Project:dbt_nightly") == NOT  # folder not mapped
    assert (out / "RUNBOOK.md").exists() and (out / "ai_tasks").is_dir()
    assert (out / "artifacts/security/keyvault_secrets.csv").read_text().count("kv-sales") == 2


def test_staging_and_connection_unblock_items(assessment: dict[str, Any], tmp_path: Path) -> None:
    led, _, out = run(
        assessment,
        tmp_path,
        staging_storage="abfss://stage@me.dfs.core.windows.net/x",
        adls_connection_id="conn-1",
        dbt_projects={"dbt_nightly": "dbt/sales_dbt"},
    )
    assert outcome(led, "Table/View:main.gold.daily_sales") == TEST
    assert outcome(led, "Table/View:main.bronze.sales_raw") == TEST
    assert outcome(led, "dbt Project:dbt_nightly") == PART  # file_format=delta flagged
    assert (out / "artifacts/databricks/export_to_staging.py").exists()
    assert (out / "artifacts/dbt/dbt_nightly/profiles.databricks.yml").exists()
    assert "type: fabric" in (out / "artifacts/dbt/dbt_nightly/profiles.yml").read_text()


def test_dbt_folder_outside_sources_is_refused(assessment: dict[str, Any], tmp_path: Path) -> None:
    led, _, _ = run(assessment, tmp_path, dbt_projects={"dbt_nightly": "../../../etc"})
    assert outcome(led, "dbt Project:dbt_nightly") == NOT


def test_catalogs_include_mapping_keys() -> None:
    assert catalogs_for({"assets": []}, {"catalog_to_lakehouse": {"hive_metastore": "lh_h"}}) == [
        "hive_metastore"
    ]


def test_preflight_report_validation_and_results(
    assessment: dict[str, Any], tmp_path: Path
) -> None:
    led, cfg, out = run(
        assessment, tmp_path, staging_storage="abfss://stage@me.dfs.core.windows.net/x"
    )
    issues = preflight(out, cfg)
    assert any(i[0] == "warn" and "TODO" in i[2] for i in issues)
    assert not [i for i in issues if i[0] == "error"]
    s = write_report(out)
    assert s["total"] == len(led["items"]) and (out / "post_migration_report.html").exists()
    n = import_validation(
        out,
        [
            {"id": "Table/View:main.gold.daily_sales", "status": "PASS", "rows_target": 10},
            {"id": "Table/View:main.silver.customers", "status": "FAIL", "note": "rows 1 != 2"},
        ],
    )
    assert n == {"PASS": 1, "FAIL": 1, "other": 0}
    record_result(out, "Dashboard:Sales KPIs", TEST, "rebuilt in Power BI")
    led2 = summarize_ledger(
        __import__("app.migration.ledger", fromlist=["load_ledger"]).load_ledger(out)
    )
    assert led2[MIG] >= 2


def test_preflight_flags_bad_lakehouse_names(assessment: dict[str, Any], tmp_path: Path) -> None:
    _, cfg, out = run(assessment, tmp_path, lakehouse_default="1-bad")
    assert any(i[0] == "error" and "lakehouse_default" in i[2] for i in preflight(out, cfg))
