"""The port must behave exactly like dbx2fabric_migrate_v4.py (ADR 0003)."""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from app.conversion import jobs, notebook
from app.conversion.code import Ctx
from app.migration import engine, planner
from app.migration.ledger import do_plan
from app.migration.mapping import init_mapping
from app.sources.lookup import SourceResolver
from tests.conftest import FIXTURES

NOTEBOOKS = sorted(p for p in (FIXTURES / "notebooks").rglob("*") if p.is_file())
LANG = {".py": "PYTHON", ".sql": "SQL", ".ipynb": "PYTHON"}


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: p.name)
def test_notebook_conversion_matches_reference(
    reference: ModuleType, assessment: dict[str, Any], path: Path
) -> None:
    cfg = init_mapping(assessment)
    cfg["mount_map"]["/mnt/raw"] = "Files/raw"
    text, lang = path.read_text(), LANG[path.suffix]
    cats = ["main"]
    ref_ctx = reference.Ctx(cfg, cats)
    ref = reference.build_ipynb(
        reference.convert_cells(reference.parse_source(text, lang), ref_ctx, "/Repos/team/etl/x"),
        "h",
    )
    ctx = Ctx(cfg, cats)
    ours = notebook.build_ipynb(
        notebook.convert_cells(notebook.parse_source(text, lang), ctx, "/Repos/team/etl/x"), "h"
    )
    assert ours == ref
    assert ctx.todos == ref_ctx.todos
    assert ctx.changes == ref_ctx.changes
    assert ctx.warns == ref_ctx.warns
    assert notebook.outcome_from_ctx(ctx) == reference.outcome_from_ctx(ref_ctx)


@pytest.mark.parametrize(
    "expr",
    [
        "0 30 2 * * ?",
        "0 0/15 * * * ?",
        "0 0 0/2 * * ?",
        "0 0 6,18 * * ?",
        "0 0 7 ? * MON-FRI",
        "0 0 7 ? * 2,4,6",
        "0 0 7 ? * SAT-MON",
        "0 0 1 1 * ?",
        "0 0 1 ? JAN *",
        "bad",
        "0 0 7 ? * XYZ",
        None,
    ],
)
def test_quartz_matches_reference(reference: ModuleType, expr: str | None) -> None:
    assert jobs.quartz_to_fabric(expr) == reference.quartz_to_fabric(expr)


def _strip_times(o: Any) -> Any:
    s = json.dumps(o)
    return json.loads(re.sub(r'"(startDateTime|endDateTime)": "[^"]+"', r'"\1": "T"', s))


def test_job_conversion_matches_reference(
    reference: ModuleType, assessment: dict[str, Any]
) -> None:
    cfg = init_mapping(assessment)
    job = next(a for a in assessment["assets"] if a["name"] == "nightly_sales")
    assert _strip_times(jobs.convert_job(job, cfg)) == _strip_times(reference.convert_job(job, cfg))


def test_plan_and_mapping_match_reference(
    reference: ModuleType, assessment: dict[str, Any], tmp_path: Path
) -> None:
    cfg = init_mapping(assessment)
    assert cfg == reference.init_mapping(assessment, tmp_path / "mapping.json")
    assert planner.plan(assessment, cfg) == reference.plan(assessment, cfg)


class _Args:
    def __init__(self, source_dir: Path) -> None:
        self.host = self.token = None
        self.source_dir = str(source_dir)
        self.ai = False


# Messages that intentionally changed in the service (they used to name CLI flags / local paths).
REWORDED = (
    ("Source code not available", "Source code not available"),
    ("dbt project folder not provided", "dbt project folder not provided"),
)


def _norm_items(items: list[dict[str, Any]], out: Path) -> list[dict[str, Any]]:
    norm = json.loads(json.dumps(items).replace(str(out), "<OUT>"))
    for it in norm:
        it["reasons"] = [
            next((tag for prefix, tag in REWORDED if r.startswith(prefix)), r)
            for r in it["reasons"]
        ]
    return norm


def test_full_convert_matches_reference(
    reference: ModuleType, assessment: dict[str, Any], tmp_path: Path
) -> None:
    cfg = init_mapping(assessment)
    cfg["mount_map"]["/mnt/raw"] = "Files/raw"
    cfg["staging_storage"] = "abfss://stage@mine.dfs.core.windows.net/dbx_export"
    src = FIXTURES / "notebooks"
    ref_out, our_out = tmp_path / "ref", tmp_path / "ours"
    ref_out.mkdir()
    reference.save_json(ref_out / "ledger.json", {"items": reference.plan(assessment, cfg)})
    reference.convert_all(ref_out, assessment, cfg, _Args(src))
    do_plan(assessment, cfg, our_out)
    engine.convert_all(our_out, assessment, cfg, source_lookup=SourceResolver(src, None))

    ref_led = json.loads((ref_out / "ledger.json").read_text())
    our_led = json.loads((our_out / "ledger.json").read_text())
    assert _norm_items(our_led["items"], our_out) == _norm_items(ref_led["items"], ref_out)
    assert json.loads(
        json.dumps(our_led["artifacts"]).replace(str(our_out), "<OUT>")
    ) == json.loads(json.dumps(ref_led["artifacts"]).replace(str(ref_out), "<OUT>"))

    # every generated file is identical, except texts that name the service's API instead of the CLI
    differ_by_design = {"artifacts/notebooks/validate_migration.ipynb"}
    ref_files = {
        str(p.relative_to(ref_out)) for p in (ref_out / "artifacts").rglob("*") if p.is_file()
    }
    our_files = {
        str(p.relative_to(our_out)) for p in (our_out / "artifacts").rglob("*") if p.is_file()
    }
    assert our_files == ref_files
    for rel in sorted(ref_files - differ_by_design):
        a, b = (ref_out / rel).read_text(), (our_out / rel).read_text()
        if rel.endswith(".schedule.json"):
            a, b = json.dumps(_strip_times(json.loads(a))), json.dumps(_strip_times(json.loads(b)))
        assert b == a, rel
    shutil.rmtree(tmp_path)
