from __future__ import annotations

from pathlib import Path

import pytest

from app.conversion.common import PART, TEST
from app.core.config import Settings
from app.core.exceptions import ValidationFailed
from app.migration.session import MigrationSession, SessionSpec


def offline_session(notebooks_dir: Path, tmp_path: Path, settings: Settings) -> MigrationSession:
    nbs = [str(p.relative_to(notebooks_dir)) for p in notebooks_dir.rglob("*") if p.is_file()]
    spec = SessionSpec(
        source_kind="upload",
        sources_dir=notebooks_dir,
        out_dir=tmp_path / "out",
        uploaded_notebooks=nbs,
        mapping_overrides={"catalog_to_lakehouse": {"main": "lh_main"}},
    )
    return MigrationSession(spec, settings)


def test_offline_pipeline(notebooks_dir: Path, tmp_path: Path, settings: Settings) -> None:
    s = offline_session(notebooks_dir, tmp_path, settings)
    inv = s.inventory()
    assert inv["by_type"] == {"Notebook": 4}
    s.plan()
    summary = s.convert()
    assert summary["total"] == 4 and "load_sales" in summary["notebooks_with_todos"]
    nb = (tmp_path / "out/artifacts/notebooks/load_sales.ipynb").read_text()
    assert "lh_main.bronze.sales_raw" in nb  # catalog from the mapping, no table inventory needed
    assert s.run_preflight() is not None and s.report()["total"] == 4


def test_update_cell_moves_partial_to_needs_testing(
    notebooks_dir: Path, tmp_path: Path, settings: Settings
) -> None:
    s = offline_session(notebooks_dir, tmp_path, settings)
    s.inventory()
    s.plan()
    s.convert()
    assert s.get_item("Notebook:explore")["outcome"] == PART
    cells = s.read_notebook("explore")["cells"]
    todo = [c for c in cells if c["has_todo"]]
    assert len(todo) == 1
    res = s.update_notebook_cell(
        "explore",
        todo[0]["index"],
        "user = notebookutils.runtime.context['userName']",
        note="context",
    )
    assert res["remaining_todo_markers"] == 0
    item = s.get_item("Notebook:explore")
    assert item["outcome"] == TEST and not any(r.startswith("TODO ") for r in item["reasons"])
    with pytest.raises(ValidationFailed):
        s.update_notebook_cell("explore", 0, "x")  # markdown header cell


def test_convert_requires_plan(notebooks_dir: Path, tmp_path: Path, settings: Settings) -> None:
    s = offline_session(notebooks_dir, tmp_path, settings)
    s.inventory()
    with pytest.raises(ValidationFailed):
        s.convert()


def test_deploy_without_fabric_credentials_refused(
    notebooks_dir: Path, tmp_path: Path, settings: Settings
) -> None:
    s = offline_session(notebooks_dir, tmp_path, settings)
    with pytest.raises(ValidationFailed):
        s.deploy()
