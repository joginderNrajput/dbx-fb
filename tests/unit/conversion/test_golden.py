"""Golden notebooks: regenerate with `make snapshots` (pytest --snapshot-update) and REVIEW the diff."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.conversion.code import Ctx
from app.conversion.notebook import build_ipynb, convert_cells, parse_source
from app.migration.mapping import load_cfg
from app.sources.offline import detect_language
from tests.conftest import FIXTURES

EXPECTED = FIXTURES / "expected_notebooks"
CASES = sorted(p for p in (FIXTURES / "notebooks").rglob("*") if p.is_file())


@pytest.mark.parametrize("path", CASES, ids=lambda p: p.name)
def test_golden(path: Path, snapshot_update: bool) -> None:
    cfg = load_cfg(None)
    cfg.update(
        catalog_to_lakehouse={"main": "lh_main"},
        mount_map={"/mnt/raw": "Files/raw"},
        keyvault_urls={"kv-sales": "https://kv-sales.vault.azure.net/"},
    )
    text = path.read_text()
    ctx = Ctx(cfg, ["main"])
    nb = build_ipynb(
        convert_cells(parse_source(text, detect_language(path, text)), ctx, "/Repos/team/etl/x")
    )
    result = {
        "notebook": nb,
        "todos": ctx.todos,
        "changes": ctx.changes,
        "warnings": {k: list(v) for k, v in ctx.warns.items()},
    }
    golden = EXPECTED / (path.stem + ".json")
    if snapshot_update:
        golden.parent.mkdir(parents=True, exist_ok=True)
        golden.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    assert golden.exists(), f"missing golden {golden.name}: run `make snapshots` and review it"
    assert json.loads(golden.read_text()) == json.loads(json.dumps(result))
