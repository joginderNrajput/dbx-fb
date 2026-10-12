from __future__ import annotations

from app.agents.cell_fixer import CellFixer
from app.conversion.code import Ctx
from app.migration.mapping import load_cfg
from tests.fakes import FakeLLM


def test_only_todo_cells_sent_and_budget_respected() -> None:
    llm = FakeLLM([], completion="Here:\n```python\nx = notebookutils.runtime.context\n```")
    ctx = Ctx(load_cfg(None), [])
    ctx.todos.append({"rule": "DBU-CONTEXT", "advice": "", "line": ""})
    cells = [
        {"kind": "code", "src": "a = 1"},
        {"kind": "code", "src": "# TODO[dbx2fabric:DBU-CONTEXT] x\nctx = getContext()"},
        {"kind": "code", "src": "# TODO[dbx2fabric:DBU-CONTEXT] y"},
    ]
    fixer = CellFixer(llm, max_cells=1)
    assert fixer(cells, ctx) == 1
    assert cells[1]["src"] == "x = notebookutils.runtime.context" and "TODO" in cells[2]["src"]
    assert len(llm.prompts) == 1 and "DBU-CONTEXT" in llm.prompts[0]


def test_unfenced_answer_leaves_cell() -> None:
    ctx = Ctx(load_cfg(None), [])
    cells = [{"kind": "code", "src": "# TODO[dbx2fabric:X] z"}]
    assert CellFixer(FakeLLM([], completion="no code"), 5)(cells, ctx) == 0
