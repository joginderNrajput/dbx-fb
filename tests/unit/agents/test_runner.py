from __future__ import annotations

from pathlib import Path

from app.agents.llm import LLMResponse
from app.agents.runner import run_agent
from app.agents.tools import TOOLS, execute_tool
from app.conversion.common import TEST
from app.core.config import Settings
from app.migration.session import MigrationSession, SessionSpec
from tests.fakes import FakeLLM, call, calls, fix_first_todo_cell


def session(notebooks_dir: Path, tmp_path: Path, settings: Settings) -> MigrationSession:
    nbs = [str(p.relative_to(notebooks_dir)) for p in notebooks_dir.rglob("*") if p.is_file()]
    return MigrationSession(
        SessionSpec(
            source_kind="upload",
            sources_dir=notebooks_dir,
            out_dir=tmp_path / "out",
            uploaded_notebooks=nbs,
        ),
        settings,
    )


def test_agent_runs_stages_fixes_cells_and_finishes(
    notebooks_dir: Path, tmp_path: Path, settings: Settings
) -> None:
    s = session(notebooks_dir, tmp_path, settings)
    llm = FakeLLM(
        [
            call("get_job_context"),
            calls(("inventory_source", {}), ("plan_migration", {})),
            call("convert_assets"),
            call("read_notebook", name="explore"),
            fix_first_todo_cell("explore", "user = notebookutils.runtime.context['userName']"),
            call("run_preflight"),
            call("deploy_to_fabric"),
            call("finish", summary="4 notebooks converted; explore fixed"),
        ]
    )
    res = run_agent(s, llm, max_turns=20)
    assert res.stopped == "finished" and res.summary.startswith("4 notebooks")
    assert s.get_item("Notebook:explore")["outcome"] == TEST
    tools_used = [t["name"] for t in res.trace if t["kind"] == "tool"]
    assert tools_used[:4] == [
        "get_job_context",
        "inventory_source",
        "plan_migration",
        "convert_assets",
    ]
    deploy_step = next(t for t in res.trace if t.get("name") == "deploy_to_fabric")
    assert "deploy was not requested" in deploy_step["result"]
    # both results of the parallel turn went back in ONE user message
    parallel_reply = llm.requests[2][-1]
    assert parallel_reply["role"] == "user" and len(parallel_reply["content"]) == 2


def test_tool_errors_are_returned_not_raised(
    notebooks_dir: Path, tmp_path: Path, settings: Settings
) -> None:
    s = session(notebooks_dir, tmp_path, settings)
    text, is_error = execute_tool(s, "convert_assets", {})
    assert is_error and "run plan before convert" in text
    text, is_error = execute_tool(
        s, "record_result", {"id": "x", "outcome": "migrated", "note": "n"}
    )
    assert is_error
    assert execute_tool(s, "nope", {})[1]


def test_turn_budget_and_refusal(notebooks_dir: Path, tmp_path: Path, settings: Settings) -> None:
    s = session(notebooks_dir, tmp_path, settings)
    res = run_agent(s, FakeLLM([call("get_job_context")] * 5), max_turns=3)
    assert res.stopped == "max_turns" and res.turns == 3
    res = run_agent(s, FakeLLM([lambda m: LLMResponse("refusal", "", [], [])]), max_turns=3)
    assert res.stopped == "refusal"


def test_tool_schemas_are_closed() -> None:
    assert all(t["input_schema"]["additionalProperties"] is False for t in TOOLS)
    assert len({t["name"] for t in TOOLS}) == len(TOOLS)
