from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import respx

from app.agents.llm import AzureOpenAILLM, build_llm
from app.agents.runner import run_agent
from app.agents.tools import TOOLS
from app.core.exceptions import AIUnavailable
from app.migration.session import MigrationSession, SessionSpec
from tests.conftest import make_settings

URL = "https://res.openai.azure.com/openai/deployments/gpt-dep/chat/completions"


def settings(tmp_path: Path, **over: object):  # type: ignore[no-untyped-def]
    base = dict(
        LLM_PROVIDER="azure_openai",
        LLM_BASE_URL="https://res.openai.azure.com/",
        LLM_API_KEY="k",
        LLM_MODEL="gpt-dep",
        AI_MAX_RETRIES=1,
    )
    base.update(over)
    return make_settings(tmp_path, **base)


def reply(
    content: str = "", tool_calls: list[dict] | None = None, finish: str = "stop"
) -> httpx.Response:
    msg: dict = {"role": "assistant", "content": content}
    if tool_calls:
        msg["tool_calls"] = tool_calls
    return httpx.Response(200, json={"choices": [{"finish_reason": finish, "message": msg}]})


def tc(i: str, name: str, args: dict) -> dict:
    return {"id": i, "type": "function", "function": {"name": name, "arguments": json.dumps(args)}}


def test_config_and_factory(tmp_path: Path) -> None:
    assert settings(tmp_path).ai_configured and settings(tmp_path).ai_model == "gpt-dep"
    assert not settings(tmp_path, LLM_API_KEY=None).ai_configured
    assert isinstance(build_llm(settings(tmp_path)), AzureOpenAILLM)
    with pytest.raises(AIUnavailable):
        AzureOpenAILLM(settings(tmp_path, LLM_MODEL=""))


def test_message_and_tool_translation() -> None:
    msgs = [
        {"role": "user", "content": "go"},
        {
            "role": "assistant",
            "content": {"role": "assistant", "content": "", "tool_calls": [tc("c1", "x", {})]},
        },
        {
            "role": "user",
            "content": [
                {"type": "tool_result", "tool_use_id": "c1", "content": "bad", "is_error": True}
            ],
        },
    ]
    out = AzureOpenAILLM.to_openai("sys", msgs)
    assert [m["role"] for m in out] == ["system", "user", "assistant", "tool"]
    assert out[3] == {"role": "tool", "tool_call_id": "c1", "content": "ERROR: bad"}
    t = AzureOpenAILLM.to_openai_tools(TOOLS)[0]
    assert t["type"] == "function" and t["function"]["parameters"]["type"] == "object"


def test_agent_loop_over_azure(tmp_path: Path, notebooks_dir: Path) -> None:
    s = settings(tmp_path)
    nbs = [str(p.relative_to(notebooks_dir)) for p in notebooks_dir.rglob("*") if p.is_file()]
    session = MigrationSession(
        SessionSpec(
            source_kind="upload",
            sources_dir=notebooks_dir,
            out_dir=tmp_path / "out",
            uploaded_notebooks=nbs,
        ),
        s,
    )
    with respx.mock() as router:
        route = router.post(url__startswith=URL).mock(
            side_effect=[
                httpx.Response(429, headers={"retry-after": "0"}),
                reply(
                    tool_calls=[tc("a", "inventory_source", {}), tc("b", "plan_migration", {})],
                    finish="tool_calls",
                ),
                reply(tool_calls=[tc("c", "convert_assets", {})], finish="tool_calls"),
                reply(tool_calls=[tc("d", "finish", {"summary": "ok"})], finish="tool_calls"),
            ]
        )
        res = run_agent(session, build_llm(s), max_turns=10)
    assert res.stopped == "finished" and res.summary == "ok"
    req = json.loads(route.calls[2].request.content)
    assert route.calls[2].request.headers[
        "api-key"
    ] == "k" and "api-version=2025-01-01-preview" in str(route.calls[2].request.url)
    assert req["max_completion_tokens"] == s.AI_MAX_TOKENS and "max_tokens" not in req
    roles = [m["role"] for m in req["messages"]]
    assert roles == [
        "system",
        "user",
        "assistant",
        "tool",
        "tool",
    ]  # both parallel results answered


def test_cell_fix_and_errors(tmp_path: Path) -> None:
    llm = AzureOpenAILLM(settings(tmp_path, AI_MAX_RETRIES=0))
    with respx.mock() as router:
        router.post(url__startswith=URL).mock(
            side_effect=[reply("```python\nx=1\n```"), httpx.Response(401, text="bad key")]
        )
        assert "x=1" in llm.complete("fix")
        with pytest.raises(AIUnavailable, match="401"):
            llm.complete("fix")
