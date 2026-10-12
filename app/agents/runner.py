"""The agent loop: Claude calls migration tools until it finishes or the turn budget runs out. Spec §6.1."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from app.agents.llm import LLMClient
from app.agents.prompts import AGENT_SYSTEM, agent_kickoff
from app.agents.tools import TOOLS, FinishSignal, execute_tool
from app.core.exceptions import Cancelled
from app.core.logging import get_logger
from app.migration.session import MigrationSession

__all__ = ["AgentResult", "run_agent"]

log = get_logger(__name__)


@dataclass
class AgentResult:
    summary: str
    stopped: str  # finished | end_turn | max_turns | refusal | error
    turns: int
    trace: list[dict[str, Any]] = field(default_factory=list)


def _step(kind: str, **data: Any) -> dict[str, Any]:
    return {"at": datetime.now(UTC).isoformat(timespec="seconds"), "kind": kind, **data}


def run_agent(
    session: MigrationSession,
    llm: LLMClient,
    *,
    max_turns: int,
    instructions: str | None = None,
    on_step: Callable[[list[dict[str, Any]]], None] | None = None,
) -> AgentResult:
    """Manual tool-use loop (no beta runner) so every call is gated, traced and budgeted here."""
    messages: list[dict[str, Any]] = [{"role": "user", "content": agent_kickoff(instructions)}]
    trace: list[dict[str, Any]] = []

    def emit(step: dict[str, Any]) -> None:
        trace.append(step)
        if on_step:
            on_step(trace)

    turns = 0
    while turns < max_turns:
        turns += 1
        session.stage("agent")  # cancellation / timeout checkpoint between turns
        resp = llm.create_message(system=AGENT_SYSTEM, messages=messages, tools=TOOLS)
        if resp.text:
            emit(_step("assistant", text=resp.text[:4000]))
        messages.append({"role": "assistant", "content": resp.assistant_content})
        if resp.stop_reason == "refusal":
            emit(_step("stopped", reason="refusal"))
            return AgentResult(
                resp.text or "The model declined to continue.", "refusal", turns, trace
            )
        if resp.stop_reason == "pause_turn":
            continue
        if resp.stop_reason == "max_tokens" and not resp.tool_calls:
            messages.append(
                {
                    "role": "user",
                    "content": "Your last response hit the output limit. Continue, more concisely.",
                }
            )
            continue
        if not resp.tool_calls:
            emit(_step("stopped", reason="end_turn"))
            return AgentResult(resp.text, "end_turn", turns, trace)
        results = []
        for call in resp.tool_calls:
            try:
                text, is_error = execute_tool(session, call.name, call.input)
            except FinishSignal as f:
                emit(_step("tool", name=call.name, input=call.input, result="finished"))
                return AgentResult(f.summary, "finished", turns, trace)
            except Cancelled:
                raise
            emit(
                _step(
                    "tool",
                    name=call.name,
                    input=_brief(call.input),
                    result=text[:2000],
                    is_error=is_error,
                )
            )
            results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": call.id,
                    "content": text,
                    **({"is_error": True} if is_error else {}),
                }
            )
        messages.append(
            {"role": "user", "content": results}
        )  # all results of one turn in ONE message
    emit(_step("stopped", reason="max_turns"))
    log.warning("agent_turn_budget_exhausted", turns=turns)
    return AgentResult(
        "Agent stopped: turn budget exhausted before finish.", "max_turns", turns, trace
    )


def _brief(inp: dict[str, Any]) -> dict[str, Any]:
    """Trim large tool inputs (rewritten cell sources) in the stored trace."""
    return {
        k: (v[:500] + "..." if isinstance(v, str) and len(v) > 500 else v) for k, v in inp.items()
    }
