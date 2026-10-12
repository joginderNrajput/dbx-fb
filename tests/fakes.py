"""Scripted stand-in for the Claude client (no network, deterministic)."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from app.agents.llm import LLMResponse, ToolCall

Step = Callable[[list[dict[str, Any]]], LLMResponse]


def call(tool: str, **inp: Any) -> Step:
    return lambda _m: LLMResponse(
        "tool_use", "", [ToolCall(f"tu_{tool}", tool, inp)], [{"type": "tool_use", "name": tool}]
    )


def calls(*pairs: tuple[str, dict[str, Any]]) -> Step:
    return lambda _m: LLMResponse(
        "tool_use", "", [ToolCall(f"tu_{n}_{i}", n, inp) for i, (n, inp) in enumerate(pairs)], []
    )


def last_tool_result(messages: list[dict[str, Any]]) -> Any:
    return json.loads(messages[-1]["content"][-1]["content"])


def fix_first_todo_cell(notebook: str, source: str) -> Step:
    """Reads the previous read_notebook result and rewrites the first cell flagged has_todo."""

    def step(messages: list[dict[str, Any]]) -> LLMResponse:
        cells = last_tool_result(messages)["cells"]
        idx = next(c["index"] for c in cells if c["has_todo"])
        return LLMResponse(
            "tool_use",
            "",
            [
                ToolCall(
                    "tu_fix",
                    "update_notebook_cell",
                    {"name": notebook, "index": idx, "source": source, "note": "resolved"},
                )
            ],
            [],
        )

    return step


class FakeLLM:
    def __init__(self, steps: list[Step], completion: str = "") -> None:
        self.steps = list(steps)
        self.completion = completion
        self.requests: list[list[dict[str, Any]]] = []
        self.prompts: list[str] = []

    def create_message(
        self, *, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> LLMResponse:
        self.requests.append(list(messages))
        if not self.steps:
            return LLMResponse("end_turn", "done", [], [{"type": "text", "text": "done"}])
        return self.steps.pop(0)(messages)

    def complete(self, prompt: str) -> str:
        self.prompts.append(prompt)
        return self.completion
