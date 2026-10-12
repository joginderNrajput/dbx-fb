"""LLM clients behind one protocol. Spec §6.1.

The ONLY module that imports the Anthropic SDK (`ai` extra, imported lazily). Azure OpenAI is
called over httpx (chat completions with tools); the agent loop keeps one conversation format
(Anthropic-style blocks) and the Azure client translates it (ADR 0006).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Protocol

import httpx

from app.core.config import Settings
from app.core.exceptions import AIUnavailable
from app.core.metrics import AI_CALLS

__all__ = ["AnthropicLLM", "AzureOpenAILLM", "LLMClient", "LLMResponse", "ToolCall", "build_llm"]


@dataclass
class ToolCall:
    id: str
    name: str
    input: dict[str, Any]


@dataclass
class LLMResponse:
    stop_reason: str
    text: str
    tool_calls: list[ToolCall] = field(default_factory=list)
    # Opaque assistant content to append to the conversation unchanged (thinking blocks included).
    assistant_content: Any = None


class LLMClient(Protocol):
    def create_message(
        self, *, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> LLMResponse: ...

    def complete(self, prompt: str) -> str: ...


class AnthropicLLM:
    def __init__(self, settings: Settings) -> None:
        try:
            import anthropic
        except ImportError as e:  # the `ai` extra is optional
            raise AIUnavailable(
                "the Anthropic SDK is not installed: install the `ai` extra (uv sync --extra ai)"
            ) from e
        if not settings.ai_configured:
            raise AIUnavailable("ANTHROPIC_API_KEY is not set")
        assert settings.ANTHROPIC_API_KEY is not None
        self._anthropic = anthropic
        self.settings = settings
        self.client = anthropic.Anthropic(
            api_key=settings.ANTHROPIC_API_KEY.get_secret_value(),
            timeout=float(settings.AI_TIMEOUT_S),
            max_retries=settings.AI_MAX_RETRIES,
        )

    def _create(self, kind: str, **kwargs: Any) -> Any:
        s = self.settings
        params: dict[str, Any] = {"model": s.AI_MODEL, "max_tokens": s.AI_MAX_TOKENS, **kwargs}
        if s.AI_EFFORT:
            params["output_config"] = {"effort": s.AI_EFFORT}
        a = self._anthropic
        try:
            if s.AI_REFUSAL_FALLBACK:
                # On a policy decline the API re-runs the request on a fallback model in the same call.
                resp = self.client.beta.messages.create(
                    betas=["server-side-fallback-2026-07-01"], fallbacks="default", **params
                )
            else:
                resp = self.client.messages.create(**params)
        except a.AuthenticationError as e:
            AI_CALLS.labels(kind, "auth_error").inc()
            raise AIUnavailable(f"Anthropic API rejected the key: {e.message}") from e
        except a.RateLimitError as e:
            AI_CALLS.labels(kind, "rate_limited").inc()
            raise AIUnavailable(f"Anthropic API rate limit: {e.message}") from e
        except a.APIStatusError as e:
            AI_CALLS.labels(kind, f"http_{e.status_code}").inc()
            raise AIUnavailable(f"Anthropic API error {e.status_code}: {e.message}") from e
        except a.APIConnectionError as e:
            AI_CALLS.labels(kind, "network").inc()
            raise AIUnavailable(f"Anthropic API unreachable: {e}") from e
        AI_CALLS.labels(kind, resp.stop_reason or "ok").inc()
        return resp

    def create_message(
        self, *, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> LLMResponse:
        resp = self._create("agent", system=system, messages=messages, tools=tools)
        text = "".join(b.text for b in resp.content if b.type == "text")
        calls = [
            ToolCall(b.id, b.name, dict(b.input or {}))
            for b in resp.content
            if b.type == "tool_use"
        ]
        return LLMResponse(
            stop_reason=resp.stop_reason or "end_turn",
            text=text,
            tool_calls=calls,
            assistant_content=resp.content,
        )

    def complete(self, prompt: str) -> str:
        resp = self._create("cell_fix", messages=[{"role": "user", "content": prompt}])
        if resp.stop_reason == "refusal":
            return ""
        return "".join(b.text for b in resp.content if b.type == "text")


_FINISH = {
    "stop": "end_turn",
    "tool_calls": "tool_use",
    "length": "max_tokens",
    "content_filter": "refusal",
}


class AzureOpenAILLM:
    """Azure OpenAI chat completions with function tools (deployment addressed in the URL)."""

    def __init__(self, settings: Settings, transport: httpx.BaseTransport | None = None) -> None:
        if not settings.ai_configured:
            raise AIUnavailable(
                "LLM_PROVIDER=azure_openai needs LLM_BASE_URL, LLM_API_KEY and LLM_MODEL (deployment name)"
            )
        assert settings.LLM_API_KEY is not None
        self.settings = settings
        self.url = (
            f"{settings.LLM_BASE_URL.rstrip('/')}/openai/deployments/{settings.LLM_MODEL}/chat/completions"
            f"?api-version={settings.LLM_API_VERSION}"
        )
        self.http = httpx.Client(
            timeout=float(settings.AI_TIMEOUT_S),
            transport=transport,
            headers={
                "api-key": settings.LLM_API_KEY.get_secret_value(),
                "Content-Type": "application/json",
            },
        )

    @staticmethod
    def to_openai(system: str, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Anthropic-style conversation -> OpenAI chat messages.

        Assistant turns carry the OpenAI assistant message produced by this client (opaque
        ``assistant_content``); user turns are a string or a list of tool_result blocks.
        """
        out: list[dict[str, Any]] = [{"role": "system", "content": system}] if system else []
        for m in messages:
            content = m["content"]
            if m["role"] == "assistant":
                out.append(
                    content
                    if isinstance(content, dict) and content.get("role") == "assistant"
                    else {"role": "assistant", "content": str(content)}
                )
            elif isinstance(content, str):
                out.append({"role": "user", "content": content})
            else:
                for block in content:
                    if block.get("type") == "tool_result":
                        text = block.get("content", "")
                        out.append(
                            {
                                "role": "tool",
                                "tool_call_id": block["tool_use_id"],
                                "content": ("ERROR: " + text) if block.get("is_error") else text,
                            }
                        )
                    elif block.get("type") == "text":
                        out.append({"role": "user", "content": block["text"]})
        return out

    @staticmethod
    def to_openai_tools(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": t["name"],
                    "description": t.get("description", ""),
                    "parameters": t["input_schema"],
                },
            }
            for t in tools
        ]

    def _post(self, kind: str, body: dict[str, Any]) -> dict[str, Any]:
        s = self.settings
        body = {**body, "max_completion_tokens": s.AI_MAX_TOKENS}
        if s.LLM_SEND_REASONING_EFFORT and s.AI_EFFORT in ("low", "medium", "high"):
            body["reasoning_effort"] = s.AI_EFFORT
        for attempt in range(s.AI_MAX_RETRIES + 1):
            try:
                r = self.http.post(self.url, content=json.dumps(body).encode())
            except httpx.HTTPError as e:
                AI_CALLS.labels(kind, "network").inc()
                if attempt >= s.AI_MAX_RETRIES:
                    raise AIUnavailable(f"Azure OpenAI unreachable: {e}") from e
                time.sleep(min(2**attempt, 30))
                continue
            if r.status_code in (429, 500, 502, 503, 504) and attempt < s.AI_MAX_RETRIES:
                AI_CALLS.labels(kind, f"http_{r.status_code}").inc()
                time.sleep(min(float(r.headers.get("retry-after") or 2**attempt), 60))
                continue
            if r.status_code != 200:
                AI_CALLS.labels(kind, f"http_{r.status_code}").inc()
                if r.status_code == 400 and "content_filter" in r.text:
                    return {
                        "choices": [
                            {
                                "finish_reason": "content_filter",
                                "message": {"role": "assistant", "content": ""},
                            }
                        ]
                    }
                raise AIUnavailable(f"Azure OpenAI error {r.status_code}: {r.text[:300]}")
            data: dict[str, Any] = r.json()
            AI_CALLS.labels(
                kind, str((data.get("choices") or [{}])[0].get("finish_reason") or "ok")
            ).inc()
            return data
        raise AIUnavailable("Azure OpenAI: retries exhausted")

    def create_message(
        self, *, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> LLMResponse:
        data = self._post(
            "agent",
            {
                "messages": self.to_openai(system, messages),
                "tools": self.to_openai_tools(tools),
                "tool_choice": "auto",
            },
        )
        choice = (data.get("choices") or [{}])[0]
        msg = choice.get("message") or {"role": "assistant", "content": ""}
        calls = []
        for tc in msg.get("tool_calls") or []:
            fn = tc.get("function", {})
            try:
                args = json.loads(fn.get("arguments") or "{}")
            except json.JSONDecodeError:
                args = {"_invalid_json": fn.get("arguments", "")}
            calls.append(
                ToolCall(tc["id"], fn.get("name", ""), args if isinstance(args, dict) else {})
            )
        assistant = {"role": "assistant", "content": msg.get("content") or ""}
        if msg.get("tool_calls"):
            assistant["tool_calls"] = msg["tool_calls"]
        stop = _FINISH.get(choice.get("finish_reason") or "stop", "end_turn")
        if calls:
            stop = "tool_use"
        return LLMResponse(
            stop_reason=stop,
            text=msg.get("content") or "",
            tool_calls=calls,
            assistant_content=assistant,
        )

    def complete(self, prompt: str) -> str:
        data = self._post("cell_fix", {"messages": [{"role": "user", "content": prompt}]})
        choice = (data.get("choices") or [{}])[0]
        if choice.get("finish_reason") == "content_filter":
            return ""
        return str((choice.get("message") or {}).get("content") or "")


def build_llm(settings: Settings) -> LLMClient:
    if settings.LLM_PROVIDER == "azure_openai":
        return AzureOpenAILLM(settings)
    return AnthropicLLM(settings)
