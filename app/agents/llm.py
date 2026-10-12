"""The ONLY module that imports the Anthropic SDK (installed via the `ai` extra). Spec §6.1."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from app.core.config import Settings
from app.core.exceptions import AIUnavailable
from app.core.metrics import AI_CALLS

__all__ = ["AnthropicLLM", "LLMClient", "LLMResponse", "ToolCall", "build_llm"]


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


def build_llm(settings: Settings) -> LLMClient:
    return AnthropicLLM(settings)
