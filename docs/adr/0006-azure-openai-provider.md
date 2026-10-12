# ADR 0006 — Azure OpenAI as a second LLM provider, over httpx

Status: accepted

Context: the team runs its other accelerators on an Azure OpenAI deployment and wants the agent
and cell fixer to use it, while keeping Claude available (spec §6.1).

Decision: `LLM_PROVIDER=anthropic|azure_openai`. The Azure client calls
`{LLM_BASE_URL}/openai/deployments/{LLM_MODEL}/chat/completions?api-version={LLM_API_VERSION}`
with httpx (already a dependency), sending function tools, `tool_choice=auto` and
`max_completion_tokens`. The agent loop keeps one conversation format (Anthropic-style blocks);
the Azure client translates it: assistant turns carry the OpenAI message it returned, and
`tool_result` blocks become `role=tool` messages, all answered before the next call. 429/5xx are
retried with `retry-after`; content-filter blocks map to `refusal`. The `openai` SDK is explicitly
not added: one endpoint does not justify a dependency.

Consequences: switching providers is configuration only. Provider-specific features (Claude
effort/fallback, Azure reasoning_effort) are opt-in settings. A new provider adds one client
class in `app/agents/llm.py`.
