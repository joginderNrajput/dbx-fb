# ADR 0002 — Claude (Anthropic SDK) for the agent and AI cell fixing, as an optional extra

Status: accepted

Context: the reference tool already used the Anthropic API (raw urllib) to rewrite cells with
TODO markers. The service adds an agent that drives the migration (spec §6).

Decision: use the official `anthropic` SDK, installed through the `ai` extra and imported only
in `app/agents/llm.py` (lazily). The agent uses a manual tool-use loop, not the beta tool runner,
so every call is gated, traced and budgeted in our code. Defaults: model `claude-opus-5-5`,
effort `high`, server-side refusal fallback (both configurable; turn the fallback off behind
gateways that lack it).

Consequences: pipeline mode works with no AI dependency or key. Agent mode costs tokens per
migration, bounded by `AGENT_MAX_TURNS` and `AI_MAX_CELLS_PER_JOB`. The import-isolation test
keeps the SDK out of the API import graph.
