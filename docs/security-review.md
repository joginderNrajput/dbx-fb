# Security review

## Assets
Databricks tokens and OAuth secrets, Fabric/Entra ID secrets and tokens, the Anthropic API key,
customer notebook source and metadata (in `OUTPUT_DIR`), and the service's API keys.

## Threats and controls

| Threat | Control | Where |
|---|---|---|
| Credential leakage via storage | credentials are never persisted; job.json stores booleans; re-deploy requires credentials again | `orchestrator/service.py`, `jobs.py` |
| Credential leakage via logs or errors | `SecretStr` schemas, `repr=False` dataclasses, key-name redaction processor, `hide_input_in_errors` on settings | `schemas/credentials.py`, `core/logging.py`, `core/config.py` |
| Unauthenticated use | `X-API-Key` against Argon2id hashes; `AUTH_DISABLED` refused in production | `api/deps.py`, `core/config.py` |
| Credentials leaking from browse GETs into access logs or proxies | credentials only in `X-Databricks-*` / `X-Fabric-*` headers, never query parameters | `api/v1/browse.py` |
| SSRF through "Databricks host" | https only, private/loopback/link-local addresses refused (`ALLOW_PRIVATE_HOSTS` dev only) | `core/security.py` |
| Path traversal via upload / zip slip | normalised member names, `..`/absolute rejected, writes confined to `sources/` | `sources/offline.py` |
| Path traversal via assessment `source_path` / dbt folder | resolved paths must stay inside `sources/` | `sources/lookup.py`, `migration/engine.py` |
| Path traversal via job id / artifact path | job id regex before any path; artifact path containment check; store root guard | `core/ids.py`, `api/v1/migrations.py`, `storage/local.py` |
| Zip bombs / huge uploads | request body limit (413), extracted bytes and file count limits | `api/middleware.py`, `sources/offline.py` |
| Reading service env vars through `env://` refs | only with `ALLOW_ENV_CREDENTIALS` (refused in production) | `core/secrets.py` |
| Agent taking unsafe actions | tools re-check preconditions in code: deploy only when requested, Fabric credentials exist and preflight is clean; cannot record `migrated`; mapping keys validated; bounded turns and cells | `agents/tools.py` |
| Prompt injection from notebook content | the agent can only call migration tools; notebooks are data; no shell or network tools | `agents/tools.py` |
| Container escape impact | non-root uid 10001, no compiler or package manager layers added | `Dockerfile` |

## Residual risks
- Generated artifacts (copy notebooks, SQL scripts) run in the customer's Databricks/Fabric with
  their identity. Review them before running, as the runbook says.
- Agent-rewritten cells are marked "review required" and stay `needs_testing`, never `migrated`.
- `OUTPUT_DIR` holds customer source code: protect the volume (encryption at rest, access control).

`make security` (bandit) runs in CI. The remaining suppressions are documented inline: generated
SQL text, a regex constant, and a fixed-argv `az` call.
