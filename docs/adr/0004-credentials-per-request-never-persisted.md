# ADR 0004 — Credentials arrive per request and are never persisted

Status: accepted

Context: the service needs Databricks and Fabric credentials, which are high-value secrets of the
customer. The blueprint requires that secrets are never committed, logged or persisted (S9).

Decision: credentials travel in the request body (`SecretStr`). They become frozen in-memory
dataclasses with `repr=False` that live only in the job closure. `job.json` records only whether
they were supplied. A later deploy (`POST /{id}/deploy`) takes credentials again. Settings
fallbacks and `env://NAME` references exist for development only (`ALLOW_ENV_CREDENTIALS`,
refused in production).

Consequences: a restart cannot resume a job, so it is failed as `interrupted`. Clients resend
credentials for re-deploys. In exchange, nothing on disk can leak them.
