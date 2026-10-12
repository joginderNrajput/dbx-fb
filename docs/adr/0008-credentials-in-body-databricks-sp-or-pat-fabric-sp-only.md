# ADR 0008 — Credentials only in request bodies; Databricks SP or PAT, Fabric SP only

Status: accepted (supersedes the header-based browse credentials of the first browse API)

Context: the team wants one rule for credentials across every endpoint. It also wants Databricks
reachable with either a service principal or a personal access token, with no ambiguity about
which one is used, and Fabric limited to service principals for now.

Decision:
- **Bodies only.** Every credential is a field of the JSON request body (or of the `config` JSON
  of a multipart upload). The browse endpoints are therefore `POST` with filters in the body;
  path segments only name resources. GET-with-body was not chosen, because clients and proxies drop
  it and OpenAPI cannot describe it. Headers were not chosen, because they would be a second
  convention.
- **Databricks: exactly one method.** If `client_id` + `client_secret` are present, the service
  principal (OAuth M2M on `{host}/oidc/v1/token`) is used and the token is dropped before the
  client is built. Otherwise `personal_access_token` (alias `token`) is used. Half a service
  principal is a 422 instead of a silent fallback. The method used is reported (`auth_type` on the
  connection check, `options.databricks_auth` on jobs) without the secret.
- **Fabric: service principal only.** `tenant_id`, `client_id` and `client_secret` are all
  required; the bearer-token and `az login` options, and the `FABRIC_TOKEN` /
  `FABRIC_ALLOW_AZ_CLI` settings, are removed. One principal mints both the Fabric API and the
  OneLake tokens, so data copy always works with the credentials deploy uses.

Consequences: browse calls are POST, not GET. Callers holding only a Fabric bearer token must
create a service principal. Revisit when user-delegated Fabric access is needed.
