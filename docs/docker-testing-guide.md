# Docker testing guide

This guide runs the service in a container with Docker Compose. The image is the same one you
would deploy: three-stage build, non-root user (uid 10001), one uvicorn worker, healthcheck. For
the host flow without Docker see [`local-testing-guide.md`](local-testing-guide.md). Both read the
same `.env` and write to the same `./output` folder.

---

## 1. Prerequisites

| Need | Check |
|---|---|
| Docker Engine 24+ or Docker Desktop, with Compose v2.24+ | `docker version`, `docker compose version` |
| `make` (optional) | `make --version` |
| `curl` | `curl --version` |

No uv is needed on the host for the Docker flow. The shell helpers below use `python3` only to
read JSON (swap in `jq` if you prefer). The tests (section 7) run on the host with uv, or in a
throwaway container.

---

## 2. Configure

```bash
cp .env.example .env
```

The container reads `.env` through `env_file`. Without a `.env`, it still starts with safe
defaults, which means **auth enabled**, so `/api/v1` answers 401. For local testing keep
`AUTH_DISABLED=true` from the example.

| Setting | Notes for Docker |
|---|---|
| `OUTPUT_DIR` | ignored: compose pins it to `/app/output`, mounted from `./output` on the host |
| `ANTHROPIC_API_KEY` / `LLM_*` | set one provider to try agent mode (Claude needs the `ai` extra, included by default; Azure OpenAI needs nothing extra) |
| `API_PORT` | host port, default 9093 |
| `DOCKER_EXTRAS` | `ai` (default) or empty to build without the Anthropic SDK |

---

## 3. Build and start

```bash
make up            # = mkdir -p output && chmod 777 output && docker compose up -d --build
```

Without make:

```bash
mkdir -p output && chmod 777 output     # the container user (uid 10001) must be able to write here
docker compose up -d --build
```

Check it:

```bash
docker compose ps                        # STATUS should become "Up … (healthy)" within ~20s
curl -s localhost:9093/healthz           # {"status":"ok"}
curl -s localhost:9093/readyz            # storage ok; AI configured or not
make logs                                # = docker compose logs -f
```

Swagger UI: <http://localhost:9093/docs>.

The default compose service mounts `./app` read-only and runs uvicorn with `--reload`, so code
edits on the host restart the server inside the container. To run the immutable image exactly as
it would ship, use plain `docker run` (section 6).

---

## 4. Test scenarios

The API is the same as in the host flow, so every scenario in
[`local-testing-guide.md` §5–§10](local-testing-guide.md#5-scenario-a--offline-notebooks-no-databricks-credentials)
works unchanged against `http://localhost:9093`. The essentials:

```bash
API=http://localhost:9093/api/v1
wait_job() { while :; do S=$(curl -s $API/migrations/$1 | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])'); echo "$S"; [ "$S" = queued ] || [ "$S" = running ] || break; sleep 2; done; }
```

**Offline notebooks (no Databricks credentials):**

```bash
cd tests/fixtures/notebooks
curl -s -X POST $API/migrations/upload \
  -F "files=@etl/load_sales.py;filename=etl/load_sales.py" -F "files=@explore.ipynb" -F "files=@report.sql" \
  -F 'config={"mapping": {"catalog_to_lakehouse": {"main": "lh_sales"}}}'
cd -
ID=<id>; wait_job $ID
curl -s $API/migrations/$ID | python3 -m json.tool
ls output/jobs/$ID/out/artifacts/notebooks/        # files written by the container appear on the host
curl -s $API/migrations/$ID/download -o migration.zip
```

**Live Databricks:**

```bash
curl -s -X POST $API/migrations -H 'content-type: application/json' -d '{
  "source": {"type": "databricks", "workspace_paths": ["/Shared"],
             "credentials": {"host": "https://adb-1234567890123456.7.azuredatabricks.net", "token": "dapi..."}}}'
```

**Deploy to Fabric** (service principal):

```bash
curl -s -X POST $API/migrations/$ID/deploy -H 'content-type: application/json' -d '{
  "target": {"workspace_id": "<workspace-guid>",
             "credentials": {"tenant_id": "<tenant>", "client_id": "<app-id>", "client_secret": "<secret>"}},
  "only": ["notebook"]}'
```

**Migrate everything (code + data)** to a workspace and lakehouse by name. Details,
requirements and the domains that must be reachable are in local-testing-guide §8:

```bash
curl -s -X POST $API/migrations -H 'content-type: application/json' -d '{
  "source": {"type": "databricks", "catalogs": ["<catalog>"], "schemas": ["<schema>"],
             "credentials": {"host": "https://adb-...azuredatabricks.net", "token": "dapi..."}},
  "target": {"workspace_name": "Alteryx_Fabric", "lakehouse": "Src_LK",
             "credentials": {"tenant_id": "<tenant>", "client_id": "<app-id>", "client_secret": "<secret>"}},
  "options": {"deploy": true, "copy_data": true}}'
```

**Agent mode:** configure an LLM in `.env` (`ANTHROPIC_API_KEY`, or `LLM_PROVIDER=azure_openai` with
`LLM_BASE_URL`, `LLM_API_KEY`, `LLM_MODEL`), check it with
`curl -s -X POST $API/connections/llm/test`, recreate the container
(`docker compose up -d --force-recreate`), then:

```bash
curl -s -X POST $API/migrations/upload -F "files=@/tmp/notebooks.zip" -F 'config={"options": {"mode": "agent"}}'
curl -s $API/migrations/$ID/agent-trace | python3 -m json.tool
```

`.env` changes need `docker compose up -d --force-recreate`. A plain restart keeps the old
environment.

---

## 5. Persistence and restarts

- Job records, uploads and artifacts live in `./output` on the host and survive
  `docker compose down`.
- A container restart while a job runs marks that job `failed / interrupted` at the next start.
  Credentials are never stored, so resubmit it.
- `make down` stops the stack. Delete `./output` (or `DELETE /api/v1/migrations/{id}`) to clear
  data.

---

## 6. Production-like run (immutable image, auth on)

```bash
docker compose build                     # or: docker build -t dbx2fabric-app:local .
make hash-key                            # needs uv on the host; or:
docker run --rm dbx2fabric-app:local python -c "from app.core.security import *;k=generate_api_key();print(k);print(hash_api_key(k))"

docker run -d --name dbx2fabric -p 9093:9093 \
  -e APP_ENV=production -e AUTH_DISABLED=false \
  -e API_KEY_HASHES='<argon2 hash>' \
  -v "$PWD/output:/app/output" \
  dbx2fabric-app:local

curl -s localhost:9093/api/v1/migrations                                 # 401
curl -s -H "X-API-Key: <key>" localhost:9093/api/v1/migrations           # 200
docker inspect --format '{{.State.Health.Status}}' dbx2fabric           # healthy
docker rm -f dbx2fabric
```

With `APP_ENV=production` the container refuses to start if `AUTH_DISABLED`,
`ALLOW_ENV_CREDENTIALS` or `ALLOW_PRIVATE_HOSTS` is set, or if no API key
hash is configured. That refusal is the intended behaviour; check `docker logs`.

---

## 7. Run the test suite in Docker (no Python on the host)

```bash
docker run --rm -v "$PWD:/src" -w /src ghcr.io/astral-sh/uv:python3.12-bookworm-slim \
  sh -c "uv sync --extra dev --extra ai && uv run pytest -m 'not slow' -q"
```

CI (`.github/workflows/ci.yml`) runs lint, types, bandit and the tests. It then builds the image
and smoke-tests `/healthz` in a container.

---

## 8. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `PermissionError: … /app/output/jobs` | host `./output` not writable by uid 10001: `chmod 777 output` (what `make up` does) |
| `/api/v1/*` returns 401 | no `.env` (auth defaults to on), or `AUTH_DISABLED=false`: copy `.env.example` or send `X-API-Key` |
| Container `unhealthy` / restarting | `docker compose logs`; a settings validation error (e.g. dev flag with `APP_ENV=production`) stops startup |
| `.env` change has no effect | `docker compose up -d --force-recreate` |
| Build fails at `uv sync --frozen` | `uv.lock` is out of date with `pyproject.toml`: run `uv lock` on the host and commit it |
| Build fails with `CERTIFICATE_VERIFY_FAILED` behind a corporate TLS proxy | the build cannot reach PyPI through your proxy: build where PyPI is reachable, or add your proxy CA to the **builder** stage only (`COPY ca.crt` + `PIP_CERT` / `SSL_CERT_FILE`) |
| `Bind for 0.0.0.0:9093 failed` | port taken (perhaps `make dev`): stop it, or `API_PORT=9094 make up` |
| Agent mode returns 503 | no LLM configured in `.env`, or Claude selected with an image built with `DOCKER_EXTRAS=""` |
| Every Databricks / Fabric call fails with `403 Forbidden` | an egress proxy or firewall blocks the host from the container: allow `*.azuredatabricks.net`, `api.fabric.microsoft.com`, `onelake.dfs.fabric.microsoft.com`, `login.microsoftonline.com`, `*.blob.core.windows.net` (and your Azure OpenAI host) |
| Reload loops while jobs run | must not happen: `--reload-dir /app/app` watches code only; check you did not change the compose command |
