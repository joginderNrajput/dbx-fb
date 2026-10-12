# syntax=docker/dockerfile:1
# Three stages: base (runtime libs) -> builder (venv) -> runtime (non-root). Spec §9.3.
FROM python:3.12-slim AS base
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1
# No apt layer: python:3.12-slim already ships ca-certificates (every outbound call - Databricks,
# Fabric, Entra ID, Anthropic - is TLS) and the healthcheck uses python, not curl.

FROM base AS builder
# Every locked dependency ships a manylinux wheel, so no compiler is installed. If a new dependency
# fails with "error: command 'gcc' failed", add build-essential HERE (never in base/runtime).
# Build the venv AT its final path: a virtualenv is not relocatable (console-script shebangs are
# absolute), so copying it elsewhere breaks `uvicorn` with "not found" errors that look like a
# missing package.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
# Extras baked into the image. "ai" enables agent mode / ai_fix; build with EXTRAS="" to omit it.
ARG EXTRAS="ai"
COPY pyproject.toml uv.lock /src/
WORKDIR /src
RUN pip install uv && uv sync --frozen --no-dev --no-install-project \
      $(for extra in $EXTRAS; do echo "--extra $extra"; done)

FROM base AS runtime
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" PYTHONPATH=/app OUTPUT_DIR=/app/output
WORKDIR /app
COPY app/ ./app/
RUN mkdir -p /app/output && useradd -m -u 10001 dbx2fabric && chown -R dbx2fabric:dbx2fabric /app
USER dbx2fabric
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://localhost:8000/healthz')"
# ONE worker: jobs run in-process and cancellation/concurrency limits are per process (ADR 0005).
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
