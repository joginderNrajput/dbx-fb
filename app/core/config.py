"""Typed settings: the only place configuration is read. Spec §9."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import PositiveFloat, PositiveInt, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = ["Settings", "get_settings"]


class Settings(BaseSettings):
    # hide_input_in_errors: a startup validation error must not echo env values (API keys) into logs.
    model_config = SettingsConfigDict(
        env_file=".env", extra="ignore", case_sensitive=True, hide_input_in_errors=True
    )

    # ── service ────────────────────────────────────────────────
    APP_ENV: Literal["development", "staging", "production"] = "development"
    APP_VERSION: str = "1.0.0"
    LOG_LEVEL: str = "INFO"
    # None = pretty console logs in development, JSON everywhere else.
    LOG_JSON: bool | None = None

    # ── api / auth (spec §8.1) ─────────────────────────────────
    # Dev-only: accept requests without an X-API-Key header. Refused in production.
    AUTH_DISABLED: bool = False
    # Whitespace-separated Argon2id hashes (`dbx2fabric hash-key` prints one). Hashes contain `$`:
    # wrap the value in SINGLE quotes in .env so neither docker compose nor dotenv expands it.
    API_KEY_HASHES: str = ""
    # Upload limits: a request body above MAX_UPLOAD_MB is a 413 before it is read.
    MAX_UPLOAD_MB: PositiveInt = 200
    # Zip bombs: total uncompressed bytes and member count allowed per migration.
    MAX_EXTRACTED_MB: PositiveInt = 1000
    MAX_UPLOAD_FILES: PositiveInt = 5000
    CORS_ORIGINS: str = ""

    # ── storage (spec §7) ──────────────────────────────────────
    # Every migration gets jobs/<id>/ under this folder: job.json, sources/, out/ (ledger, artifacts).
    OUTPUT_DIR: Path = Path("output")
    OBJECT_STORE_BACKEND: Literal["local"] = "local"

    # ── orchestration / budgets (spec §4.3) ────────────────────
    MAX_CONCURRENT_JOBS: PositiveInt = 2
    # Wall-clock budget per migration job; the job is marked failed and stops at the next stage.
    JOB_TIMEOUT_S: PositiveInt = 3600

    # ── databricks (spec §5.1) ─────────────────────────────────
    DATABRICKS_TIMEOUT_S: PositiveInt = 90
    DATABRICKS_MAX_RETRIES: int = 3
    # Data copy (options.copy_data, spec §5.9): tables above DATA_COPY_MAX_ROWS are skipped (use
    # export_to_staging). EXTERNAL_LINKS downloads chunks from presigned cloud-storage URLs
    # (*.blob.core.windows.net / *.dfs.core.windows.net must be reachable); INLINE stays on the
    # workspace host but Databricks caps an inline result at 25 MiB.
    DATA_COPY_MAX_ROWS: PositiveInt = 10_000_000
    DATA_COPY_DISPOSITION: Literal["EXTERNAL_LINKS", "INLINE"] = "EXTERNAL_LINKS"
    DATA_COPY_STATEMENT_TIMEOUT_S: PositiveInt = 1800
    DATA_COPY_POLL_S: PositiveFloat = 2.0
    DATA_COPY_FOLDER: str = "dbx_migration"
    # Upper bound on notebooks discovered per migration (protects the job budget).
    DATABRICKS_MAX_NOTEBOOKS: PositiveInt = 5000

    # ── fabric (spec §5.4) ─────────────────────────────────────
    FABRIC_API_BASE: str = "https://api.fabric.microsoft.com/v1"
    FABRIC_AUTHORITY: str = "https://login.microsoftonline.com"
    FABRIC_SCOPE: str = "https://api.fabric.microsoft.com/.default"
    FABRIC_TIMEOUT_S: PositiveInt = 120
    FABRIC_LRO_POLL_S: PositiveFloat = 2.0
    FABRIC_LRO_MAX_POLLS: PositiveInt = 150
    FABRIC_MAX_THROTTLE_RETRIES: PositiveInt = 4
    # OneLake (ADLS Gen2 DFS API) for options.copy_data; tokens use the Azure Storage audience.
    ONELAKE_DFS_BASE: str = "https://onelake.dfs.fabric.microsoft.com"
    ONELAKE_SCOPE: str = "https://storage.azure.com/.default"
    # Fabric job polling (loader / view notebooks run on demand).
    FABRIC_JOB_POLL_S: PositiveFloat = 10.0
    FABRIC_JOB_TIMEOUT_S: PositiveInt = 3600

    # ── dev credential fallbacks (refused in production, spec §8.2) ─
    # Lets a request omit credentials (or use env://NAME references) and fall back to the values
    # below. Production callers must send credentials per request; they are never persisted.
    ALLOW_ENV_CREDENTIALS: bool = False
    DATABRICKS_HOST: str = ""
    # Databricks: CLIENT_ID + CLIENT_SECRET (service principal) win over DATABRICKS_TOKEN when both are set.
    DATABRICKS_TOKEN: SecretStr | None = None
    DATABRICKS_CLIENT_ID: str = ""
    DATABRICKS_CLIENT_SECRET: SecretStr | None = None
    # Fabric: service principal only.
    FABRIC_TENANT_ID: str = ""
    FABRIC_CLIENT_ID: str = ""
    FABRIC_CLIENT_SECRET: SecretStr | None = None
    FABRIC_WORKSPACE_ID: str = ""
    FABRIC_WORKSPACE_NAME: str = ""
    FABRIC_LAKEHOUSE: str = ""
    # Dev-only: allow http:// and private/loopback Databricks hosts (local mocks). Without it the
    # host must be https and public, which stops the API being used to probe the internal network.
    ALLOW_PRIVATE_HOSTS: bool = False

    # ── ai / agent (spec §6) ───────────────────────────────────
    # Needed for mode=agent and options.ai_fix. "anthropic" uses the Claude API (`ai` extra);
    # "azure_openai" uses an Azure OpenAI chat-completions deployment over httpx (no extra).
    LLM_PROVIDER: Literal["anthropic", "azure_openai"] = "anthropic"
    ANTHROPIC_API_KEY: SecretStr | None = None
    AI_MODEL: str = "claude-opus-5-5"
    # Effort for every Claude call; empty string omits the parameter (for models without it).
    AI_EFFORT: Literal["", "low", "medium", "high", "xhigh", "max"] = "high"
    AI_MAX_TOKENS: PositiveInt = 16000
    AI_TIMEOUT_S: PositiveInt = 300
    AI_MAX_RETRIES: int = 2
    # Server-side refusal fallback (Claude API only). Turn off for Bedrock/Vertex/Foundry gateways.
    AI_REFUSAL_FALLBACK: bool = True
    # Azure OpenAI (LLM_PROVIDER=azure_openai): resource endpoint, key, DEPLOYMENT name (Azure
    # addresses a deployment, not a model family) and API version. AI_MAX_TOKENS is sent as
    # max_completion_tokens; AI_EFFORT is sent as reasoning_effort when set (Beyond §6).
    LLM_BASE_URL: str = ""
    LLM_API_KEY: SecretStr | None = None
    LLM_MODEL: str = ""
    LLM_API_VERSION: str = "2025-01-01-preview"
    # Send AI_EFFORT as reasoning_effort to Azure OpenAI (reasoning deployments accept low|medium|high).
    LLM_SEND_REASONING_EFFORT: bool = False
    # Budgets: agent turns per job, and cells the AI fixer may rewrite per job.
    AGENT_MAX_TURNS: PositiveInt = 40
    AI_MAX_CELLS_PER_JOB: PositiveInt = 200

    # ── validators ─────────────────────────────────────────────
    @model_validator(mode="after")
    def _dev_flags_refused_in_production(self) -> Settings:
        if self.APP_ENV != "production":
            return self
        for flag in (
            "AUTH_DISABLED",
            "ALLOW_ENV_CREDENTIALS",
            "ALLOW_PRIVATE_HOSTS",
        ):
            if getattr(self, flag):
                raise ValueError(f"{flag} is development-only and refused when APP_ENV=production")
        if not self.api_key_hashes:
            raise ValueError("API_KEY_HASHES must hold at least one hash when APP_ENV=production")
        return self

    @property
    def api_key_hashes(self) -> list[str]:
        return self.API_KEY_HASHES.split()

    @property
    def cors_origins(self) -> list[str]:
        return [o.strip() for o in self.CORS_ORIGINS.split(",") if o.strip()]

    @property
    def ai_configured(self) -> bool:
        if self.LLM_PROVIDER == "azure_openai":
            key = self.LLM_API_KEY.get_secret_value() if self.LLM_API_KEY else ""
            return bool(key and self.LLM_BASE_URL and self.LLM_MODEL)
        return self.ANTHROPIC_API_KEY is not None and bool(
            self.ANTHROPIC_API_KEY.get_secret_value()
        )

    @property
    def ai_model(self) -> str:
        return self.LLM_MODEL if self.LLM_PROVIDER == "azure_openai" else self.AI_MODEL


@lru_cache
def get_settings() -> Settings:
    """Process-wide settings singleton. Clear with ``get_settings.cache_clear()``."""
    return Settings()
