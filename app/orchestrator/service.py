"""MigrationService: turns API requests into sessions and runs them as jobs. Spec §4.1, §5.

Request credentials are converted to in-memory credential objects here and handed to the job
closure; the persisted JobRecord only notes that they were supplied (spec §8.2).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from app.agents.cell_fixer import CellFixer
from app.agents.llm import LLMClient, build_llm
from app.agents.runner import run_agent
from app.core.config import Settings
from app.core.exceptions import AIUnavailable, AppError, Conflict, ValidationFailed
from app.core.ids import new_job_id
from app.core.logging import bind_job, get_logger
from app.core.metrics import ITEM_OUTCOMES, JOBS_STARTED
from app.core.secrets import resolve_secret
from app.core.security import ensure_safe_url
from app.databricks.auth import DatabricksCredentials
from app.databricks.discovery import DiscoveryOptions
from app.fabric.auth import FabricCredentials
from app.migration.mapping import validate_overrides
from app.migration.session import MigrationSession, SessionSpec
from app.orchestrator.jobs import ACTIVE, JobRecord, JobRepository
from app.orchestrator.runner import JobControl, JobRunner
from app.schemas.credentials import DatabricksCredentialsIn, FabricCredentialsIn
from app.schemas.migrations import DeployIn, MigrationCreate, MigrationOptionsIn, UploadConfig
from app.sources.offline import IngestLimits, ingest_uploads
from app.storage import keys
from app.storage.base import ObjectStore

__all__ = ["MigrationService", "build_databricks_credentials", "build_fabric_credentials"]

log = get_logger(__name__)


def build_databricks_credentials(
    c: DatabricksCredentialsIn | None, settings: Settings, *, field: str
) -> DatabricksCredentials | None:
    """Request credentials, else (dev only) the DATABRICKS_* settings; None when nothing is given."""
    c = c or DatabricksCredentialsIn()
    host = c.host
    token = resolve_secret(c.token, settings, field=f"{field}.token")
    secret = resolve_secret(c.client_secret, settings, field=f"{field}.client_secret")
    if not host and not token and not c.client_id and settings.ALLOW_ENV_CREDENTIALS:
        host = settings.DATABRICKS_HOST
        token = resolve_secret(settings.DATABRICKS_TOKEN, settings, field=f"{field}.token")
    if not host and not token and not c.client_id:
        return None
    host = ensure_safe_url(host, field=f"{field}.host", allow_private=settings.ALLOW_PRIVATE_HOSTS)
    try:
        return DatabricksCredentials(
            host=host, token=token, client_id=c.client_id, client_secret=secret
        )
    except ValidationFailed as e:
        raise ValidationFailed(e.message, field=f"{field}.token") from e


def build_fabric_credentials(
    c: FabricCredentialsIn | None, settings: Settings, *, field: str
) -> FabricCredentials | None:
    c = c or FabricCredentialsIn()
    token = resolve_secret(c.access_token, settings, field=f"{field}.access_token")
    secret = resolve_secret(c.client_secret, settings, field=f"{field}.client_secret")
    tenant, client_id, use_cli = c.tenant_id, c.client_id, c.use_az_cli
    if not (token or tenant or client_id or use_cli) and settings.ALLOW_ENV_CREDENTIALS:
        token = resolve_secret(settings.FABRIC_TOKEN, settings, field=f"{field}.access_token")
        tenant, client_id = settings.FABRIC_TENANT_ID or None, settings.FABRIC_CLIENT_ID or None
        secret = resolve_secret(
            settings.FABRIC_CLIENT_SECRET, settings, field=f"{field}.client_secret"
        )
        use_cli = not (token or (tenant and client_id and secret)) and settings.FABRIC_ALLOW_AZ_CLI
    if not (token or tenant or client_id or use_cli):
        return None
    if use_cli and not settings.FABRIC_ALLOW_AZ_CLI:
        raise ValidationFailed(
            "az CLI tokens are disabled on this server (FABRIC_ALLOW_AZ_CLI)",
            field=f"{field}.use_az_cli",
        )
    try:
        return FabricCredentials(
            access_token=token,
            tenant_id=tenant,
            client_id=client_id,
            client_secret=secret,
            use_az_cli=use_cli,
        )
    except ValidationFailed as e:
        raise ValidationFailed(e.message, field=field) from e


class MigrationService:
    def __init__(
        self,
        settings: Settings,
        store: ObjectStore,
        repo: JobRepository,
        runner: JobRunner,
        llm_factory: Callable[[Settings], LLMClient] = build_llm,
    ) -> None:
        self.settings = settings
        self.store = store
        self.repo = repo
        self.runner = runner
        self.llm_factory = llm_factory

    # ── creation ──
    def _check_ai(self, options: MigrationOptionsIn) -> LLMClient | None:
        if options.mode != "agent" and not options.ai_fix:
            return None
        if not self.settings.ai_configured:
            raise AIUnavailable("mode=agent and ai_fix need ANTHROPIC_API_KEY on the server")
        return self.llm_factory(
            self.settings
        )  # fail now (missing extra) rather than inside the job

    def _target(
        self, target: Any, options: MigrationOptionsIn, mapping: dict[str, Any]
    ) -> tuple[FabricCredentials | None, str | None]:
        fabric = build_fabric_credentials(
            target.credentials, self.settings, field="target.credentials"
        )
        ws = (
            target.workspace_id
            or mapping.get("workspace_id")
            or (self.settings.FABRIC_WORKSPACE_ID if self.settings.ALLOW_ENV_CREDENTIALS else "")
            or None
        )
        if options.deploy:
            if not fabric:
                raise ValidationFailed(
                    "options.deploy needs Fabric credentials", field="target.credentials"
                )
            if not ws:
                raise ValidationFailed(
                    "options.deploy needs target.workspace_id", field="target.workspace_id"
                )
        return fabric, ws

    def _options_record(
        self,
        options: MigrationOptionsIn,
        ws: str | None,
        fabric: FabricCredentials | None,
        dbx: DatabricksCredentials | None,
    ) -> dict[str, Any]:
        return {
            "deploy": options.deploy,
            "deploy_only": list(options.deploy_only),
            "force_deploy": options.force_deploy,
            "ai_fix": options.ai_fix,
            "workspace_id": ws,
            "fabric_credentials": bool(fabric),
            "databricks_credentials": bool(dbx),
            "databricks_host": dbx.host if dbx else None,
        }

    def create_from_databricks(self, req: MigrationCreate) -> JobRecord:
        validate_overrides(req.mapping)
        llm = self._check_ai(req.options)
        dbx = build_databricks_credentials(
            req.source.credentials, self.settings, field="source.credentials"
        )
        if dbx is None:
            raise ValidationFailed(
                "Databricks host and token (or client_id/client_secret) are required",
                field="source.credentials",
            )
        if req.assessment is not None and not isinstance(req.assessment.get("assets"), list):
            raise ValidationFailed("assessment must have an 'assets' list", field="assessment")
        fabric, ws = self._target(req.target, req.options, req.mapping)
        job_id = new_job_id()
        s = req.source
        spec = SessionSpec(
            source_kind="databricks",
            sources_dir=self.store.path(keys.job_sources(job_id)),
            out_dir=self.store.path(keys.job_out(job_id)),
            databricks=dbx,
            assessment=req.assessment,
            discovery=DiscoveryOptions(
                workspace_paths=s.workspace_paths or ["/"],
                include_jobs=s.include_jobs,
                include_tables=s.include_tables,
                catalogs=s.catalogs,
                include_secret_scopes=s.include_secret_scopes,
                include_clusters=s.include_clusters,
                max_notebooks=min(
                    s.max_notebooks or self.settings.DATABRICKS_MAX_NOTEBOOKS,
                    self.settings.DATABRICKS_MAX_NOTEBOOKS,
                ),
            ),
            mapping_overrides=req.mapping,
            fabric=fabric,
            workspace_id=ws,
            deploy_requested=req.options.deploy,
            deploy_only=list(req.options.deploy_only),
            force_deploy=req.options.force_deploy,
            ai_fix=req.options.ai_fix,
        )
        rec = self.repo.create(
            JobRecord(
                id=job_id,
                mode=req.options.mode,
                source_kind="databricks",
                options=self._options_record(req.options, ws, fabric, dbx),
            )
        )
        self._start(rec, spec, req.options, llm)
        return rec

    def create_from_upload(self, files: list[tuple[str, bytes]], cfg: UploadConfig) -> JobRecord:
        validate_overrides(cfg.mapping)
        llm = self._check_ai(cfg.options)
        dbx = build_databricks_credentials(cfg.databricks, self.settings, field="databricks")
        job_id = new_job_id()
        sources = self.store.path(keys.job_sources(job_id))
        try:
            res = ingest_uploads(
                files,
                sources,
                IngestLimits(
                    self.settings.MAX_UPLOAD_FILES, self.settings.MAX_EXTRACTED_MB * 1024 * 1024
                ),
            )
            if not res.notebooks and res.assessment is None:
                raise ValidationFailed(
                    "no notebooks found in the upload (accepted: .ipynb .py .sql .scala .r or a .zip of them, plus optional assessment.json / mapping.json)",
                    field="files",
                )
            if res.mapping:
                validate_overrides(res.mapping, field="files.mapping.json")
            mapping = {**(res.mapping or {}), **cfg.mapping}
            fabric, ws = self._target(cfg.target, cfg.options, mapping)
        except AppError:
            self.store.delete_prefix(keys.job_prefix(job_id))
            raise
        spec = SessionSpec(
            source_kind="upload",
            sources_dir=sources,
            out_dir=self.store.path(keys.job_out(job_id)),
            databricks=dbx,
            uploaded_notebooks=res.notebooks,
            assessment=res.assessment,
            uploaded_mapping=res.mapping or {},
            mapping_overrides=cfg.mapping,
            fabric=fabric,
            workspace_id=ws,
            deploy_requested=cfg.options.deploy,
            deploy_only=list(cfg.options.deploy_only),
            force_deploy=cfg.options.force_deploy,
            ai_fix=cfg.options.ai_fix,
        )
        rec = self.repo.create(
            JobRecord(
                id=job_id,
                mode=cfg.options.mode,
                source_kind="upload",
                options=self._options_record(cfg.options, ws, fabric, dbx),
                upload={
                    "notebooks": len(res.notebooks),
                    "skipped": res.skipped[:200],
                    "assessment_supplied": res.assessment is not None,
                    "mapping_supplied": res.mapping is not None,
                    "bytes": res.total_bytes,
                },
            )
        )
        self._start(rec, spec, cfg.options, llm)
        return rec

    def redeploy(self, job_id: str, req: DeployIn) -> JobRecord:
        """Deploy the artifacts of a finished migration (credentials arrive with this request)."""
        rec = self.repo.get(job_id)
        if rec.status in ACTIVE or self.runner.is_active(job_id):
            raise Conflict("the migration is still running")
        out = self.store.path(keys.job_out(job_id))
        if not (out / "ledger.json").exists() or not rec.summary:
            raise Conflict("the migration has no converted artifacts to deploy")
        fabric = build_fabric_credentials(
            req.target.credentials, self.settings, field="target.credentials"
        )
        if not fabric:
            raise ValidationFailed("Fabric credentials are required", field="target.credentials")
        ws = req.target.workspace_id or rec.options.get("workspace_id")
        if not ws:
            raise ValidationFailed("target.workspace_id is required", field="target.workspace_id")
        spec = SessionSpec(
            source_kind=rec.source_kind,
            sources_dir=self.store.path(keys.job_sources(job_id)),
            out_dir=out,
            fabric=fabric,
            workspace_id=ws,
            deploy_requested=True,
            deploy_only=list(req.only),
            force_deploy=req.force,
        )
        rec = (
            self.repo.update(
                job_id,
                status="queued",
                options={
                    **rec.options,
                    "workspace_id": ws,
                    "deploy": True,
                    "fabric_credentials": True,
                },
            )
            or rec
        )

        def work(control: JobControl) -> None:
            bind_job(job_id)
            session = MigrationSession(spec, self.settings, checkpoint=control.checkpoint)
            session.done = ["inventory", "plan", "convert"]
            issues = session.run_preflight()
            self.repo.update(job_id, only_if_active=True, preflight=_counts(issues))
            summary = session.deploy()
            self.repo.update(job_id, only_if_active=True, summary=summary)

        self.runner.submit(job_id, work)
        return rec

    def cancel(self, job_id: str) -> JobRecord:
        rec = self.repo.get(job_id)
        if rec.status not in ACTIVE:
            raise Conflict(f"migration is already {rec.status}")
        if not self.runner.cancel(job_id):
            raise Conflict("the migration is not running in this process")
        return rec

    def delete(self, job_id: str) -> None:
        rec = self.repo.get(job_id)
        if rec.status in ACTIVE:
            raise Conflict("cancel the migration before deleting it")
        self.repo.delete(job_id)

    def ensure_idle(self, job_id: str) -> JobRecord:
        rec = self.repo.get(job_id)
        if rec.status in ACTIVE:
            raise Conflict("the migration is still running")
        return rec

    # ── execution ──
    def _start(
        self, rec: JobRecord, spec: SessionSpec, options: MigrationOptionsIn, llm: LLMClient | None
    ) -> None:
        JOBS_STARTED.labels(options.mode, rec.source_kind).inc()

        def work(control: JobControl) -> None:
            bind_job(rec.id)
            fixer = (
                CellFixer(llm, self.settings.AI_MAX_CELLS_PER_JOB)
                if (llm and options.ai_fix)
                else None
            )
            session = MigrationSession(
                spec, self.settings, cell_fixer=fixer, checkpoint=control.checkpoint
            )
            try:
                if options.mode == "agent" and llm is not None:
                    self._run_agent(rec.id, session, llm, options)
                else:
                    self._run_pipeline(rec.id, session)
            finally:
                session.close()
                for outcome, n in session.outcome_counts().items():
                    ITEM_OUTCOMES.labels(outcome).inc(n)

        self.runner.submit(rec.id, work)

    def _run_pipeline(self, job_id: str, s: MigrationSession) -> None:
        inv = s.inventory()
        self.repo.update(job_id, only_if_active=True, inventory=inv)
        s.plan()
        summary = s.convert()
        self.repo.update(job_id, only_if_active=True, summary=summary)
        issues = s.run_preflight()
        self.repo.update(job_id, only_if_active=True, preflight=_counts(issues))
        if s.spec.deploy_requested:
            summary = s.deploy()
        summary = s.report()
        self.repo.update(job_id, only_if_active=True, summary=summary)

    def _run_agent(
        self, job_id: str, s: MigrationSession, llm: LLMClient, options: MigrationOptionsIn
    ) -> None:
        trace_key = keys.agent_trace(job_id)
        agent: dict[str, Any] = {"model": self.settings.AI_MODEL}
        try:
            res = run_agent(
                s,
                llm,
                max_turns=self.settings.AGENT_MAX_TURNS,
                instructions=options.agent_instructions,
                on_step=lambda trace: self.store.write_json(trace_key, trace),
            )
            agent.update(stopped=res.stopped, turns=res.turns, summary=res.summary)
        except AIUnavailable as e:
            agent.update(stopped="error", error=e.message)
            log.warning("agent_failed", error=e.message)
        # Safety net: whatever the agent did, the migration ends with converted artifacts and a report.
        if "convert" not in s.done:
            agent["safety_net"] = (
                "the agent stopped before converting; the deterministic pipeline completed inventory/plan/convert"
            )
            if "inventory" not in s.done:
                s.inventory()
            if "plan" not in s.done:
                s.plan()
            s.convert()
        self.repo.update(job_id, only_if_active=True, inventory=_inventory(s))
        issues = s.run_preflight()
        agent["deployed"] = s.deployed
        if s.spec.deploy_requested and not s.deployed:
            agent["deploy_note"] = (
                "deploy was requested but the agent did not deploy (see summary); use POST /deploy after review"
            )
        summary = s.report()
        self.repo.update(
            job_id, only_if_active=True, summary=summary, preflight=_counts(issues), agent=agent
        )


def _counts(issues: list[dict[str, str]]) -> dict[str, int]:
    return {
        "errors": sum(1 for i in issues if i["level"] == "error"),
        "warnings": sum(1 for i in issues if i["level"] == "warn"),
    }


def _inventory(s: MigrationSession) -> dict[str, Any] | None:
    try:
        assess = s.assessment()
    except ValidationFailed:
        return None
    by_type: dict[str, int] = {}
    for a in assess["assets"]:
        by_type[a["type"]] = by_type.get(a["type"], 0) + 1
    return {
        "total_assets": len(assess["assets"]),
        "by_type": by_type,
        "warnings": assess.get("warnings", []),
    }
