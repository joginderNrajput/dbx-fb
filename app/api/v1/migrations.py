"""Migration resource: create (live or offline), follow, inspect, deploy, validate. Spec §5.2-§5.5."""

from __future__ import annotations

import json
import zipfile
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends, File, Form, Query, UploadFile, status
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse, Response
from pydantic import ValidationError
from starlette.concurrency import run_in_threadpool

from app.api.deps import get_repo, get_service, get_settings_dep, get_store
from app.core.config import Settings
from app.core.exceptions import NotFound, PayloadTooLarge, ValidationFailed
from app.core.ids import require_job_id
from app.migration.ledger import load_ledger, record_result, summarize_ledger
from app.migration.mapping import load_cfg
from app.migration.preflight import preflight
from app.migration.report import write_report
from app.migration.validation import import_validation
from app.orchestrator.jobs import JobRecord, JobRepository
from app.orchestrator.service import MigrationService
from app.schemas.migrations import (
    DeployIn,
    ItemResultIn,
    MigrationAccepted,
    MigrationCreate,
    MigrationOut,
    UploadConfig,
)
from app.storage import keys
from app.storage.base import ObjectStore

router = APIRouter(prefix="/migrations", tags=["migrations"])

Service = Annotated[MigrationService, Depends(get_service)]
Repo = Annotated[JobRepository, Depends(get_repo)]
Store = Annotated[ObjectStore, Depends(get_store)]


def _links(job_id: str) -> dict[str, str]:
    base = f"/api/v1/migrations/{job_id}"
    return {
        "self": base,
        "items": f"{base}/items",
        "report": f"{base}/report",
        "report_csv": f"{base}/report.csv",
        "runbook": f"{base}/runbook",
        "preflight": f"{base}/preflight",
        "artifacts": f"{base}/artifacts",
        "download": f"{base}/download",
        "agent_trace": f"{base}/agent-trace",
        "deploy": f"{base}/deploy",
    }


def _out(rec: JobRecord) -> MigrationOut:
    return MigrationOut(**rec.model_dump(), links=_links(rec.id))


def _accepted(rec: JobRecord) -> MigrationAccepted:
    return MigrationAccepted(id=rec.id, status="queued", links=_links(rec.id))


def _out_dir(store: ObjectStore, job_id: str) -> Path:
    return store.path(keys.job_out(require_job_id(job_id)))


@router.post("", status_code=status.HTTP_202_ACCEPTED, response_model=MigrationAccepted)
def create_migration(body: MigrationCreate, service: Service) -> MigrationAccepted:
    """Migrate a live Databricks workspace (discovery + export through the Databricks REST API)."""
    return _accepted(service.create_from_databricks(body))


@router.post("/upload", status_code=status.HTTP_202_ACCEPTED, response_model=MigrationAccepted)
async def create_upload_migration(
    service: Service,
    settings: Annotated[Settings, Depends(get_settings_dep)],
    files: Annotated[
        list[UploadFile],
        File(
            description="Notebooks (.ipynb .py .sql .scala .r), .zip archives, assessment.json, mapping.json"
        ),
    ],
    config: Annotated[str, Form(description="JSON: {target, mapping, options, databricks}")] = "{}",
) -> MigrationAccepted:
    """Migrate offline notebooks: no Databricks credentials needed."""
    try:
        cfg = UploadConfig.model_validate(json.loads(config or "{}"))
    except json.JSONDecodeError as e:
        raise ValidationFailed(f"config is not valid JSON: {e}", field="config") from e
    except ValidationError as e:
        err = e.errors()[0]
        raise ValidationFailed(
            f"config: {err['msg']}", field="config." + ".".join(str(x) for x in err["loc"])
        ) from e
    limit = settings.MAX_UPLOAD_MB * 1024 * 1024
    data: list[tuple[str, bytes]] = []
    total = 0
    for f in files:
        chunk = await f.read()
        total += len(chunk)
        if total > limit:
            raise PayloadTooLarge(f"uploads exceed MAX_UPLOAD_MB={settings.MAX_UPLOAD_MB}")
        data.append((f.filename or "", chunk))
    rec = await run_in_threadpool(service.create_from_upload, data, cfg)
    return _accepted(rec)


@router.get("", response_model=list[MigrationOut])
def list_migrations(
    repo: Repo, limit: Annotated[int, Query(ge=1, le=500)] = 50
) -> list[MigrationOut]:
    return [_out(r) for r in repo.list(limit)]


@router.get("/{job_id}", response_model=MigrationOut)
def get_migration(job_id: str, repo: Repo) -> MigrationOut:
    return _out(repo.get(require_job_id(job_id)))


@router.post("/{job_id}/cancel", response_model=MigrationOut)
def cancel_migration(job_id: str, service: Service) -> MigrationOut:
    return _out(service.cancel(require_job_id(job_id)))


@router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_migration(job_id: str, service: Service) -> Response:
    service.delete(require_job_id(job_id))
    return Response(status_code=204)


@router.get("/{job_id}/items")
def list_items(
    job_id: str, repo: Repo, store: Store, outcome: str | None = None, type: str | None = None
) -> dict[str, Any]:
    repo.get(require_job_id(job_id))
    led = load_ledger(_out_dir(store, job_id))
    items = [
        i
        for i in led["items"]
        if (not outcome or i["outcome"] == outcome) and (not type or i["type"] == type)
    ]
    return {"summary": summarize_ledger(led), "items": items}


@router.get("/{job_id}/preflight")
def get_preflight(job_id: str, service: Service, store: Store) -> dict[str, Any]:
    service.ensure_idle(require_job_id(job_id))
    out = _out_dir(store, job_id)
    issues = [
        {"level": lvl, "where": w, "message": m}
        for lvl, w, m in preflight(out, load_cfg(out / "mapping.json"))
    ]
    return {
        "errors": sum(1 for i in issues if i["level"] == "error"),
        "warnings": sum(1 for i in issues if i["level"] == "warn"),
        "issues": issues,
    }


def _file(store: ObjectStore, job_id: str, name: str) -> Path:
    p = _out_dir(store, job_id) / name
    if not p.is_file():
        raise NotFound(f"{name} is not available yet")
    return p


@router.get("/{job_id}/report", response_class=HTMLResponse)
def get_report(job_id: str, store: Store) -> HTMLResponse:
    return HTMLResponse(_file(store, job_id, "post_migration_report.html").read_text("utf-8"))


@router.get("/{job_id}/report.csv")
def get_report_csv(job_id: str, store: Store) -> FileResponse:
    return FileResponse(
        _file(store, job_id, "post_migration_report.csv"),
        media_type="text/csv",
        filename=f"{job_id}_report.csv",
    )


@router.get("/{job_id}/runbook", response_class=PlainTextResponse)
def get_runbook(job_id: str, store: Store) -> PlainTextResponse:
    return PlainTextResponse(
        _file(store, job_id, "RUNBOOK.md").read_text("utf-8"), media_type="text/markdown"
    )


@router.get("/{job_id}/agent-trace")
def get_agent_trace(job_id: str, repo: Repo, store: Store) -> dict[str, Any]:
    rec = repo.get(require_job_id(job_id))
    key = keys.agent_trace(job_id)
    trace = store.read_json(key) if store.exists(key) else []
    return {"mode": rec.mode, "agent": rec.agent, "steps": trace}


@router.get("/{job_id}/artifacts")
def list_artifacts(job_id: str, repo: Repo, store: Store) -> dict[str, Any]:
    repo.get(require_job_id(job_id))
    out = _out_dir(store, job_id)
    files = (
        sorted(str(p.relative_to(out)) for p in out.rglob("*") if p.is_file())
        if out.is_dir()
        else []
    )
    return {"files": files}


@router.get("/{job_id}/artifacts/{path:path}")
def get_artifact(job_id: str, path: str, store: Store) -> FileResponse:
    out = _out_dir(store, job_id).resolve()
    p = (out / path).resolve()
    if out not in p.parents or not p.is_file():
        raise NotFound(f"artifact {path!r} not found")
    return FileResponse(p, filename=p.name)


@router.get("/{job_id}/download")
def download(job_id: str, service: Service, store: Store) -> FileResponse:
    """Zip of everything generated: notebooks, pipelines, scripts, ledger, report, runbook, AI work orders."""
    service.ensure_idle(require_job_id(job_id))
    out = _out_dir(store, job_id)
    if not out.is_dir():
        raise NotFound("nothing generated yet")
    zpath = store.path(f"{keys.job_prefix(job_id)}/{job_id}_fabric_migration.zip")
    with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in sorted(out.rglob("*")):
            if p.is_file():
                zf.write(p, p.relative_to(out))
    return FileResponse(zpath, media_type="application/zip", filename=zpath.name)


@router.post(
    "/{job_id}/deploy", status_code=status.HTTP_202_ACCEPTED, response_model=MigrationAccepted
)
def deploy_migration(job_id: str, body: DeployIn, service: Service) -> MigrationAccepted:
    """Deploy (or re-deploy) a finished migration to Fabric. Credentials travel with this request only."""
    return _accepted(service.redeploy(require_job_id(job_id), body))


@router.post("/{job_id}/validation")
async def upload_validation(
    job_id: str,
    service: Service,
    store: Store,
    file: Annotated[
        UploadFile, File(description="results.json written by the validate_migration notebook")
    ],
) -> dict[str, Any]:
    service.ensure_idle(require_job_id(job_id))
    try:
        results = json.loads(await file.read())
    except json.JSONDecodeError as e:
        raise ValidationFailed(f"results file is not valid JSON: {e}", field="file") from e
    if not isinstance(results, list):
        raise ValidationFailed("results.json must be a list of results", field="file")
    out = _out_dir(store, job_id)
    counts = import_validation(out, results)
    summary = write_report(out)
    service.repo.update(job_id, summary=summary)
    return {"imported": counts, "summary": summary}


@router.post("/{job_id}/results")
def record_item_result(
    job_id: str, body: ItemResultIn, service: Service, store: Store
) -> dict[str, Any]:
    """Record manual or AI work done outside the service so the report stays accurate."""
    service.ensure_idle(require_job_id(job_id))
    out = _out_dir(store, job_id)
    res = record_result(out, body.item_id, body.outcome, body.note, body.target_id, actor="manual")
    summary = write_report(out)
    service.repo.update(job_id, summary=summary)
    return {**res, "summary": summary}
