"""Synchronous conversion of a single notebook or job definition (no job, nothing stored). Spec §5.6."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, File, Form, UploadFile
from fastapi.responses import JSONResponse

from app.conversion.code import Ctx
from app.conversion.common import safe
from app.conversion.jobs import convert_job
from app.conversion.notebook import build_ipynb, convert_cells, outcome_from_ctx, parse_source
from app.core.exceptions import ValidationFailed
from app.migration.mapping import load_cfg, merge_mapping, validate_overrides
from app.schemas.convert import JobConvertIn, JobConvertOut, NotebookConvertOut
from app.sources.offline import EXT_LANG, detect_language

router = APIRouter(prefix="/convert", tags=["convert"])


def _json_form(raw: str, field: str) -> Any:
    try:
        return json.loads(raw or "null")
    except json.JSONDecodeError as e:
        raise ValidationFailed(f"{field} is not valid JSON: {e}", field=field) from e


@router.post("/notebook", response_model=NotebookConvertOut)
async def convert_notebook(
    file: Annotated[
        UploadFile, File(description=".ipynb or Databricks source (.py .sql .scala .r)")
    ],
    mapping: Annotated[str, Form(description="JSON object of mapping overrides")] = "{}",
    catalogs: Annotated[str, Form(description="JSON list of catalog names to rewrite")] = "[]",
    download: Annotated[
        bool, Form(description="Return the .ipynb file instead of the JSON report")
    ] = False,
) -> Any:
    name = file.filename or "notebook.py"
    if Path(name).suffix.lower() not in EXT_LANG:
        raise ValidationFailed(
            "unsupported file type (accepted: .ipynb .py .sql .scala .r)", field="file"
        )
    text = (await file.read()).decode("utf-8", "ignore")
    overrides = _json_form(mapping, "mapping") or {}
    cats = _json_form(catalogs, "catalogs") or []
    if not isinstance(overrides, dict) or not isinstance(cats, list):
        raise ValidationFailed("mapping must be an object and catalogs a list", field="mapping")
    cfg = merge_mapping(load_cfg(None), validate_overrides(overrides))
    ctx = Ctx(cfg, sorted(set(map(str, cats)) | set(cfg["catalog_to_lakehouse"])))
    cells = convert_cells(parse_source(text, detect_language(Path(name), text)), ctx)
    nb = build_ipynb(cells)
    if download:
        return JSONResponse(
            nb,
            headers={
                "Content-Disposition": f'attachment; filename="{safe(Path(name).stem)}.ipynb"'
            },
        )
    return NotebookConvertOut(
        outcome=outcome_from_ctx(ctx),
        changes=ctx.changes,
        todos=ctx.todos,
        warnings={k: v[1] for k, v in ctx.warns.items()},
        notebook=nb,
    )


@router.post("/job", response_model=JobConvertOut)
def convert_job_definition(body: JobConvertIn) -> JobConvertOut:
    cfg = merge_mapping(load_cfg(None), validate_overrides(body.mapping))
    pipe, sched, notes, omitted = convert_job(
        {"name": body.name, "meta": {"settings": body.settings}}, cfg
    )
    return JobConvertOut(pipeline=pipe, schedule=sched, notes=notes, omitted_tasks=omitted)
