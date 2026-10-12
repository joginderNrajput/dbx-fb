"""Offline sources: safely ingest uploads and build an inventory from them. Spec §5.2.

Accepted uploads: notebooks (.ipynb, Databricks source .py/.sql/.scala/.r), .zip archives of
those (folders kept), ``assessment.json`` (an inventory from dbx2fabric_assess.py) and
``mapping.json``. Everything else is skipped and reported.
"""

from __future__ import annotations

import io
import json
import posixpath
import zipfile
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from app.conversion.common import nb_name
from app.core.exceptions import PayloadTooLarge, ValidationFailed

__all__ = [
    "EXT_LANG",
    "IngestLimits",
    "IngestResult",
    "build_offline_assessment",
    "detect_language",
    "ingest_uploads",
]

EXT_LANG = {".py": "PYTHON", ".sql": "SQL", ".scala": "SCALA", ".r": "R", ".ipynb": "PYTHON"}
CONTROL_FILES = {"assessment.json", "mapping.json"}
# Supporting files kept for dbt projects uploaded with the sources (spec §3.6).
DBT_EXT = {".yml", ".yaml", ".md", ".csv", ".txt", ".jinja", ".j2"}
SKIP_DIRS = {"__MACOSX", ".git", ".ipynb_checkpoints", "__pycache__"}


@dataclass
class IngestLimits:
    max_files: int
    max_total_bytes: int


@dataclass
class IngestResult:
    notebooks: list[str] = field(default_factory=list)  # relative posix paths under the sources dir
    assessment: dict[str, Any] | None = None
    mapping: dict[str, Any] | None = None
    skipped: list[str] = field(default_factory=list)
    total_bytes: int = 0


def _safe_rel(name: str) -> str | None:
    """Normalise an upload/zip member name; None if it is absolute or climbs out (zip slip)."""
    name = name.replace("\\", "/")
    if name.startswith("/") or (len(name) > 1 and name[1] == ":"):
        return None
    parts = [p for p in PurePosixPath(posixpath.normpath(name)).parts if p not in ("", ".")]
    if not parts or ".." in parts or any(p in SKIP_DIRS for p in parts):
        return None
    return "/".join(parts)


def _store(rel: str, data: bytes, dest: Path, res: IngestResult, limits: IngestLimits) -> None:
    res.total_bytes += len(data)
    if res.total_bytes > limits.max_total_bytes:
        raise PayloadTooLarge(
            f"uploaded sources exceed {limits.max_total_bytes // (1024 * 1024)} MB once extracted"
        )
    if len(res.notebooks) + len(res.skipped) > limits.max_files:
        raise PayloadTooLarge(f"more than {limits.max_files} files uploaded")
    base = PurePosixPath(rel).name.lower()
    suffix = PurePosixPath(rel).suffix.lower()
    if base in CONTROL_FILES and "/" not in rel:
        try:
            obj = json.loads(data.decode("utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise ValidationFailed(f"{base} is not valid JSON: {e}", field=f"files.{base}") from e
        if base == "assessment.json":
            if not isinstance(obj, dict) or not isinstance(obj.get("assets"), list):
                raise ValidationFailed(
                    "assessment.json must be an object with an 'assets' list",
                    field="files.assessment.json",
                )
            res.assessment = obj
        else:
            if not isinstance(obj, dict):
                raise ValidationFailed("mapping.json must be an object", field="files.mapping.json")
            res.mapping = obj
        return
    if suffix in EXT_LANG or suffix in DBT_EXT:
        target = dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        if suffix in EXT_LANG:
            res.notebooks.append(rel)
        return
    res.skipped.append(rel)


def ingest_uploads(
    files: list[tuple[str, bytes]], dest: Path, limits: IngestLimits
) -> IngestResult:
    """Write accepted uploads under ``dest``; zips are extracted member by member with guards."""
    dest.mkdir(parents=True, exist_ok=True)
    res = IngestResult()
    for filename, data in files:
        rel = _safe_rel(filename or "")
        if rel is None:
            res.skipped.append(filename or "<unnamed>")
            continue
        if rel.lower().endswith(".zip"):
            try:
                zf = zipfile.ZipFile(io.BytesIO(data))
            except zipfile.BadZipFile as e:
                raise ValidationFailed(f"{rel} is not a valid zip archive", field="files") from e
            with zf:
                for info in zf.infolist():
                    if info.is_dir():
                        continue
                    mrel = _safe_rel(info.filename)
                    if mrel is None:
                        res.skipped.append(info.filename)
                        continue
                    if info.file_size > limits.max_total_bytes:
                        raise PayloadTooLarge(f"zip member {info.filename} is too large")
                    _store(mrel, zf.read(info), dest, res, limits)
        elif rel.lower().endswith(".dbc"):
            res.skipped.append(
                rel + " (DBC archives are not supported: export as SOURCE or .ipynb)"
            )
        else:
            _store(rel, data, dest, res, limits)
    return res


def detect_language(path: Path, text: str | None = None) -> str:
    """Notebook language from the extension, or from .ipynb metadata (Databricks or Jupyter)."""
    suffix = path.suffix.lower()
    if suffix != ".ipynb":
        return EXT_LANG.get(suffix, "PYTHON")
    try:
        meta = json.loads(text if text is not None else path.read_text("utf-8", "ignore")).get(
            "metadata", {}
        )
    except (json.JSONDecodeError, AttributeError):
        return "PYTHON"
    lang = (
        meta.get("application/vnd.databricks.v1+notebook", {}).get("language")
        or meta.get("language_info", {}).get("name")
        or meta.get("kernelspec", {}).get("language")
        or "python"
    )
    lang = str(lang).upper()
    return {"PYSPARK": "PYTHON", "SPARK": "SCALA", "SPARKR": "R"}.get(
        lang, lang if lang in ("PYTHON", "SQL", "SCALA", "R") else "PYTHON"
    )


def build_offline_assessment(source_dir: Path, notebooks: list[str]) -> dict[str, Any]:
    """One Notebook asset per uploaded notebook file; location mirrors the uploaded folder tree."""
    assets: list[dict[str, Any]] = []
    for rel in sorted(notebooks):
        p = source_dir / rel
        stem = str(PurePosixPath(rel).with_suffix(""))
        location = "/" + stem
        assets.append(
            {
                "type": "Notebook",
                "name": nb_name(location),
                "location": location,
                "meta": {
                    "source_path": location,
                    "language": detect_language(p),
                    "uploaded_file": rel,
                },
            }
        )
    by_type = {"Notebook": len(assets)} if assets else {}
    return {
        "source": {"kind": "upload"},
        "summary": {"total_assets": len(assets), "by_type": by_type},
        "warnings": [],
        "assets": assets,
    }
