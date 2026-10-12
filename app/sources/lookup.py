"""Find a notebook's source text: uploaded files first, then the Databricks export API. Spec §5.3."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from app.core.exceptions import DatabricksError
from app.databricks.client import DatabricksClient
from app.sources.offline import EXT_LANG, detect_language

__all__ = ["SourceResolver"]


class SourceResolver:
    """Callable used by the engine as ``source_lookup(asset) -> (text, language) | None``.

    Matching rules are those of the reference tool's ``get_source``: exact path with each
    notebook extension, then any uploaded file with the same stem.
    """

    def __init__(self, source_dir: Path | None, dbx: DatabricksClient | None) -> None:
        self.source_dir = source_dir
        self.dbx = dbx
        self._files: list[Path] | None = None

    def _all_files(self) -> list[Path]:
        if self._files is None:
            root = self.source_dir
            self._files = (
                [p for p in root.rglob("*") if p.is_file() and p.suffix.lower() in EXT_LANG]
                if root and root.is_dir()
                else []
            )
        return self._files

    def __call__(self, a: dict[str, Any]) -> tuple[str, str] | None:
        path = a["meta"].get("source_path") or a["location"]
        if self.source_dir and self.source_dir.is_dir():
            root = self.source_dir
            rel = path.lstrip("/")
            cands = [root / (rel + e) for e in ("", ".py", ".sql", ".scala", ".r", ".ipynb")]
            cands += [p for p in self._all_files() if p.stem == Path(rel).name.split(".")[0]]
            base = root.resolve()
            for p in cands:
                # source_path comes from user-supplied assessment.json: never read outside the uploads
                if p.is_file() and base in p.resolve().parents:
                    text = p.read_text("utf-8", "ignore")
                    return text, a["meta"].get("language") or detect_language(p, text)
        if self.dbx:
            try:
                text = self.dbx.export_source(path)
            except DatabricksError:
                return None
            if text is not None:
                return text, a["meta"].get("language") or "PYTHON"
        return None
