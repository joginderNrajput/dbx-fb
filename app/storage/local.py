"""Local-filesystem object store with path-traversal guards. Spec §7."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any

from app.core.exceptions import ValidationFailed

__all__ = ["LocalStore"]


class LocalStore:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def path(self, key: str) -> Path:
        p = (self.root / key).resolve()
        if p != self.root and self.root not in p.parents:
            raise ValidationFailed(f"storage key escapes the store root: {key!r}")
        return p

    def exists(self, key: str) -> bool:
        return self.path(key).exists()

    def read_bytes(self, key: str) -> bytes:
        return self.path(key).read_bytes()

    def write_bytes(self, key: str, data: bytes) -> None:
        p = self.path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, p)  # atomic: a reader never sees a half-written job.json

    def read_json(self, key: str) -> Any:
        return json.loads(self.read_bytes(key))

    def write_json(self, key: str, obj: Any) -> None:
        self.write_bytes(key, json.dumps(obj, indent=2, ensure_ascii=False, default=str).encode())

    def list_prefixes(self, prefix: str) -> list[str]:
        p = self.path(prefix)
        if not p.is_dir():
            return []
        return sorted(c.name for c in p.iterdir() if c.is_dir())

    def delete_prefix(self, prefix: str) -> None:
        shutil.rmtree(self.path(prefix), ignore_errors=True)
