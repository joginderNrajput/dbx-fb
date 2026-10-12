"""Object-store protocol for job records and job workspaces. Spec §7."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

__all__ = ["ObjectStore"]


class ObjectStore(Protocol):
    """Key/value store for job state.

    The migration engine writes many files (notebooks, pipelines, reports) and needs a real
    directory, so ``path()`` returns a local path for a key prefix. A remote backend would sync
    that directory; only the local backend exists today (ADR 0001).
    """

    def path(self, key: str) -> Path: ...

    def exists(self, key: str) -> bool: ...

    def read_bytes(self, key: str) -> bytes: ...

    def write_bytes(self, key: str, data: bytes) -> None: ...

    def read_json(self, key: str) -> Any: ...

    def write_json(self, key: str, obj: Any) -> None: ...

    def list_prefixes(self, prefix: str) -> list[str]: ...

    def delete_prefix(self, prefix: str) -> None: ...
