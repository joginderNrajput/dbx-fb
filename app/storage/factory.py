"""Object store selection by setting. Spec §7."""

from __future__ import annotations

from app.core.config import Settings
from app.storage.base import ObjectStore
from app.storage.local import LocalStore

__all__ = ["build_store"]


def build_store(settings: Settings) -> ObjectStore:
    # OBJECT_STORE_BACKEND only allows "local" today; the Literal keeps the switch explicit.
    return LocalStore(settings.OUTPUT_DIR)
