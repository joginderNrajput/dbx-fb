"""Credential resolution. Values live only in memory for the job's lifetime. Spec §8.2."""

from __future__ import annotations

import os

from pydantic import SecretStr

from app.core.config import Settings
from app.core.exceptions import ValidationFailed

__all__ = ["resolve_secret"]


def resolve_secret(value: SecretStr | str | None, settings: Settings, *, field: str) -> str | None:
    """Return the plain value of a credential field.

    ``env://NAME`` references are a development convenience (ALLOW_ENV_CREDENTIALS): they let a
    request name a variable instead of carrying the secret. Production refuses them so a caller
    cannot read arbitrary environment variables of the service.
    """
    if value is None:
        return None
    raw = value.get_secret_value() if isinstance(value, SecretStr) else value
    if not raw:
        return None
    if raw.startswith("env://"):
        if not settings.ALLOW_ENV_CREDENTIALS:
            raise ValidationFailed("env:// credential references are disabled", field=field)
        resolved = os.environ.get(raw[len("env://") :])
        if not resolved:
            raise ValidationFailed(
                f"environment variable {raw[len('env://') :]} is not set", field=field
            )
        return resolved
    return raw
