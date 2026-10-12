"""Shared schema base and the error body. Spec §4.4."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

__all__ = ["ErrorBody", "ErrorOut", "StrictModel"]


class StrictModel(BaseModel):
    """Inbound models reject unknown fields: a typo is a 400 naming the field."""

    model_config = ConfigDict(extra="forbid")


class ErrorBody(BaseModel):
    code: str
    message: str
    field: str | None = None


class ErrorOut(BaseModel):
    error: ErrorBody
