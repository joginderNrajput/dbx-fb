"""Synchronous single-asset conversion models. Spec §5.6."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from app.schemas.common import StrictModel

__all__ = ["JobConvertIn", "JobConvertOut", "NotebookConvertOut"]


class NotebookConvertOut(BaseModel):
    outcome: str
    changes: dict[str, int]
    todos: list[dict[str, str]]
    warnings: dict[str, str]
    notebook: dict[str, Any]


class JobConvertIn(StrictModel):
    name: str = "job"
    settings: dict[str, Any] = Field(
        ..., description="Databricks job settings (as in jobs/get .settings)"
    )
    mapping: dict[str, Any] = Field(default_factory=dict)


class JobConvertOut(BaseModel):
    pipeline: dict[str, Any] | None
    schedule: dict[str, Any] | None
    notes: list[str]
    omitted_tasks: list[str]
