"""Domain error hierarchy; the API maps these to one error body in app/api/errors.py. Spec §4.4."""

from __future__ import annotations

from typing import Any

__all__ = [
    "AIUnavailable",
    "AppError",
    "Cancelled",
    "Conflict",
    "DatabricksError",
    "FabricError",
    "NotFound",
    "PayloadTooLarge",
    "Unauthorized",
    "UpstreamError",
    "ValidationFailed",
]


class AppError(Exception):
    """Base error with a stable machine code and an HTTP status."""

    code: str = "internal_error"
    http_status: int = 500

    def __init__(self, message: str, *, field: str | None = None, details: Any = None) -> None:
        super().__init__(message)
        self.message = message
        self.field = field
        self.details = details


class ValidationFailed(AppError):
    code = "validation_failed"
    http_status = 422


class NotFound(AppError):
    code = "not_found"
    http_status = 404


class Conflict(AppError):
    code = "conflict"
    http_status = 409


class Unauthorized(AppError):
    code = "unauthorized"
    http_status = 401


class PayloadTooLarge(AppError):
    code = "payload_too_large"
    http_status = 413


class UpstreamError(AppError):
    code = "upstream_error"
    http_status = 502


class DatabricksError(UpstreamError):
    code = "databricks_error"


class FabricError(UpstreamError):
    code = "fabric_error"


class AIUnavailable(AppError):
    code = "ai_unavailable"
    http_status = 503


class Cancelled(AppError):
    """Raised inside a job thread at a stage boundary once the job was cancelled or timed out."""

    code = "cancelled"
    http_status = 409
