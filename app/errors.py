"""Error types that map to clean JSON responses."""

from __future__ import annotations

from typing import Any


class ApiError(Exception):
    """An expected, client-facing outcome rendered as {"error": code, "message": ...}."""

    def __init__(self, status: int, code: str, message: str, **extra: Any) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.extra = extra

    def body(self) -> dict[str, Any]:
        return {"error": self.code, "message": self.message, **self.extra}


def bad_request(code: str, message: str, **extra: Any) -> ApiError:
    return ApiError(400, code, message, **extra)


def unauthorized(message: str = "missing or invalid bearer token") -> ApiError:
    return ApiError(401, "unauthorized", message)


def forbidden(code: str, message: str) -> ApiError:
    return ApiError(403, code, message)


def not_found(code: str, message: str) -> ApiError:
    return ApiError(404, code, message)


def unprocessable(code: str, message: str, **extra: Any) -> ApiError:
    return ApiError(422, code, message, **extra)
