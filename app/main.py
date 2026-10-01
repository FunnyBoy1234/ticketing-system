"""HTTP API: wiring, validation, auth, and health/metrics/log endpoints."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from contextlib import asynccontextmanager
from typing import Annotated, Any

import asyncpg
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ConfigDict, Field, StrictInt, StringConstraints, ValidationError

from . import auth
from .config import settings
from .db import Database, DatabaseUnavailable
from .errors import ApiError, bad_request, forbidden, not_found, unprocessable
from .observability import (
    RequestContextMiddleware,
    current_request_id,
    log_buffer,
    seat_collector,
    setup_logging,
)
from .reservations import cancel, get_reservation, reservation_view, reserve
from .shows import create_show, get_show_state, seat_state_snapshot, show_cache

setup_logging(settings.log_level, settings.log_buffer_size)
log = logging.getLogger("app")

db = Database(settings)


@asynccontextmanager
async def lifespan(_: FastAPI):
    if settings.using_dev_secrets:
        log.warning("using_dev_secrets", extra={"hint": "set ADMIN_KEY and JWT_SECRET in production"})
    await db.start()
    log.info("startup_complete", extra={"pool_max": settings.db_pool_max})
    yield
    await db.stop()


app = FastAPI(
    title="Seat Reservation at Scale",
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url=None,
)
app.add_middleware(RequestContextMiddleware)


# --------------------------------------------------------------------------------------
# Error rendering: every non-2xx is JSON with a stable `error` code and the request id.
# --------------------------------------------------------------------------------------


def _json(status: int, body: dict[str, Any], headers: dict[str, str] | None = None) -> JSONResponse:
    return JSONResponse(status_code=status, content=body, headers=headers)


@app.exception_handler(ApiError)
async def _api_error(_: Request, exc: ApiError) -> JSONResponse:
    request_context_outcome(exc.code)
    return _json(exc.status, {**exc.body(), "request_id": current_request_id()})


@app.exception_handler(RequestValidationError)
async def _validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
    return _json(422, {"error": "validation_error", "details": exc.errors(), "request_id": current_request_id()})


async def _db_unavailable(_: Request, exc: Exception) -> JSONResponse:
    log.warning("db_unavailable", extra={"error": repr(exc)})
    return _json(
        503,
        {"error": "database_unavailable", "message": "storage is unreachable; retry shortly", "request_id": current_request_id()},
        headers={"Retry-After": "5"},
    )


for _exc in (DatabaseUnavailable, asyncpg.PostgresConnectionError, asyncpg.TooManyConnectionsError,
             ConnectionError, TimeoutError, asyncpg.InterfaceError):
    app.add_exception_handler(_exc, _db_unavailable)


def request_context_outcome(outcome: str) -> None:
    from .observability import request_context

    request_context()["outcome"] = outcome


# --------------------------------------------------------------------------------------
# Request models. Money is StrictInt paise: 25000.0 or "25000" is rejected, not coerced.
# --------------------------------------------------------------------------------------

SeatLabel = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_-]{1,16}$")]


class CreateShowIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=200)
    seats: list[SeatLabel] = Field(min_length=1, max_length=50_000)
    price_paise: StrictInt = Field(ge=0, le=10**12)
    per_user_limit: StrictInt | None = Field(default=None, ge=1, le=1000)


class ReserveIn(BaseModel):
    # Unknown fields (e.g. a spoofed "user_id") are ignored: identity comes only from the token.
    model_config = ConfigDict(extra="ignore")
    seats: list[SeatLabel] = Field(min_length=1, max_length=50)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=128)


class TokenIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    user_id: str | None = None
    user_ids: list[str] | None = Field(default=None, max_length=10_000)


async def _json_body(request: Request) -> Any:
    try:
        return await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise bad_request("invalid_json", "request body must be valid JSON") from exc


def _validate(model: type[BaseModel], raw: Any) -> Any:
    try:
        return model.model_validate(raw)
    except ValidationError as exc:
        raise RequestValidationError(exc.errors()) from exc


def _parse_uuid(raw: str, code: str, what: str) -> uuid.UUID:
    try:
        return uuid.UUID(raw)
    except ValueError as exc:
        raise not_found(code, f"{what} {raw} does not exist") from exc


# --------------------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------------------


@app.get("/")
async def index() -> dict[str, Any]:
    return {
        "service": "seat-reservation",
        "endpoints": [
            "POST /auth/token (admin)",
            "POST /shows (admin)",
            "GET  /shows/{id}",
            "POST /shows/{id}/reserve",
            "POST /reservations/{id}/cancel",
            "GET  /reservations/{id}",
            "GET  /livez", "GET  /readyz", "GET  /metrics", "GET  /logs",
        ],
        "docs": "/docs",
    }


@app.post("/auth/token")
async def issue_token(request: Request) -> JSONResponse:
    """Admin-only stand-in for an identity provider: mint user tokens for testing."""
    auth.require_admin(request)
    body: TokenIn = _validate(TokenIn, await _json_body(request))
    ids = ([body.user_id] if body.user_id else []) + (body.user_ids or [])
    if not ids:
        raise unprocessable("user_id_required", "provide user_id or user_ids")
    bad = [u for u in ids if not auth.USER_ID_RE.match(u)]
    if bad:
        raise unprocessable("invalid_user_id", "user ids must match [A-Za-z0-9_.@:-]{1,64}", invalid=bad[:10])
    tokens = {u: auth.mint_token(u) for u in ids}
    if body.user_id and not body.user_ids:
        return _json(200, {"user_id": body.user_id, "access_token": tokens[body.user_id], "token_type": "bearer",
                           "expires_in": settings.token_ttl_seconds})
    return _json(200, {"tokens": tokens, "token_type": "bearer", "expires_in": settings.token_ttl_seconds})


@app.post("/shows")
async def post_show(request: Request) -> JSONResponse:
    auth.require_admin(request)
    body: CreateShowIn = _validate(CreateShowIn, await _json_body(request))
    if len(set(body.seats)) != len(body.seats):
        seen, dupes = set(), []
        for s in body.seats:
            if s in seen:
                dupes.append(s)
            seen.add(s)
        raise unprocessable("duplicate_seats", "seat labels must be unique", seats=sorted(set(dupes))[:20])
    show = await create_show(
        db,
        name=body.name,
        seats=body.seats,
        price_paise=body.price_paise,
        per_user_limit=body.per_user_limit or settings.default_per_user_limit,
    )
    log.info("show_created", extra={"show_id": show["id"], "total_seats": show["total_seats"],
                                    "per_user_limit": show["per_user_limit"]})
    return _json(201, show)


@app.get("/shows/{show_id}")
async def get_show(show_id: str, request: Request) -> JSONResponse:
    sid = _parse_uuid(show_id, "show_not_found", "show")
    state = await get_show_state(db, sid, include_owners=auth.is_admin(request))
    return _json(200, state)


@app.post("/shows/{show_id}/reserve")
async def post_reserve(show_id: str, request: Request) -> JSONResponse:
    user_id = auth.require_user(request)  # identity: token only
    sid = _parse_uuid(show_id, "show_not_found", "show")
    raw = await _json_body(request)
    body: ReserveIn = _validate(ReserveIn, raw)

    if isinstance(raw, dict) and "user_id" in raw and raw["user_id"] != user_id:
        log.warning("ignored_body_identity", extra={"body_user_id": str(raw["user_id"])[:64]})

    header_key = request.headers.get("idempotency-key")
    if header_key and body.idempotency_key and header_key != body.idempotency_key:
        raise bad_request("idempotency_key_conflict", "Idempotency-Key header and body idempotency_key differ")
    key = header_key or body.idempotency_key
    if not key:
        raise bad_request("idempotency_key_required", "send an Idempotency-Key header or idempotency_key in the body")
    if len(key) > 128:
        raise bad_request("idempotency_key_too_long", "idempotency keys are at most 128 characters")

    if len(set(body.seats)) != len(body.seats):
        raise unprocessable("duplicate_seats", "each seat may appear only once in a request")
    show = await show_cache.get(db, sid)
    unknown = [s for s in body.seats if s not in show.seat_labels]
    if unknown:
        raise unprocessable("unknown_seats", "these seats do not exist in this show", seats=unknown[:20])

    outcome = await reserve(db, show, user_id, body.seats, key)
    payload = outcome.body if outcome.status < 400 else {**outcome.body, "request_id": current_request_id()}
    return _json(outcome.status, payload, headers=outcome.headers or None)


@app.post("/reservations/{reservation_id}/cancel")
async def post_cancel(reservation_id: str, request: Request) -> JSONResponse:
    user_id = auth.require_user(request)
    rid = _parse_uuid(reservation_id, "reservation_not_found", "reservation")
    result = await cancel(db, rid, user_id)
    return _json(result.status, result.body)


@app.get("/reservations/{reservation_id}")
async def get_reservation_route(reservation_id: str, request: Request) -> JSONResponse:
    admin = auth.is_admin(request)
    user_id = None if admin else auth.require_user(request)
    rid = _parse_uuid(reservation_id, "reservation_not_found", "reservation")
    row = await get_reservation(db, rid)
    if row is None:
        raise not_found("reservation_not_found", f"reservation {rid} does not exist")
    if not admin and row["user_id"] != user_id:
        raise forbidden("not_reservation_owner", "you can only view your own reservations")
    return _json(200, reservation_view(row))


# ---- health -------------------------------------------------------------------------


@app.get("/livez")
async def livez() -> dict[str, str]:
    """Liveness: the process is up and serving. Deliberately no dependency checks."""
    return {"status": "ok"}


@app.get("/readyz")
async def readyz() -> JSONResponse:
    """Readiness: can we serve traffic right now? Checks Postgres; fails closed (503)."""
    ok, detail = await db.probe()
    body = {"status": "ready" if ok else "not_ready", "checks": {"database": detail}, "pool": db.pool_stats()}
    return _json(200 if ok else 503, body)


# ---- metrics & logs -----------------------------------------------------------------


@app.get("/metrics")
async def metrics() -> Response:
    try:
        seat_collector.snapshot = await db.observer.run(
            lambda conn: seat_state_snapshot(conn, settings.metrics_max_shows), timeout=3.0
        )
        seat_collector.db_up = True
    except Exception as exc:  # noqa: BLE001 - metrics must still render when the DB is down
        seat_collector.db_up = False
        seat_collector.snapshot = []
        log.warning("metrics_db_snapshot_failed", extra={"error": repr(exc)})
    seat_collector.pool = db.pool_stats()
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/logs")
async def logs(request: Request, limit: int = 200, request_id: str | None = None, event: str | None = None) -> JSONResponse:
    """Tail of this instance's structured logs (newest last). Filter by request_id or event."""
    if not settings.logs_endpoint_public:
        auth.require_admin(request)
    limit = max(1, min(limit, 2000))
    items = log_buffer.tail(limit, request_id=request_id, event=event)
    return _json(200, {"count": len(items), "logs": items})
