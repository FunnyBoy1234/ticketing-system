"""Structured logging, request correlation, and Prometheus metrics."""

from __future__ import annotations

import collections
import contextvars
import json
import logging
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from prometheus_client import Counter, Gauge, Histogram
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import REGISTRY, Collector
from starlette.datastructures import MutableHeaders

# --------------------------------------------------------------------------------------
# Request context: one mutable dict per request, visible to every log line it produces.
# --------------------------------------------------------------------------------------

_request_ctx: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar("request_ctx", default=None)
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


def request_context() -> dict[str, Any]:
    """The current request's context dict (a throwaway dict outside a request)."""
    ctx = _request_ctx.get()
    return ctx if ctx is not None else {}


def current_request_id() -> str | None:
    ctx = _request_ctx.get()
    return ctx["request_id"] if ctx else None


# --------------------------------------------------------------------------------------
# Logging: one JSON object per line on stdout, plus an in-memory ring buffer that backs
# GET /logs so reviewers can read live logs without an account on the hosting platform.
# --------------------------------------------------------------------------------------

_STD_ATTRS = set(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {"message", "asctime", "taskName", "color_message"}


def _record_to_dict(record: logging.LogRecord) -> dict[str, Any]:
    out: dict[str, Any] = {
        "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
        "level": record.levelname,
        "logger": record.name,
        "event": record.getMessage(),
    }
    ctx = _request_ctx.get()
    if ctx:
        out["request_id"] = ctx["request_id"]
        if ctx.get("user_id"):
            out["user_id"] = ctx["user_id"]
    for key, value in record.__dict__.items():
        if key not in _STD_ATTRS and not key.startswith("_"):
            out[key] = value
    if record.exc_info:
        out["exc"] = logging.Formatter().formatException(record.exc_info)
    return out


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(_record_to_dict(record), default=str, separators=(",", ":"))


class RingBufferHandler(logging.Handler):
    def __init__(self, capacity: int) -> None:
        super().__init__()
        self.buffer: collections.deque[dict[str, Any]] = collections.deque(maxlen=capacity)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.buffer.append(_record_to_dict(record))
        except Exception:  # noqa: BLE001 - logging must never break a request
            self.handleError(record)

    def tail(self, limit: int, request_id: str | None = None, event: str | None = None) -> list[dict[str, Any]]:
        items = list(self.buffer)
        if request_id:
            items = [i for i in items if i.get("request_id") == request_id]
        if event:
            items = [i for i in items if i.get("event") == event]
        return items[-limit:]


log_buffer = RingBufferHandler(capacity=5000)


def setup_logging(level: str, buffer_size: int) -> None:
    log_buffer.buffer = collections.deque(log_buffer.buffer, maxlen=buffer_size)
    stdout = logging.StreamHandler(sys.stdout)
    stdout.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [stdout, log_buffer]
    root.setLevel(level)
    # Our middleware writes the access log (with request id); silence uvicorn's.
    logging.getLogger("uvicorn.access").disabled = True
    for name in ("uvicorn", "uvicorn.error"):
        logging.getLogger(name).handlers[:] = []
        logging.getLogger(name).propagate = True


# --------------------------------------------------------------------------------------
# Metrics. Counters are bumped after the transaction commits, so they count decisions
# that actually happened. Seat gauges are read from Postgres at scrape time, so they
# always agree with GET /shows/{id} (both come from the same rows).
# --------------------------------------------------------------------------------------

RESERVATIONS_CONFIRMED = Counter(
    "reservations_confirmed", "Reservations confirmed (HTTP 201).", ["show_id"]
)
SEATS_CONFIRMED = Counter(
    "reservation_seats_confirmed", "Seats moved from available to confirmed.", ["show_id"]
)
RESERVATIONS_DECLINED = Counter(
    "reservations_declined",
    "Reserve requests that did not create a reservation, by reason "
    "(seat_taken, per_user_limit, idempotent_replay, idempotency_key_mismatch).",
    ["show_id", "reason"],
)
RESERVATIONS_CANCELLED = Counter(
    "reservations_cancelled", "Reservations cancelled by their owner.", ["show_id"]
)
SEATS_RELEASED = Counter(
    "reservation_seats_released", "Seats returned to available by cancellation.", ["show_id"]
)
DB_POOL_ACQUIRE_TIMEOUTS = Counter(
    "db_pool_acquire_timeouts",
    "Requests that waited DB_ACQUIRE_TIMEOUT_S for a pool connection and got 503 overloaded.",
)
RESERVE_RETRIES = Counter(
    "reserve_transaction_retries", "Reserve transactions retried after a deadlock/serialization error.", ["sqlstate"]
)
HTTP_REQUESTS = Counter("http_requests", "HTTP requests by route and status.", ["method", "route", "status"])
HTTP_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency.",
    ["method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
)
IN_FLIGHT = Gauge("http_requests_in_flight", "HTTP requests currently being served.")


class SeatStateCollector(Collector):
    """Exposes per-show seat counts from the last DB snapshot taken by /metrics."""

    def __init__(self) -> None:
        self.snapshot: list[dict[str, Any]] = []
        self.db_up: bool = False
        self.pool: dict[str, int] = {}

    def collect(self):
        up = GaugeMetricFamily("db_up", "1 if the last metrics scrape could query Postgres.")
        up.add_metric([], 1 if self.db_up else 0)
        yield up

        pool = GaugeMetricFamily("db_pool_connections", "asyncpg pool connections by state.", labels=["state"])
        if self.pool:
            pool.add_metric(["open"], self.pool["size"])
            pool.add_metric(["idle"], self.pool["idle"])
            pool.add_metric(["in_use"], self.pool["size"] - self.pool["idle"])
            pool.add_metric(["max"], self.pool["max"])
        yield pool

        families = {
            name: GaugeMetricFamily(f"seats_{name}", f"Seats currently {name}, per show.", labels=["show_id", "show_name"])
            for name in ("available", "held", "confirmed", "total")
        }
        recon = GaugeMetricFamily(
            "seats_reconciliation_ok",
            "1 if available + held + confirmed == total_seats and owned seats match active reservations.",
            labels=["show_id", "show_name"],
        )
        for row in self.snapshot:
            labels = [row["show_id"], row["show_name"]]
            for name in ("available", "held", "confirmed", "total"):
                families[name].add_metric(labels, row[name])
            recon.add_metric(labels, 1 if row["ok"] else 0)
        yield from families.values()
        yield recon


seat_collector = SeatStateCollector()
REGISTRY.register(seat_collector)


# --------------------------------------------------------------------------------------
# ASGI middleware: request id, access log, HTTP metrics, last-resort 500 handler.
# --------------------------------------------------------------------------------------

_access_log = logging.getLogger("app.access")
_QUIET_ROUTES = {"/metrics", "/livez", "/readyz", "/logs"}


class RequestContextMiddleware:
    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        incoming = ""
        for name, value in scope.get("headers", []):
            if name == b"x-request-id":
                incoming = value.decode("latin-1")
                break
        request_id = incoming if _REQUEST_ID_RE.fullmatch(incoming) else uuid.uuid4().hex
        ctx = {"request_id": request_id, "user_id": None, "outcome": None}
        token = _request_ctx.set(ctx)

        state = {"status": 500, "started": False}

        async def send_wrapper(message) -> None:
            if message["type"] == "http.response.start":
                state["status"] = message["status"]
                state["started"] = True
                MutableHeaders(scope=message).append("X-Request-ID", request_id)
            await send(message)

        started_at = time.perf_counter()
        IN_FLIGHT.inc()
        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            _access_log.exception("unhandled_exception", extra={"path": scope.get("path")})
            if state["started"]:
                raise
            body = json.dumps(
                {"error": "internal_error", "message": "unexpected server error", "request_id": request_id}
            ).encode()
            await send_wrapper(
                {
                    "type": "http.response.start",
                    "status": 500,
                    "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())],
                }
            )
            await send({"type": "http.response.body", "body": body})
        finally:
            IN_FLIGHT.dec()
            elapsed = time.perf_counter() - started_at
            route = getattr(scope.get("route"), "path", "unmatched")
            method = scope.get("method", "")
            status = state["status"]
            HTTP_REQUESTS.labels(method, route, str(status)).inc()
            HTTP_LATENCY.labels(method, route).observe(elapsed)
            if route not in _QUIET_ROUTES or status >= 400:
                fields = {
                    "method": method,
                    "path": scope.get("path"),
                    "route": route,
                    "status": status,
                    "duration_ms": round(elapsed * 1000, 2),
                }
                if ctx.get("outcome"):
                    fields["outcome"] = ctx["outcome"]
                level = logging.ERROR if status >= 500 else logging.INFO
                _access_log.log(level, "http_request", extra=fields)
            _request_ctx.reset(token)
