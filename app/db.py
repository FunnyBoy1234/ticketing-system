"""Postgres access: connection pool, startup with retry, schema migration, health probe."""

from __future__ import annotations

import asyncio
import logging
import random
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import asyncpg

from .config import Settings
from .observability import DB_POOL_ACQUIRE_TIMEOUTS

log = logging.getLogger("app.db")

_SCHEMA = (Path(__file__).parent / "schema.sql").read_text()
_MIGRATION_LOCK_ID = 7_421_001  # arbitrary, app-wide advisory lock id


class DatabaseUnavailable(Exception):
    """Raised when a request needs the database and it is not reachable."""


class PoolExhausted(Exception):
    """Raised when a request waited DB_ACQUIRE_TIMEOUT_S for a pool connection. The database
    is up; every connection stayed busy. Rendered as 503 `overloaded`, not `database_unavailable`."""


class SideConnection:
    """A single dedicated connection outside the pool, reconnected on demand.

    Readiness probes and metrics scrapes use these, never a pool slot. Under a burst
    the pool is saturated by design (requests queue for it). A probe queued behind
    them would report "not ready" and could get a healthy instance restarted
    mid-burst; a metrics scrape queued behind them would go blind exactly when we
    most want to watch.
    """

    def __init__(self, dsn: str, name: str) -> None:
        self._dsn = dsn
        self._name = name
        self._conn: asyncpg.Connection | None = None
        self._lock = asyncio.Lock()

    async def run(self, fn, timeout: float):
        async with self._lock:
            try:
                if self._conn is None or self._conn.is_closed():
                    self._conn = await asyncpg.connect(
                        self._dsn,
                        timeout=timeout,
                        statement_cache_size=0,
                        server_settings={"application_name": f"seat-reservation-{self._name}"},
                    )
                return await asyncio.wait_for(fn(self._conn), timeout=timeout)
            except BaseException:
                if self._conn is not None:
                    self._conn.terminate()
                    self._conn = None
                raise

    async def close(self) -> None:
        if self._conn is not None and not self._conn.is_closed():
            await self._conn.close()


class Database:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._pool: asyncpg.Pool | None = None
        self._ready = asyncio.Event()
        self._connect_task: asyncio.Task | None = None
        self._probe = SideConnection(settings.database_url, "probe")
        self.observer = SideConnection(settings.database_url, "observer")

    # ---- lifecycle -------------------------------------------------------------

    async def start(self) -> None:
        """Begin connecting in the background and wait (bounded) for the first success.

        This runs during uvicorn's startup, which finishes BEFORE the server opens its port.
        So while we wait, nothing - not even /livez - answers, and no platform can route
        traffic to an instance that can't reach the database yet (on a cold start the first
        requests are held by the platform instead of getting 503s). If the database is still
        unreachable after DB_STARTUP_WAIT_S, we start anyway: /livez answers, /readyz reports
        not ready, and the background loop keeps retrying until the pool exists and the
        schema is applied.
        """
        self._connect_task = asyncio.create_task(self._connect_loop(), name="db-connect")
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=self._settings.db_startup_wait_s)
        except asyncio.TimeoutError:
            log.warning("db_not_ready_at_startup", extra={"waited_s": self._settings.db_startup_wait_s})

    async def stop(self) -> None:
        if self._connect_task and not self._connect_task.done():
            self._connect_task.cancel()
        await self._probe.close()
        await self.observer.close()
        if self._pool is not None:
            await self._pool.close()

    async def _connect_loop(self) -> None:
        delay = 0.5
        attempt = 0
        while True:
            attempt += 1
            try:
                pool = await asyncpg.create_pool(
                    self._settings.database_url,
                    min_size=self._settings.db_pool_min,
                    max_size=self._settings.db_pool_max,
                    statement_cache_size=self._settings.db_statement_cache_size,
                    command_timeout=self._settings.db_statement_timeout_ms / 1000 + 5,
                    server_settings={
                        "application_name": "seat-reservation",
                        "statement_timeout": str(self._settings.db_statement_timeout_ms),
                        # Never leave a transaction open if the app stalls mid-request.
                        "idle_in_transaction_session_timeout": "60000",
                        "jit": "off",
                    },
                )
                await self._migrate(pool)
                self._pool = pool
                self._ready.set()
                log.info("db_ready", extra={"attempt": attempt})
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - any failure means "retry later"
                log.warning("db_connect_failed", extra={"attempt": attempt, "error": repr(exc)})
                await asyncio.sleep(delay + random.uniform(0, delay / 2))
                delay = min(delay * 2, 10.0)

    @staticmethod
    async def _migrate(pool: asyncpg.Pool) -> None:
        async with pool.acquire() as conn:
            await conn.execute("SELECT pg_advisory_lock($1)", _MIGRATION_LOCK_ID)
            try:
                await conn.execute(_SCHEMA)
            finally:
                await conn.execute("SELECT pg_advisory_unlock($1)", _MIGRATION_LOCK_ID)

    # ---- access ----------------------------------------------------------------

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise DatabaseUnavailable("database not ready")
        return self._pool

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[asyncpg.Connection]:
        """A pool connection. Waits (FIFO) up to DB_ACQUIRE_TIMEOUT_S, then PoolExhausted.

        Only the wait for a connection is turned into PoolExhausted; a timeout while a query
        runs (command_timeout) is a different failure and surfaces as database_unavailable.
        """
        pool = self.pool
        try:
            conn = await pool.acquire(timeout=self._settings.db_acquire_timeout_s)
        except asyncio.TimeoutError as exc:
            DB_POOL_ACQUIRE_TIMEOUTS.inc()
            raise PoolExhausted(f"no connection within {self._settings.db_acquire_timeout_s}s") from exc
        try:
            yield conn
        finally:
            try:
                await pool.release(conn)  # rolls back anything left open and returns it to the pool
            except Exception as exc:  # noqa: BLE001
                # A connection broken mid-request (e.g. Postgres restarting) can fail its reset
                # here. asyncpg has already terminated it, so the pool will not reuse it. Raising
                # would replace the request's own outcome - a committed success, or the
                # connection error that maps to 503 - with an unmapped error, i.e. a 500.
                log.warning("db_release_failed", extra={"error": repr(exc)})

    def pool_stats(self) -> dict[str, int]:
        if self._pool is None:
            return {"size": 0, "idle": 0, "max": self._settings.db_pool_max}
        return {
            "size": self._pool.get_size(),
            "idle": self._pool.get_idle_size(),
            "max": self._pool.get_max_size(),
        }

    async def probe(self, timeout: float = 2.0) -> tuple[bool, str]:
        """Readiness check: is the database reachable *right now*? Fails closed."""
        if self._pool is None:
            return False, "pool not initialised (database unreachable or schema not applied yet)"
        try:
            await self._probe.run(lambda conn: conn.fetchval("SELECT 1"), timeout=timeout)
            return True, "ok"
        except Exception as exc:  # noqa: BLE001 - any failure means "not ready"
            return False, repr(exc)
