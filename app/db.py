"""Postgres access: connection pool, startup with retry, schema migration, health probe."""

from __future__ import annotations

import asyncio
import logging
import random
from pathlib import Path

import asyncpg

from .config import Settings

log = logging.getLogger("app.db")

_SCHEMA = (Path(__file__).parent / "schema.sql").read_text()
_MIGRATION_LOCK_ID = 7_421_001  # arbitrary, app-wide advisory lock id


class DatabaseUnavailable(Exception):
    """Raised when a request needs the database and it is not reachable."""


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

        Cold starts on free tiers often bring the app up before the database is
        reachable. We keep retrying in the background; liveness stays green and
        readiness stays red until the pool exists and the schema is applied.
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

    def acquire(self):
        return self.pool.acquire(timeout=self._settings.db_acquire_timeout_s)

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
