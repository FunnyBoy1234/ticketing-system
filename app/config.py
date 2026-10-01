"""Runtime configuration, read once from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass

_DEV_ADMIN_KEY = "dev-admin-key"
_DEV_JWT_SECRET = "dev-jwt-secret-change-me"


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    database_url: str
    admin_key: str
    jwt_secret: str
    token_ttl_seconds: int
    default_per_user_limit: int
    # Pool sizing: the pool is the only queue in front of Postgres. Requests wait
    # for a connection instead of failing, so the acquire timeout is generous:
    # under a burst we would rather be slow than return a 5xx.
    db_pool_min: int
    db_pool_max: int
    db_acquire_timeout_s: float
    db_statement_timeout_ms: int
    # Set to 0 when running behind PgBouncer in transaction mode (Neon/Supabase poolers).
    db_statement_cache_size: int
    db_startup_wait_s: float
    log_level: str
    log_buffer_size: int
    logs_endpoint_public: bool
    metrics_max_shows: int

    @property
    def using_dev_secrets(self) -> bool:
        return self.admin_key == _DEV_ADMIN_KEY or self.jwt_secret == _DEV_JWT_SECRET


def load_settings() -> Settings:
    db_url = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/seats")
    # Some platforms hand out postgres:// URLs; asyncpg accepts both, normalise anyway.
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]
    return Settings(
        database_url=db_url,
        admin_key=os.environ.get("ADMIN_KEY", _DEV_ADMIN_KEY),
        jwt_secret=os.environ.get("JWT_SECRET", _DEV_JWT_SECRET),
        token_ttl_seconds=_int("TOKEN_TTL_SECONDS", 7 * 24 * 3600),
        default_per_user_limit=_int("DEFAULT_PER_USER_LIMIT", 4),
        db_pool_min=_int("DB_POOL_MIN", 2),
        db_pool_max=_int("DB_POOL_MAX", 20),
        db_acquire_timeout_s=float(os.environ.get("DB_ACQUIRE_TIMEOUT_S", 120)),
        db_statement_timeout_ms=_int("DB_STATEMENT_TIMEOUT_MS", 30_000),
        db_statement_cache_size=_int("DB_STATEMENT_CACHE_SIZE", 100),
        db_startup_wait_s=float(os.environ.get("DB_STARTUP_WAIT_S", 45)),
        log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        log_buffer_size=_int("LOG_BUFFER_SIZE", 5000),
        logs_endpoint_public=_bool("LOGS_ENDPOINT_PUBLIC", True),
        metrics_max_shows=_int("METRICS_MAX_SHOWS", 50),
    )


settings = load_settings()
