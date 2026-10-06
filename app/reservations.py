"""The reservation core: reserve and cancel.

Where the atomic decision lives
-------------------------------
A seat is a row in `seats`. It moves from available to confirmed only through

    UPDATE seats SET status = 'confirmed', ...
    WHERE show_id = $1 AND label = ANY($2) AND status = 'available'

executed while this transaction holds row locks on exactly those seats, taken by
`SELECT ... ORDER BY label FOR UPDATE`. Locking in one global order (label) means two
multi-seat requests can never wait on each other in a cycle, so there is no deadlock.
Under READ COMMITTED, FOR UPDATE returns the latest committed version of each row after
any wait, so the status we check is the status we hold the lock on - nobody can change
it until we commit. The guarded UPDATE re-asserts it (belt and braces) and we require
it to touch every requested seat, otherwise the whole attempt rolls back.

Lock order for every write path: seat rows (by label) -> user quota row. Reserve claims its
idempotency key first and cancel locks its reservation row first; neither ever locks the
other's kind of row, so those come before the shared order without creating a cycle.
Because every path takes seats-then-quota, reserve and cancel cannot deadlock.

Seats are locked before the quota row so that a request which is both over the limit and
asking for a taken seat is always declined as seat_taken - the same answer the fast path
gives - no matter which path it takes.

Multi-seat requests are ALL-OR-NOTHING: either every requested seat is confirmed in one
reservation or nothing changes.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import uuid
from dataclasses import dataclass, field
from typing import Any

import asyncpg

from .db import Database
from .observability import (
    RESERVATIONS_CANCELLED,
    RESERVATIONS_CONFIRMED,
    RESERVATIONS_DECLINED,
    RESERVE_RETRIES,
    SEATS_CONFIRMED,
    SEATS_RELEASED,
    request_context,
)
from .shows import ShowMeta

log = logging.getLogger("app.reservations")

_RETRYABLE = (asyncpg.exceptions.DeadlockDetectedError, asyncpg.exceptions.SerializationError)
_MAX_ATTEMPTS = 4


@dataclass
class Outcome:
    status: int
    body: dict[str, Any]
    reason: str
    replayed: bool = False
    headers: dict[str, str] = field(default_factory=dict)


class _Decline(Exception):
    """Raised inside the savepoint to roll back a losing attempt."""

    def __init__(self, reason: str, body: dict[str, Any]) -> None:
        super().__init__(reason)
        self.reason = reason
        self.body = body


def request_hash(show_id: uuid.UUID, seats: list[str]) -> str:
    canonical = json.dumps({"show_id": str(show_id), "seats": sorted(seats)}, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _seat_taken_body(show_id: uuid.UUID, taken: list[str]) -> dict[str, Any]:
    return {
        "error": "seat_taken",
        "message": "one or more requested seats are no longer available; nothing was reserved",
        "show_id": str(show_id),
        "seats": taken,
    }


def _limit_body(show: ShowMeta, requested: int) -> dict[str, Any]:
    return {
        "error": "per_user_limit",
        "message": f"this would take you over the limit of {show.per_user_limit} seats for this show",
        "show_id": str(show.id),
        "per_user_limit": show.per_user_limit,
        "requested": requested,
    }



async def reserve(db: Database, show: ShowMeta, user_id: str, seats: list[str], key: str) -> Outcome:
    """Reserve `seats` (all-or-nothing) for `user_id`, exactly once per idempotency key."""
    req_hash = request_hash(show.id, seats)
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            async with db.acquire() as conn:
                outcome = await _reserve_once(conn, show, user_id, seats, key, req_hash)
            break
        except _RETRYABLE as exc:
            # Whole transaction rolled back (including the key claim), so retrying is safe.
            # By design this should not happen; the counter tells us if it ever does.
            RESERVE_RETRIES.labels(exc.sqlstate).inc()
            log.warning("reserve_retry", extra={"attempt": attempt, "sqlstate": exc.sqlstate})
            if attempt == _MAX_ATTEMPTS:
                raise
            await asyncio.sleep(random.uniform(0.005, 0.05) * attempt)

    _record_metrics(show, seats, outcome)
    return outcome


async def _reserve_once(
    conn: asyncpg.Connection, show: ShowMeta, user_id: str, seats: list[str], key: str, req_hash: str
) -> Outcome:
    # Step 1: Retry of a request we already decided? Cheap read, no locks. Most retries end here.
    existing = await _load_key(conn, show.id, user_id, key)
    if existing is not None:
        return _replay(existing, req_hash)

    # Step 2: Fast decline for hot seats: a plain read of committed state. If any seat is already
    #    taken we decline right away instead of queueing on its row lock. This is safe because
    #    it can only produce a *decline*, and the seat really was taken at the moment we read
    #    it. A *confirm* never comes from this read - it has to win the locked path below.
    #    It runs before the limit check so seat_taken always wins over per_user_limit.
    taken = await conn.fetch(
        "SELECT label FROM seats WHERE show_id = $1 AND label = ANY($2::text[]) AND status <> 'available' ORDER BY label",
        show.id, seats,
    )
    if taken:
        return await _record_decline(
            conn, show, user_id, key, req_hash, "seat_taken", _seat_taken_body(show.id, [r["label"] for r in taken])
        )

    # Step 3: Over the limit in a single request: decline without touching any locks.
    if len(seats) > show.per_user_limit:
        return await _record_decline(conn, show, user_id, key, req_hash, "per_user_limit", _limit_body(show, len(seats)))

    # Step 4: Authoritative path: one transaction.
    async with conn.transaction():
        # Step 4a: Claim the idempotency key. If another request holds it uncommitted, this
        #     INSERT waits for that transaction; if it committed we get no row back.
        claimed = await conn.fetchval(
            """
            INSERT INTO idempotency_keys (user_id, key, request_hash, show_id)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (user_id, show_id, key) DO NOTHING
            RETURNING true
            """,
            user_id, key, req_hash, show.id,
        )
        if not claimed:
            existing = await _load_key(conn, show.id, user_id, key)
            return _replay(existing, req_hash)

        try:
            async with conn.transaction():  # SAVEPOINT: a decline undoes only this block
                reservation = await _take_seats(conn, show, user_id, seats)
            status, body, reason = 201, reservation, "confirmed"
        except _Decline as decline:
            status, body, reason = 409, decline.body, decline.reason

        # Step 4b: Record the decision under the key, in the same transaction as its effects.
        await conn.execute(
            """
            UPDATE idempotency_keys
               SET status_code = $4, response = $5::jsonb, reservation_id = $6
             WHERE user_id = $1 AND show_id = $2 AND key = $3
            """,
            user_id, show.id, key, status, json.dumps(body),
            uuid.UUID(body["reservation_id"]) if status == 201 else None,
        )
    return Outcome(status=status, body=body, reason=reason)


async def _take_seats(conn: asyncpg.Connection, show: ShowMeta, user_id: str, seats: list[str]) -> dict[str, Any]:
    n = len(seats)

    """
        Lock the requested seats in a deterministic order (by label) - no deadlocks. Seats come
        before the quota row, so a request that is over the limit AND wants a taken seat is
        declined as seat_taken here, matching the fast path.
    """

    locked = await conn.fetch(
        "SELECT label, status FROM seats WHERE show_id = $1 AND label = ANY($2::text[]) ORDER BY label FOR UPDATE",
        show.id, seats,
    )
    taken = [r["label"] for r in locked if r["status"] != "available"]
    if taken:
        raise _Decline("seat_taken", _seat_taken_body(show.id, taken))
    if len(locked) != n:  # validated against the show's seat list already; defensive only
        raise RuntimeError(f"seat rows missing for show {show.id}: wanted {n}, found {len(locked)}")

    """
        Per-user limit: check-and-increment in ONE statement. The row lock it takes also
        serialises this user's concurrent requests for this show, so 10 parallel requests
        on a limit of 4 cannot all see "0 held" and all succeed.
    """

    held_after = await conn.fetchval(
        """
        INSERT INTO user_show_quota (show_id, user_id, seats_held)
        SELECT $1::uuid, $2::text, $3::int WHERE $3::int <= $4::int
        ON CONFLICT (show_id, user_id) DO UPDATE
           SET seats_held = user_show_quota.seats_held + EXCLUDED.seats_held
         WHERE user_show_quota.seats_held + EXCLUDED.seats_held <= $4::int
        RETURNING seats_held
        """,
        show.id, user_id, n, show.per_user_limit,
    )
    if held_after is None:
        raise _Decline("per_user_limit", _limit_body(show, n))

    reservation_id = uuid.uuid4()
    result = await conn.execute(
        """
        UPDATE seats
           SET status = 'confirmed', reservation_id = $3, user_id = $4, updated_at = now()
         WHERE show_id = $1 AND label = ANY($2::text[]) AND status = 'available'
        """,
        show.id, seats, reservation_id, user_id,
    )
    if result != f"UPDATE {n}":  # impossible while we hold the locks; never paper over it
        raise RuntimeError(f"guarded seat update touched {result!r}, expected {n} rows")

    amount = show.price_paise * n  # integer paise, never float
    row = await conn.fetchrow(
        """
        INSERT INTO reservations (id, show_id, user_id, seats, amount_paise, status)
        VALUES ($1, $2, $3, $4, $5, 'confirmed')
        RETURNING created_at
        """,
        reservation_id, show.id, user_id, seats, amount,
    )
    return {
        "reservation_id": str(reservation_id),
        "show_id": str(show.id),
        "user_id": user_id,
        "seats": seats,
        "amount_paise": amount,
        "status": "confirmed",
        "created_at": row["created_at"].isoformat(),
    }


async def _load_key(conn: asyncpg.Connection, show_id: uuid.UUID, user_id: str, key: str) -> asyncpg.Record | None:
    """
        The join brings the reservation's *current* status, so a replay after a cancel can say so.
    """

    return await conn.fetchrow(
        """
        SELECT k.request_hash, k.status_code, k.response,
               r.status AS current_status, r.cancelled_at
          FROM idempotency_keys k
          LEFT JOIN reservations r ON r.id = k.reservation_id
         WHERE k.user_id = $1 AND k.show_id = $2 AND k.key = $3
        """,
        user_id, show_id, key,
    )


async def _record_decline(
    conn: asyncpg.Connection,
    show: ShowMeta,
    user_id: str,
    key: str,
    req_hash: str,
    reason: str,
    body: dict[str, Any],
) -> Outcome:
    """
        Store a decline under the key (single autocommit INSERT). If someone else already
        claimed the key, their decision wins and we replay it instead.
    """

    inserted = await conn.fetchval(
        """
        INSERT INTO idempotency_keys (user_id, key, request_hash, show_id, status_code, response)
        VALUES ($1, $2, $3, $4, 409, $5::jsonb)
        ON CONFLICT (user_id, show_id, key) DO NOTHING
        RETURNING true
        """,
        user_id, key, req_hash, show.id, json.dumps(body),
    )
    if inserted:
        return Outcome(status=409, body=body, reason=reason)
    return _replay(await _load_key(conn, show.id, user_id, key), req_hash)


def _replay(existing: asyncpg.Record, req_hash: str) -> Outcome:
    if existing["request_hash"] != req_hash:
        return Outcome(
            status=409,
            body={
                "error": "idempotency_key_reused",
                "message": "this idempotency key was already used with a different request body",
            },
            reason="idempotency_key_mismatch",
        )
    original = json.loads(existing["response"]) if isinstance(existing["response"], str) else existing["response"]
    # A replayed success returns 200, not 201: nothing new was created. This keeps
    # "exactly one 201 per seat" true even when the winner retries.
    status = 200 if existing["status_code"] == 201 else existing["status_code"]
    # Same reservation, current status: if it was cancelled since, the replay says so rather
    # than echoing a stale "confirmed". A retry never re-books a cancelled reservation.
    if existing["status_code"] == 201 and existing["current_status"] == "cancelled":
        original = {
            **original,
            "status": "cancelled",
            "cancelled_at": existing["cancelled_at"].isoformat() if existing["cancelled_at"] else None,
        }
    return Outcome(
        status=status,
        body=original,
        reason="idempotent_replay",
        replayed=True,
        headers={"Idempotent-Replayed": "true"},
    )


def _record_metrics(show: ShowMeta, seats: list[str], outcome: Outcome) -> None:
    sid = str(show.id)
    request_context()["outcome"] = outcome.reason
    if outcome.reason == "confirmed":
        RESERVATIONS_CONFIRMED.labels(sid).inc()
        SEATS_CONFIRMED.labels(sid).inc(len(seats))
        log.info(
            "reservation_confirmed",
            extra={
                "show_id": sid,
                "reservation_id": outcome.body["reservation_id"],
                "seats": seats,
                "amount_paise": outcome.body["amount_paise"],
            },
        )
    else:
        RESERVATIONS_DECLINED.labels(sid, outcome.reason).inc()


# --------------------------------------------------------------------------------------
# Cancel
# --------------------------------------------------------------------------------------


@dataclass
class CancelResult:
    status: int
    body: dict[str, Any]


async def cancel(db: Database, reservation_id: uuid.UUID, user_id: str) -> CancelResult:
    from .errors import forbidden, not_found  # local import keeps the core import-light

    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            async with db.acquire() as conn:
                async with conn.transaction():
                    res = await conn.fetchrow(
                        "SELECT id, show_id, user_id, seats, amount_paise, status, created_at, cancelled_at "
                        "FROM reservations WHERE id = $1 FOR UPDATE",
                        reservation_id,
                    )
                    if res is None:
                        raise not_found("reservation_not_found", f"reservation {reservation_id} does not exist")
                    if res["user_id"] != user_id:
                        raise forbidden("not_reservation_owner", "only the user who made a reservation can cancel it")
                    if res["status"] == "cancelled":
                        # Cancelling twice is a no-op, not an error.
                        return CancelResult(200, {**_reservation_view(res), "already_cancelled": True})

                    # Same global lock order as reserve: seats by label, then the quota row.
                    await conn.execute(
                        "SELECT 1 FROM seats WHERE show_id = $1 AND reservation_id = $2 ORDER BY label FOR UPDATE",
                        res["show_id"], reservation_id,
                    )
                    await conn.execute(
                        "SELECT 1 FROM user_show_quota WHERE show_id = $1 AND user_id = $2 FOR UPDATE",
                        res["show_id"], user_id,
                    )
                    # Release ONLY seats that still point at this reservation. A seat that
                    # has since been sold to someone else carries their reservation_id and
                    # is untouched - a cancel can never resurrect another user's seat.
                    released = await conn.fetch(
                        """
                        UPDATE seats
                           SET status = 'available', reservation_id = NULL, user_id = NULL, updated_at = now()
                         WHERE show_id = $1 AND reservation_id = $2
                        RETURNING label
                        """,
                        res["show_id"], reservation_id,
                    )
                    await conn.execute(
                        "UPDATE user_show_quota SET seats_held = seats_held - $3 WHERE show_id = $1 AND user_id = $2",
                        res["show_id"], user_id, len(released),
                    )
                    updated = await conn.fetchrow(
                        "UPDATE reservations SET status = 'cancelled', cancelled_at = now() WHERE id = $1 "
                        "RETURNING id, show_id, user_id, seats, amount_paise, status, created_at, cancelled_at",
                        reservation_id,
                    )
            break
        except _RETRYABLE as exc:
            RESERVE_RETRIES.labels(exc.sqlstate).inc()
            if attempt == _MAX_ATTEMPTS:
                raise
            await asyncio.sleep(random.uniform(0.005, 0.05) * attempt)

    sid = str(updated["show_id"])
    RESERVATIONS_CANCELLED.labels(sid).inc()
    SEATS_RELEASED.labels(sid).inc(len(released))
    request_context()["outcome"] = "cancelled"
    log.info(
        "reservation_cancelled",
        extra={"show_id": sid, "reservation_id": str(reservation_id), "seats_released": [r["label"] for r in released]},
    )
    return CancelResult(200, {**_reservation_view(updated), "seats_released": [r["label"] for r in released]})


async def get_reservation(db: Database, reservation_id: uuid.UUID) -> asyncpg.Record | None:
    async with db.acquire() as conn:
        return await conn.fetchrow(
            "SELECT id, show_id, user_id, seats, amount_paise, status, created_at, cancelled_at "
            "FROM reservations WHERE id = $1",
            reservation_id,
        )


def _reservation_view(r: asyncpg.Record) -> dict[str, Any]:
    return {
        "reservation_id": str(r["id"]),
        "show_id": str(r["show_id"]),
        "user_id": r["user_id"],
        "seats": list(r["seats"]),
        "amount_paise": r["amount_paise"],
        "status": r["status"],
        "created_at": r["created_at"].isoformat(),
        "cancelled_at": r["cancelled_at"].isoformat() if r["cancelled_at"] else None,
    }


reservation_view = _reservation_view
