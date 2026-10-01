"""Shows: creation, the read model with reconciliation, and an immutable metadata cache."""

from __future__ import annotations

import uuid
from collections import OrderedDict
from dataclasses import dataclass

import asyncpg

from .db import Database
from .errors import not_found


@dataclass(frozen=True)
class ShowMeta:
    id: uuid.UUID
    name: str
    price_paise: int
    per_user_limit: int
    total_seats: int
    seat_labels: frozenset[str]


class ShowCache:
    """Show metadata never changes after creation (seat list, price, limit), so it is
    safe to cache per process and lets us validate seat labels without a DB round trip."""

    def __init__(self, capacity: int = 512) -> None:
        self._items: OrderedDict[uuid.UUID, ShowMeta] = OrderedDict()
        self._capacity = capacity

    def put(self, meta: ShowMeta) -> None:
        self._items[meta.id] = meta
        self._items.move_to_end(meta.id)
        while len(self._items) > self._capacity:
            self._items.popitem(last=False)

    async def get(self, db: Database, show_id: uuid.UUID) -> ShowMeta:
        meta = self._items.get(show_id)
        if meta is not None:
            return meta
        async with db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, name, price_paise, per_user_limit, total_seats FROM shows WHERE id = $1", show_id
            )
            if row is None:
                raise not_found("show_not_found", f"show {show_id} does not exist")
            labels = await conn.fetchval("SELECT array_agg(label) FROM seats WHERE show_id = $1", show_id)
        meta = ShowMeta(
            id=row["id"],
            name=row["name"],
            price_paise=row["price_paise"],
            per_user_limit=row["per_user_limit"],
            total_seats=row["total_seats"],
            seat_labels=frozenset(labels or []),
        )
        self.put(meta)
        return meta


show_cache = ShowCache()


async def create_show(
    db: Database, *, name: str, seats: list[str], price_paise: int, per_user_limit: int
) -> dict:
    show_id = uuid.uuid4()
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO shows (id, name, price_paise, per_user_limit, total_seats) VALUES ($1, $2, $3, $4, $5)",
                show_id, name, price_paise, per_user_limit, len(seats),
            )
            # One statement for all seats; WITH ORDINALITY keeps the caller's order for display.
            await conn.execute(
                """
                INSERT INTO seats (show_id, label, position)
                SELECT $1, t.label, t.ord
                FROM unnest($2::text[]) WITH ORDINALITY AS t(label, ord)
                """,
                show_id, seats,
            )
    show_cache.put(
        ShowMeta(show_id, name, price_paise, per_user_limit, len(seats), frozenset(seats))
    )
    return {
        "id": str(show_id),
        "name": name,
        "price_paise": price_paise,
        "per_user_limit": per_user_limit,
        "total_seats": len(seats),
        "counts": {"available": len(seats), "held": 0, "confirmed": 0, "total_seats": len(seats)},
        "seats": [{"seat": s, "status": "available"} for s in seats],
    }


# All reads for the show view run in ONE read-only REPEATABLE READ transaction, i.e. one
# snapshot. Mid-burst the counts, the per-seat list and the cross-checks all describe the
# same instant, so the invariant can be checked "during" the burst, not only after it.
async def get_show_state(db: Database, show_id: uuid.UUID, *, include_owners: bool) -> dict:
    async with db.acquire() as conn:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            show = await conn.fetchrow(
                "SELECT id, name, price_paise, per_user_limit, total_seats, created_at FROM shows WHERE id = $1",
                show_id,
            )
            if show is None:
                raise not_found("show_not_found", f"show {show_id} does not exist")
            seats = await conn.fetch(
                "SELECT label, status, reservation_id, user_id FROM seats WHERE show_id = $1 ORDER BY position",
                show_id,
            )
            cross = await conn.fetchrow(
                """
                SELECT
                  (SELECT coalesce(sum(cardinality(seats)), 0) FROM reservations
                     WHERE show_id = $1 AND status = 'confirmed')            AS seats_in_active_reservations,
                  (SELECT count(*) FROM reservations
                     WHERE show_id = $1 AND status = 'confirmed')            AS active_reservations,
                  (SELECT coalesce(sum(seats_held), 0) FROM user_show_quota
                     WHERE show_id = $1)                                       AS seats_in_user_quotas
                """,
                show_id,
            )

    counts = {"available": 0, "held": 0, "confirmed": 0}
    seat_list = []
    for s in seats:
        counts[s["status"]] += 1
        item = {"seat": s["label"], "status": s["status"]}
        if include_owners and s["reservation_id"] is not None:
            item["reservation_id"] = str(s["reservation_id"])
            item["user_id"] = s["user_id"]
        seat_list.append(item)

    owned = counts["held"] + counts["confirmed"]
    total = show["total_seats"]
    checks = {
        "available_plus_held_plus_confirmed_equals_total": counts["available"] + owned == total,
        "seat_rows_equal_total": len(seats) == total,
        "owned_seats_equal_active_reservation_seats": owned == cross["seats_in_active_reservations"],
        "owned_seats_equal_user_quota_sum": owned == cross["seats_in_user_quotas"],
    }
    return {
        "id": str(show["id"]),
        "name": show["name"],
        "price_paise": show["price_paise"],
        "per_user_limit": show["per_user_limit"],
        "total_seats": total,
        "created_at": show["created_at"].isoformat(),
        "counts": {**counts, "total_seats": total},
        "reconciliation": {
            "ok": all(checks.values()),
            "checks": checks,
            "active_reservations": cross["active_reservations"],
            "seats_in_active_reservations": cross["seats_in_active_reservations"],
            "seats_in_user_quotas": cross["seats_in_user_quotas"],
        },
        "seats": seat_list,
    }


async def seat_state_snapshot(conn: asyncpg.Connection, max_shows: int) -> list[dict]:
    """Per-show seat counts for the metrics collector (most recent `max_shows` shows)."""
    rows = await conn.fetch(
        """
        WITH recent AS (
            SELECT id, name, total_seats FROM shows ORDER BY created_at DESC LIMIT $1
        )
        SELECT r.id, r.name, r.total_seats,
               count(*) FILTER (WHERE s.status = 'available') AS available,
               count(*) FILTER (WHERE s.status = 'held')      AS held,
               count(*) FILTER (WHERE s.status = 'confirmed') AS confirmed,
               count(s.label)                                 AS seat_rows,
               (SELECT coalesce(sum(cardinality(seats)), 0) FROM reservations
                  WHERE show_id = r.id AND status = 'confirmed') AS reserved_seats
        FROM recent r
        LEFT JOIN seats s ON s.show_id = r.id
        GROUP BY r.id, r.name, r.total_seats
        """,
        max_shows,
    )
    out = []
    for r in rows:
        owned = r["held"] + r["confirmed"]
        out.append(
            {
                "show_id": str(r["id"]),
                "show_name": r["name"],
                "available": r["available"],
                "held": r["held"],
                "confirmed": r["confirmed"],
                "total": r["total_seats"],
                "ok": r["available"] + owned == r["total_seats"] == r["seat_rows"]
                and owned == r["reserved_seats"],
            }
        )
    return out
