"""Focused regression checks for replay-after-cancel and decline precedence.

Needs direct database access (it holds a seat lock to force the locked path), so it runs
against a local stack, not a deployment:

    docker compose up -d
    DATABASE_URL=postgresql://postgres:postgres@localhost:5432/seats python scripts/check_edge_cases.py

1. Replay after cancel shows status "cancelled" and does not re-book.
2a. Fast path: over-limit request that also wants a taken seat -> seat_taken.
2b. Locked path (forced): user at their limit asks for a seat that is available at the
    fast check but gets sold while the request waits on the seat lock -> seat_taken.
"""
import asyncio
import json
import os
import sys
import uuid

import aiohttp
import asyncpg

BASE = os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")
ADMIN = {"X-Admin-Key": os.environ.get("ADMIN_KEY", "dev-admin-key")}
DSN = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/seats")
results = []
RUN = uuid.uuid4().hex[:6]


def check(name, ok, detail=""):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""))


async def main():
    async with aiohttp.ClientSession() as s:
        async def call(method, path, body=None, headers=None):
            async with s.request(method, BASE + path, json=body, headers=headers) as r:
                return r.status, await r.json(), r.headers

        _, show, _ = await call("POST", "/shows", {"name": "edge-case-checks", "seats": [f"A{i}" for i in range(1, 9)],
                                                  "price_paise": 25000}, ADMIN)
        sid = show["id"]
        _, toks, _ = await call("POST", "/auth/token", {"user_ids": [f"{u}-{RUN}" for u in ("alice", "bob", "lim", "other")]}, ADMIN)
        auth = {u.rsplit("-", 1)[0]: {"Authorization": f"Bearer {t}"} for u, t in toks["tokens"].items()}

        # 1. replay after cancel
        st, res, _ = await call("POST", f"/shows/{sid}/reserve", {"seats": ["A1"], "idempotency_key": "k1"}, auth["alice"])
        rid = res["reservation_id"]
        st_c, _, _ = await call("POST", f"/reservations/{rid}/cancel", headers=auth["alice"])
        st_r, rep, h = await call("POST", f"/shows/{sid}/reserve", {"seats": ["A1"], "idempotency_key": "k1"}, auth["alice"])
        _, state, _ = await call("GET", f"/shows/{sid}")
        a1 = next(x for x in state["seats"] if x["seat"] == "A1")
        check("replay after cancel: 200 + Idempotent-Replayed, same reservation, status cancelled",
              st == 201 and st_c == 200 and st_r == 200 and h.get("Idempotent-Replayed") == "true"
              and rep["reservation_id"] == rid and rep["status"] == "cancelled" and rep.get("cancelled_at"),
              f"reserve {st}, cancel {st_c}, replay {st_r} status={rep.get('status')}")
        check("replay after cancel does not re-book the seat", a1["status"] == "available"
              and state["reconciliation"]["ok"], f"A1 {a1['status']}")
        st_r2, rep2, _ = await call("POST", f"/shows/{sid}/reserve", {"seats": ["A2"], "idempotency_key": "k2"}, auth["bob"])
        st_r3, rep3, _ = await call("POST", f"/shows/{sid}/reserve", {"seats": ["A2"], "idempotency_key": "k2"}, auth["bob"])
        check("replay of a live reservation is unchanged (no extra fields)", st_r3 == 200 and rep3 == rep2,
              f"{sorted(rep3) == sorted(rep2)}")

        # 2a. fast path precedence: A2 is bob's; alice asks for 5 seats incl. A2 (limit 4)
        st, body, _ = await call("POST", f"/shows/{sid}/reserve",
                                 {"seats": ["A2", "A3", "A4", "A5", "A6"], "idempotency_key": "k3"}, auth["alice"])
        check("fast path: over-limit + taken seat -> seat_taken", st == 409 and body["error"] == "seat_taken",
              f"{st} {body.get('error')}")

        # 2b. locked path precedence (forced). "lim" takes 4 seats -> at limit.
        st, _, _ = await call("POST", f"/shows/{sid}/reserve", {"seats": ["A3", "A4", "A5", "A6"],
                                                                 "idempotency_key": "k4"}, auth["lim"])
        assert st == 201, st
        conn = await asyncpg.connect(DSN)
        tx = conn.transaction()
        await tx.start()
        await conn.execute("SELECT 1 FROM seats WHERE show_id=$1 AND label='A7' FOR UPDATE", uuid.UUID(sid))
        # A7 is still available at "lim"'s fast check, so the request goes into the transaction
        # and waits on A7's lock.
        pending = asyncio.create_task(call("POST", f"/shows/{sid}/reserve",
                                           {"seats": ["A7"], "idempotency_key": "k5"}, auth["lim"]))
        await asyncio.sleep(1.0)
        # Sell A7 to "other" consistently (seat, reservation, quota), then release the lock.
        r_other = uuid.uuid4()
        await conn.execute("UPDATE seats SET status='confirmed', reservation_id=$2, user_id=$3 "
                           "WHERE show_id=$1 AND label='A7'", uuid.UUID(sid), r_other, f"other-{RUN}")
        await conn.execute("INSERT INTO reservations (id, show_id, user_id, seats, amount_paise, status) "
                           "VALUES ($1,$2,$3,ARRAY['A7'],25000,'confirmed')", r_other, uuid.UUID(sid), f"other-{RUN}")
        await conn.execute("INSERT INTO user_show_quota (show_id, user_id, seats_held) VALUES ($1,$2,1)",
                           uuid.UUID(sid), f"other-{RUN}")
        await tx.commit()
        await conn.close()
        st, body, _ = await pending
        check("locked path: user at limit, seat sold while waiting -> seat_taken", st == 409
              and body["error"] == "seat_taken", f"{st} {body.get('error')}")

        _, state, _ = await call("GET", f"/shows/{sid}")
        check("reconciliation ok at the end", state["reconciliation"]["ok"], json.dumps(state["counts"]))
    return 0 if all(results) else 1


sys.exit(asyncio.run(main()))
