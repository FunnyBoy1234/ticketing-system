# /// script
# requires-python = ">=3.10"
# dependencies = ["aiohttp>=3.9"]
# ///
"""On-sale stampede against a live deployment, with correctness checks.

    ./burst.sh https://your-app.example.com            # or: python scripts/burst.py URL

What it fires (default ~20,000 reserve requests, all released at the same instant):
  * hot-seat storm   - H hot seats, each grabbed by S distinct users at once
  * duplicate retries- a slice of the storm re-sent concurrently with the SAME key and body
  * key misuse       - same user + same key, different seats, sent concurrently
  * per-user limit   - L users each fire 10 parallel reserves on a limit-4 show
  * spoofing         - body carries someone else's user_id
  * general stampede - everyone else, 1-3 seats each, biased to the front rows
Then: sequential retries, owner/non-owner cancels, re-booking released seats, and a
cancel-vs-rebook race. GET /shows/{id} is sampled throughout to check the invariant
mid-burst, and /metrics is scraped before/after to reconcile counters and gauges.

Exit code 0 only if every check passes. A JSON report is written to burst-report.json.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import statistics
import string
import sys
import time
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any

import aiohttp


# ----------------------------------------------------------------------------------------
# plumbing
# ----------------------------------------------------------------------------------------


@dataclass
class Req:
    kind: str
    user: str
    seats: list[str]
    key: str
    group: str = ""
    spoof_as: str | None = None
    # filled in after the call
    status: int = 0
    body: Any = None
    replayed: bool = False
    latency: float = 0.0
    error: str | None = None

    @property
    def outcome(self) -> str:
        if self.error:
            return "transport_error"
        if self.status >= 500:
            return "server_error_5xx"
        if self.replayed:
            return "idempotent_replay"
        if self.status == 201:
            return "confirmed"
        if self.status == 409 and isinstance(self.body, dict):
            code = self.body.get("error")
            return {"idempotency_key_reused": "idempotency_key_mismatch"}.get(code, code)
        return f"http_{self.status}"


class Api:
    def __init__(self, base: str, admin_key: str, concurrency: int, timeout: float) -> None:
        self.base = base.rstrip("/")
        self.admin = {"X-Admin-Key": admin_key}
        self.concurrency = concurrency
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.session: aiohttp.ClientSession | None = None
        self.side: aiohttp.ClientSession | None = None  # separate pool for the sampler
        self.slots = asyncio.Semaphore(concurrency)
        self.tokens: dict[str, str] = {}

    async def __aenter__(self) -> "Api":
        connector = aiohttp.TCPConnector(limit=self.concurrency, limit_per_host=self.concurrency, ttl_dns_cache=300)
        self.session = aiohttp.ClientSession(connector=connector, timeout=self.timeout)
        self.side = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=4), timeout=self.timeout)
        return self

    async def __aexit__(self, *exc) -> None:
        assert self.session and self.side
        await self.session.close()
        await self.side.close()

    async def call(self, method: str, path: str, *, json_body: Any = None, headers: dict | None = None,
                   side: bool = False):
        session = self.side if side else self.session
        assert session
        async with session.request(method, self.base + path, json=json_body, headers=headers) as resp:
            text = await resp.text()
            try:
                body = json.loads(text) if text else None
            except json.JSONDecodeError:
                body = text
            return resp.status, body, resp.headers

    def auth(self, user: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.tokens[user]}"}

    async def mint(self, users: list[str]) -> None:
        for i in range(0, len(users), 2000):
            chunk = users[i : i + 2000]
            status, body, _ = await self.call("POST", "/auth/token", json_body={"user_ids": chunk}, headers=self.admin)
            if status != 200:
                raise SystemExit(f"token mint failed: {status} {body}")
            self.tokens.update(body["tokens"])

    async def reserve(self, show_id: str, r: Req) -> None:
        payload: dict[str, Any] = {"seats": r.seats, "idempotency_key": r.key}
        if r.spoof_as:
            payload["user_id"] = r.spoof_as
        async with self.slots:  # latency below = time on the wire, not time queued in this client
            t0 = time.perf_counter()
            try:
                status, body, headers = await self.call(
                    "POST", f"/shows/{show_id}/reserve", json_body=payload, headers=self.auth(r.user)
                )
                r.status, r.body = status, body
                r.replayed = headers.get("Idempotent-Replayed", "").lower() == "true"
            except Exception as exc:  # noqa: BLE001 - record and keep going
                r.error = f"{exc.__class__.__name__}: {exc}"
            r.latency = time.perf_counter() - t0

    async def show(self, show_id: str, admin: bool = False, side: bool = False) -> dict:
        status, body, _ = await self.call("GET", f"/shows/{show_id}", headers=self.admin if admin else None,
                                          side=side)
        if status != 200:
            raise RuntimeError(f"GET show -> {status} {body}")
        return body

    async def metrics(self) -> dict[tuple[str, frozenset], float]:
        assert self.side
        async with self.side.get(self.base + "/metrics") as resp:
            text = await resp.text()
        out: dict[tuple[str, frozenset], float] = {}
        for line in text.splitlines():
            if not line or line.startswith("#"):
                continue
            name_labels, _, value = line.rpartition(" ")
            if "{" in name_labels:
                name, _, rest = name_labels.partition("{")
                labels = frozenset(
                    tuple(kv.split("=", 1)) for kv in _split_labels(rest.rstrip("}"))
                )
            else:
                name, labels = name_labels, frozenset()
            try:
                out[(name, labels)] = float(value)
            except ValueError:
                pass
        return out


def _split_labels(raw: str) -> list[str]:
    parts, cur, in_q = [], [], False
    for ch in raw:
        if ch == '"':
            in_q = not in_q
            continue
        if ch == "," and not in_q:
            parts.append("".join(cur))
            cur = []
            continue
        cur.append(ch)
    if cur:
        parts.append("".join(cur))
    return parts


def metric(m: dict, name: str, **labels: str) -> float:
    want = frozenset(labels.items())
    return sum(v for (n, ls), v in m.items() if n == name and want <= ls)


def pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    k = min(len(values) - 1, max(0, int(round(p / 100 * (len(values) - 1)))))
    return values[k]


class Checks:
    def __init__(self) -> None:
        self.items: list[tuple[str, bool, str]] = []

    def add(self, name: str, ok: bool, detail: str = "") -> None:
        self.items.append((name, bool(ok), detail))

    @property
    def ok(self) -> bool:
        return all(ok for _, ok, _ in self.items)


# ----------------------------------------------------------------------------------------
# scenario
# ----------------------------------------------------------------------------------------


def seat_map(rows: int, per_row: int) -> list[str]:
    letters = string.ascii_uppercase
    names = [letters[i] if i < 26 else letters[i // 26 - 1] + letters[i % 26] for i in range(rows)]
    return [f"{r}{n}" for r in names for n in range(1, per_row + 1)]


def build_plan(args, seats: list[str], run: str, rng: random.Random) -> tuple[list[Req], dict]:
    per_row = args.seats_per_row
    rows = [seats[i : i + per_row] for i in range(0, len(seats), per_row)]
    hot = rows[0][: args.hot_seats]
    limit_block = [s for row in rows[-max(1, (args.limit_users * 10 + per_row - 1) // per_row):] for s in row]
    general = [s for s in seats if s not in set(hot) | set(limit_block)]
    front = general[: max(1, len(general) // 5)]  # the "good" seats everyone wants

    plan: list[Req] = []
    key = lambda: uuid.uuid4().hex  # noqa: E731

    # hot-seat storm: dedicated users, one request each, unique keys
    hot_users = [f"{run}-hot{i}" for i in range(args.hot_seats * args.storm_size)]
    storm: list[Req] = []
    for h_idx, seat in enumerate(hot):
        for j in range(args.storm_size):
            storm.append(Req("hot", hot_users[h_idx * args.storm_size + j], [seat], key(), group=seat))
    plan += storm

    # concurrent duplicate retries of storm requests (same user, key, body)
    dups = rng.sample(storm, k=int(len(storm) * args.dup_fraction))
    for r in dups:
        plan.append(Req("dup", r.user, list(r.seats), r.key, group=r.key))
        r.group = r.key  # pair them up for checking
        r.kind = "hot_dup_original"

    # per-user limit: each limit user fires 10 parallel single-seat requests on its own seats
    limit_users = [f"{run}-lim{i}" for i in range(args.limit_users)]
    for i, u in enumerate(limit_users):
        for s in limit_block[i * 10 : i * 10 + 10]:
            plan.append(Req("limit", u, [s], key(), group=u))

    general_users = [f"{run}-u{i}" for i in range(args.users)]

    # same key, different seats, fired together: exactly one may proceed
    for i in range(args.key_misuse_pairs):
        u = rng.choice(general_users)
        k = key()
        a, b = rng.sample(general, 2)
        plan.append(Req("key_misuse", u, [a], k, group=f"km{i}"))
        plan.append(Req("key_misuse", u, [b], k, group=f"km{i}"))

    # spoofing: body claims to be somebody else
    for _ in range(args.spoof):
        u, other = rng.sample(general_users, 2)
        plan.append(Req("spoof", u, [rng.choice(general)], key(), spoof_as=other))

    # general stampede fills the rest
    remaining = max(0, args.requests - len(plan))
    for _ in range(remaining):
        n = rng.choices([1, 2, 3], weights=[70, 20, 10])[0]
        pool = front if rng.random() < 0.5 else general
        plan.append(Req("general", rng.choice(general_users), rng.sample(pool, min(n, len(pool))), key()))

    rng.shuffle(plan)
    users = hot_users + limit_users + general_users
    return plan, {"hot": hot, "limit_block": limit_block, "general": general, "users": users,
                  "limit_users": limit_users, "general_users": general_users}


async def run(args) -> int:
    rng = random.Random(args.seed)
    run_id = "b" + uuid.uuid4().hex[:6]
    checks = Checks()
    report: dict[str, Any] = {"run_id": run_id, "base_url": args.base_url}

    async with Api(args.base_url, args.admin_key, args.concurrency, args.timeout) as api:
        # ---- 0. wait for the service (cold starts) ---------------------------------------
        t0 = time.time()
        while True:
            try:
                status, body, _ = await api.call("GET", "/readyz")
                if status == 200:
                    break
            except Exception:  # noqa: BLE001
                pass
            if time.time() - t0 > args.ready_timeout:
                print(f"service not ready after {args.ready_timeout}s", file=sys.stderr)
                return 2
            await asyncio.sleep(2)
        print(f"[ready] {args.base_url} ready after {time.time() - t0:.1f}s")

        # ---- 1. fresh show + users ---------------------------------------------------------
        seats = seat_map(args.rows, args.seats_per_row)
        status, show, _ = await api.call(
            "POST", "/shows", headers=api.admin,
            json_body={"name": f"burst-{run_id}", "seats": seats, "price_paise": args.price_paise,
                       "per_user_limit": args.limit},
        )
        if status != 201:
            print(f"create show failed: {status} {show}", file=sys.stderr)
            return 2
        show_id = show["id"]
        plan, meta = build_plan(args, seats, run_id, rng)
        await api.mint(meta["users"])
        print(f"[setup] show {show_id}: {len(seats)} seats, limit {args.limit}; "
              f"{len(meta['users'])} users; {len(plan)} reserve requests queued")
        m_before = await api.metrics()

        # ---- 2. the stampede ---------------------------------------------------------------
        samples: list[dict] = []
        stop = asyncio.Event()

        async def sampler() -> None:
            while not stop.is_set():
                try:
                    st = await api.show(show_id, side=True)
                    c = st["counts"]
                    samples.append({
                        "t": time.time(),
                        "sum_ok": c["available"] + c["held"] + c["confirmed"] == c["total_seats"],
                        "recon_ok": st["reconciliation"]["ok"],
                        "counts": c,
                    })
                except Exception as exc:  # noqa: BLE001
                    samples.append({"t": time.time(), "error": repr(exc)})
                try:
                    await asyncio.wait_for(stop.wait(), timeout=args.sample_every)
                except asyncio.TimeoutError:
                    pass

        gate = asyncio.Event()

        async def fire(r: Req) -> None:
            await gate.wait()
            await api.reserve(show_id, r)

        tasks = [asyncio.create_task(fire(r)) for r in plan]
        sampler_task = asyncio.create_task(sampler())
        await asyncio.sleep(0.2)
        print(f"[burst] releasing {len(plan)} requests at once (client concurrency {args.concurrency}) ...")
        t_burst = time.perf_counter()
        gate.set()
        done = 0
        for fut in asyncio.as_completed(tasks):
            await fut
            done += 1
            if done % 2000 == 0:
                print(f"        {done}/{len(plan)} done, {time.perf_counter() - t_burst:.1f}s")
        burst_s = time.perf_counter() - t_burst
        stop.set()
        await sampler_task

        after = await api.show(show_id, admin=True)

        # ---- 3. outcome distribution ------------------------------------------------------
        dist = Counter(r.outcome for r in plan)
        lat = [r.latency for r in plan if not r.error]
        print(f"\n[burst] {len(plan)} requests in {burst_s:.1f}s ({len(plan) / burst_s:.0f} req/s)  "
              f"latency p50 {pct(lat, 50) * 1000:.0f}ms  p95 {pct(lat, 95) * 1000:.0f}ms  p99 {pct(lat, 99) * 1000:.0f}ms")
        print("\n  outcome                       count")
        for k, v in sorted(dist.items(), key=lambda kv: -kv[1]):
            print(f"  {k:<28} {v:>6}")
        by_kind = defaultdict(Counter)
        for r in plan:
            by_kind[r.kind][r.outcome] += 1
        report["burst"] = {"requests": len(plan), "seconds": round(burst_s, 2), "distribution": dict(dist),
                           "by_scenario": {k: dict(v) for k, v in by_kind.items()},
                           "latency_ms": {"p50": pct(lat, 50) * 1000, "p95": pct(lat, 95) * 1000,
                                          "p99": pct(lat, 99) * 1000}}

        # ---- 4. correctness checks on the burst ---------------------------------------------
        fivexx = [r for r in plan if r.status >= 500]
        transport = [r for r in plan if r.error]
        checks.add("zero 5xx during burst", not fivexx, f"{len(fivexx)} responses >= 500")
        checks.add("zero transport errors", not transport,
                   f"{len(transport)} errors" + (f", e.g. {transport[0].error}" if transport else ""))
        unexpected = Counter(r.outcome for r in plan if r.outcome.startswith("http_"))
        checks.add("no unexpected status codes", not unexpected, str(dict(unexpected)))

        # ledger of confirmed reservations from what the API told us
        confirmed: dict[str, dict] = {}
        for r in plan:
            if r.status in (200, 201) and isinstance(r.body, dict) and r.body.get("status") == "confirmed":
                confirmed[r.body["reservation_id"]] = r.body
        seat_owner: dict[str, set[str]] = defaultdict(set)
        for rid, b in confirmed.items():
            for s in b["seats"]:
                seat_owner[s].add(rid)
        double = {s: ids for s, ids in seat_owner.items() if len(ids) > 1}
        checks.add("no seat confirmed twice (client ledger)", not double, f"{len(double)} seats with >1 reservation")

        server_seats = {s["seat"]: s for s in after["seats"]}
        mismatch = []
        for rid, b in confirmed.items():
            for s in b["seats"]:
                if server_seats[s].get("reservation_id") != rid or server_seats[s]["status"] != "confirmed":
                    mismatch.append(s)
        phantom = [s for s, v in server_seats.items()
                   if v["status"] != "available" and v.get("reservation_id") not in confirmed]
        checks.add("every 201 is reflected in show state", not mismatch, f"{len(mismatch)} seats disagree")
        checks.add("no confirmed seat without a 201", not phantom, f"{len(phantom)} phantom seats")

        hot_detail = []
        hot_ok = True
        for seat in meta["hot"]:
            reqs = [r for r in plan if r.kind in ("hot", "hot_dup_original", "dup") and r.seats == [seat]]
            wins = [r for r in reqs if r.status == 201]
            # everyone else: a clean 409, or (only the winner's own duplicate) a 200 replay of the win
            losers = [r for r in reqs if r.status != 201]
            clean = all(r.status == 409 or (r.replayed and r.status == 200) for r in losers)
            replays_of_win = sum(r.status == 200 for r in losers)
            hot_ok &= len(wins) == 1 and clean
            hot_detail.append(f"{seat}: {len(wins)}x201 / {sum(r.status == 409 for r in losers)}x409"
                              + (f" / {replays_of_win}x200 replay" if replays_of_win else ""))
        checks.add(f"hot seats: exactly one 201 each, rest 409 ({args.storm_size} users per seat)",
                   hot_ok, "; ".join(hot_detail))

        # duplicates fired concurrently with the same key + body
        pairs = defaultdict(list)
        for r in plan:
            if r.kind in ("dup", "hot_dup_original"):
                pairs[r.group].append(r)
        bad_pairs = 0
        for rs in pairs.values():
            fresh = [r for r in rs if not r.replayed]
            ids = {r.body.get("reservation_id") for r in rs if isinstance(r.body, dict) and r.body.get("reservation_id")}
            same_answer = len({json.dumps({k: v for k, v in r.body.items() if k != "request_id"}, sort_keys=True)
                               for r in rs if isinstance(r.body, dict)}) == 1
            if len(fresh) != 1 or len(ids) > 1 or not same_answer:
                bad_pairs += 1
        checks.add("concurrent same-key retries: one decision, the other replays it",
                   bad_pairs == 0, f"{len(pairs)} pairs, {bad_pairs} inconsistent")

        km = defaultdict(list)
        for r in plan:
            if r.kind == "key_misuse":
                km[r.group].append(r)
        km_bad = sum(1 for rs in km.values()
                     if sum(r.outcome == "idempotency_key_mismatch" for r in rs) != 1
                     or sum(r.status == 201 for r in rs) > 1)
        checks.add("same key + different seats: exactly one 409 idempotency_key_reused per pair",
                   km_bad == 0, f"{len(km)} pairs, {km_bad} wrong")

        per_user = Counter(v["user_id"] for v in after["seats"] if v.get("user_id"))
        over = {u: n for u, n in per_user.items() if n > args.limit}
        checks.add(f"no user holds more than {args.limit} seats (server state)", not over, f"{len(over)} users over")
        lim_counts = Counter(per_user.get(u, 0) for u in meta["limit_users"])
        checks.add("limit users (10 parallel requests each) end with <= limit",
                   all(n <= args.limit for n in lim_counts.elements()),
                   f"seats held -> users: {dict(sorted(lim_counts.items()))}")

        spoof_bad = [r for r in plan if r.kind == "spoof" and r.status == 201
                     and (r.body.get("user_id") != r.user or server_seats[r.seats[0]].get("user_id") != r.user)]
        checks.add("spoofed body user_id is ignored (token identity wins)", not spoof_bad, f"{len(spoof_bad)} wrong")

        sample_errors = [s for s in samples if "error" in s]
        sample_bad = [s for s in samples if "error" not in s and not (s["sum_ok"] and s["recon_ok"])]
        checks.add("invariant held in every mid-burst sample",
                   samples and not sample_bad and not sample_errors,
                   f"{len(samples)} samples, {len(sample_bad)} violations, {len(sample_errors)} sample errors")

        # ---- 5. sequential retries after the dust settles ------------------------------------
        settled = [r for r in plan if r.kind in ("general", "hot") and r.status in (201, 409) and not r.replayed]
        retry_sample = rng.sample(settled, min(args.sequential_retries, len(settled)))
        retries = [Req("retry", r.user, list(r.seats), r.key) for r in retry_sample]
        await asyncio.gather(*(api.reserve(show_id, r) for r in retries))
        retry_bad = 0
        for orig, again in zip(retry_sample, retries):
            want_status = 200 if orig.status == 201 else orig.status
            strip = lambda b: {k: v for k, v in b.items() if k != "request_id"}  # noqa: E731
            if again.status != want_status or not again.replayed or strip(again.body) != strip(orig.body):
                retry_bad += 1
        checks.add("later retries replay the original response exactly", retry_bad == 0,
                   f"{len(retries)} retries, {retry_bad} differ")

        winners = [r for r in plan if r.status == 201]
        misuse_later = [Req("key_misuse_late", r.user, [rng.choice([s for s in meta["general"] if s not in r.seats])], r.key)
                        for r in rng.sample(winners, min(50, len(winners)))]
        await asyncio.gather(*(api.reserve(show_id, r) for r in misuse_later))
        checks.add("winner's key reused with other seats -> 409, nothing reserved",
                   all(r.outcome == "idempotency_key_mismatch" for r in misuse_later),
                   f"{Counter(r.outcome for r in misuse_later)}")

        # ---- 6. cancel / release / rebook ---------------------------------------------------
        rbids = list(confirmed)
        rng.shuffle(rbids)
        to_cancel = rbids[: args.cancels]
        cancel_results = {"non_owner": Counter(), "owner": Counter(), "again": Counter()}
        released_seats: list[str] = []
        for rid in to_cancel:
            owner = confirmed[rid]["user_id"]
            intruder = rng.choice([u for u in meta["general_users"] if u != owner])
            st, _, _ = await api.call("POST", f"/reservations/{rid}/cancel", headers=api.auth(intruder))
            cancel_results["non_owner"][st] += 1
        mid = await api.show(show_id, admin=True)
        untouched = all(next(s for s in mid["seats"] if s["seat"] == seat).get("reservation_id") == rid
                        for rid in to_cancel for seat in confirmed[rid]["seats"])
        checks.add("non-owner cancel is refused and changes nothing",
                   set(cancel_results["non_owner"]) == {403} and untouched, str(dict(cancel_results["non_owner"])))
        for rid in to_cancel:
            owner = confirmed[rid]["user_id"]
            st, b, _ = await api.call("POST", f"/reservations/{rid}/cancel", headers=api.auth(owner))
            cancel_results["owner"][st] += 1
            if st == 200:
                released_seats += b.get("seats_released", [])
            st2, _, _ = await api.call("POST", f"/reservations/{rid}/cancel", headers=api.auth(owner))
            cancel_results["again"][st2] += 1
        checks.add("owner cancel -> 200, second cancel is a no-op 200",
                   set(cancel_results["owner"]) == {200} and set(cancel_results["again"]) == {200},
                   f"owner {dict(cancel_results['owner'])}, again {dict(cancel_results['again'])}")

        # re-storm the released seats: each must go to exactly one new user
        restorm_seats = released_seats[: args.restorm_seats]
        restorm_users = [f"{run_id}-re{i}" for i in range(len(restorm_seats) * args.restorm_size)]
        await api.mint(restorm_users)
        restorm = [Req("restorm", restorm_users[i * args.restorm_size + j], [seat], uuid.uuid4().hex, group=seat)
                   for i, seat in enumerate(restorm_seats) for j in range(args.restorm_size)]
        await asyncio.gather(*(api.reserve(show_id, r) for r in restorm))
        re_wins = Counter(r.group for r in restorm if r.status == 201)
        checks.add("released seats are re-bookable, exactly one winner each",
                   all(re_wins[s] == 1 for s in restorm_seats) and all(r.status in (201, 409) for r in restorm),
                   f"{len(restorm_seats)} seats x {args.restorm_size} users")

        # cancel racing against people trying to grab the same seats
        now = await api.show(show_id, admin=True)
        live = defaultdict(list)
        for s in now["seats"]:
            if s.get("reservation_id"):
                live[s["reservation_id"]].append(s["seat"])
        race_ids = [rid for rid in rbids[args.cancels:] if rid in live][: args.race_cancels]
        race_users = [f"{run_id}-rc{i}" for i in range(len(race_ids) * args.race_size)]
        await api.mint(race_users)
        race_reqs, cancel_calls = [], []
        for i, rid in enumerate(race_ids):
            owner = confirmed[rid]["user_id"]
            cancel_calls.append(api.call("POST", f"/reservations/{rid}/cancel", headers=api.auth(owner)))
            for j in range(args.race_size):
                race_reqs.append(Req("race", race_users[i * args.race_size + j], list(live[rid]), uuid.uuid4().hex,
                                     group=rid))
        rng.shuffle(race_reqs)
        cancel_out = await asyncio.gather(*cancel_calls, *(api.reserve(show_id, r) for r in race_reqs))
        race_cancel_status = Counter(c[0] for c in cancel_out[: len(cancel_calls)])
        race_wins = Counter(r.group for r in race_reqs if r.status == 201)
        checks.add("cancel racing re-bookers: cancels succeed, <= 1 winner per released reservation",
                   set(race_cancel_status) <= {200} and all(v <= 1 for v in race_wins.values())
                   and all(r.status in (201, 409) for r in race_reqs),
                   f"{len(race_ids)} cancels {dict(race_cancel_status)}, rebook wins {sum(race_wins.values())}")

        # ---- 7. final reconciliation: API state vs metrics -----------------------------------
        final = await api.show(show_id, admin=True)
        c = final["counts"]
        checks.add("final: available + held + confirmed == total_seats",
                   c["available"] + c["held"] + c["confirmed"] == c["total_seats"] == len(seats), str(c))
        checks.add("final: reconciliation cross-checks (reservations, quotas)", final["reconciliation"]["ok"],
                   json.dumps(final["reconciliation"]["checks"]))

        m_after = await api.metrics()
        everything = plan + retries + misuse_later + restorm + race_reqs
        observed = Counter(r.outcome for r in everything)
        seats_confirmed_obs = sum(len(r.seats) for r in everything if r.status == 201)

        def delta(name: str, **labels: str) -> float:
            return metric(m_after, name, show_id=show_id, **labels) - metric(m_before, name, show_id=show_id, **labels)

        rows = [
            ("reservations_confirmed_total", delta("reservations_confirmed_total"), observed["confirmed"]),
            ("reservation_seats_confirmed_total", delta("reservation_seats_confirmed_total"), seats_confirmed_obs),
        ]
        for reason in ("seat_taken", "per_user_limit", "idempotent_replay", "idempotency_key_mismatch"):
            rows.append((f"reservations_declined_total{{reason={reason}}}",
                         delta("reservations_declined_total", reason=reason), observed[reason]))
        gauge_rows = [
            ("seats_available (gauge)", metric(m_after, "seats_available", show_id=show_id), c["available"]),
            ("seats_held (gauge)", metric(m_after, "seats_held", show_id=show_id), c["held"]),
            ("seats_confirmed (gauge)", metric(m_after, "seats_confirmed", show_id=show_id), c["confirmed"]),
            ("seats_reconciliation_ok (gauge)", metric(m_after, "seats_reconciliation_ok", show_id=show_id), 1),
        ]
        net_conf = delta("reservation_seats_confirmed_total") - delta("reservation_seats_released_total")
        gauge_rows.append(("seats confirmed - released (counters)", net_conf, c["confirmed"]))

        print("\n  metrics reconciliation              /metrics   observed")
        all_match = True
        for name, got, want in rows + gauge_rows:
            match = abs(got - want) < 1e-9
            all_match &= match
            print(f"  {name:<40} {got:>8.0f} {want:>9}  {'ok' if match else 'MISMATCH'}")
        checks.add("metrics reconcile with API state and with client-observed outcomes", all_match,
                   "single-instance deployment assumed (per-process counters)")

        report["final_counts"] = c
        report["metrics"] = [{"name": n, "metrics": g, "observed": w} for n, g, w in rows + gauge_rows]

    # ---- summary ---------------------------------------------------------------------------
    print("\n  checks")
    for name, ok, detail in checks.items:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""))
    report["checks"] = [{"name": n, "ok": ok, "detail": d} for n, ok, d in checks.items]
    report["passed"] = checks.ok
    with open(args.report, "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    print(f"\n{'ALL CHECKS PASSED' if checks.ok else 'SOME CHECKS FAILED'} - report written to {args.report}")
    return 0 if checks.ok else 1


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("base_url")
    p.add_argument("--admin-key", default=os.environ.get("ADMIN_KEY", "dev-admin-key"))
    p.add_argument("--requests", type=int, default=20_000, help="total reserve requests in the burst")
    p.add_argument("--concurrency", type=int, default=500, help="max in-flight requests from this client")
    p.add_argument("--rows", type=int, default=20)
    p.add_argument("--seats-per-row", type=int, default=50)
    p.add_argument("--price-paise", type=int, default=25_000)
    p.add_argument("--limit", type=int, default=4, help="per_user_limit for the show")
    p.add_argument("--hot-seats", type=int, default=5)
    p.add_argument("--storm-size", type=int, default=500, help="distinct users per hot seat")
    p.add_argument("--dup-fraction", type=float, default=0.1, help="share of storm requests re-sent with the same key")
    p.add_argument("--limit-users", type=int, default=20)
    p.add_argument("--key-misuse-pairs", type=int, default=200)
    p.add_argument("--spoof", type=int, default=100)
    p.add_argument("--users", type=int, default=2000, help="general-population users")
    p.add_argument("--sequential-retries", type=int, default=200)
    p.add_argument("--cancels", type=int, default=20)
    p.add_argument("--restorm-seats", type=int, default=10)
    p.add_argument("--restorm-size", type=int, default=50)
    p.add_argument("--race-cancels", type=int, default=10)
    p.add_argument("--race-size", type=int, default=30)
    p.add_argument("--sample-every", type=float, default=0.5, help="seconds between mid-burst GET /shows samples")
    p.add_argument("--timeout", type=float, default=180, help="per-request timeout, seconds")
    p.add_argument("--ready-timeout", type=float, default=180, help="how long to wait for /readyz (cold start)")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--report", default="burst-report.json")
    args = p.parse_args()
    sys.exit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
