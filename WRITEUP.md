# Write-up

> Draft. Sections marked **TODO** need my own words before submission.

## 1. The atomic decision

A seat is one row in `seats`, primary key `(show_id, label)`. Two physical copies of a
seat cannot exist, and the row can point at exactly one `reservation_id`. So a double-sell
can only happen as a *lost update*: request B overwrites A's claim after A already got
its 201. Everything below exists to make that impossible.

Inside one Postgres transaction (READ COMMITTED), per reserve:

1. **Claim the idempotency key** – `INSERT … ON CONFLICT (user_id, show_id, key) DO NOTHING`.
2. **Lock the seats in a fixed order** – `SELECT label, status FROM seats WHERE … ORDER BY label FOR UPDATE`.
   After any wait, FOR UPDATE returns the latest committed version, so the status I check
   is the status I now hold a lock on; nobody can change it until I commit. Any seat not
   `available` → decline `seat_taken`.
3. **Per-user quota** – one conditional upsert that checks and increments in a single statement:
   ```sql
   INSERT INTO user_show_quota … SELECT $show, $user, $n WHERE $n <= $limit
   ON CONFLICT (show_id, user_id) DO UPDATE
      SET seats_held = user_show_quota.seats_held + EXCLUDED.seats_held
    WHERE user_show_quota.seats_held + EXCLUDED.seats_held <= $limit
   RETURNING seats_held           -- no row back => over the limit
   ```
   The row lock it takes serialises one user's parallel requests for one show, so ten
   parallel requests on a limit of 4 cannot all read "0 held".
4. **Guarded write** – `UPDATE seats SET status='confirmed', reservation_id=$r … WHERE … AND status='available'`,
   and the update must touch exactly `n` rows or the transaction aborts. Under the locks
   this is redundant; it is there so that a future refactor that drops the lock fails
   loudly instead of double-selling.
5. Insert the reservation, record the response under the idempotency key, commit.

Steps 2–4 run inside a savepoint. A decline rolls back to it (undoing the quota increment
and releasing the seat locks), stores the 409 under the key, and commits.

**Decline precedence:** seats are checked before the limit everywhere – the fast path
(below) reads seats before the single-request limit check, and the transaction locks seats
before the quota row – so a request that is over the limit *and* wants a taken seat is
always declined as `seat_taken`, whichever path it takes.

**Why it is race-free:** the only path from `available` to `confirmed` is a write made
while holding the row lock on that seat, after re-reading its current committed state
under that lock. Two transactions cannot both hold it; the second one reads `confirmed`
and declines.

**Deadlocks:** every write path takes the shared locks in one global order: seat rows
sorted by label, then the user's quota row. Reserve claims its idempotency key first and
cancel locks its reservation row first; reserve never locks reservation rows and cancel
never locks key rows, so those first locks cannot be part of a cycle. Nothing is locked
after the quota row except rows the transaction already holds. With a single global
order no cycle can form. Deadlock and serialization errors are still caught
and the whole transaction retried (it rolled back entirely, key claim included), and
`reserve_transaction_retries_total` counts them; it stayed at 0 in every burst.

**Hot-seat fast path:** before the transaction, a plain `SELECT … WHERE status <> 'available'`.
If any requested seat is already taken, the request is declined immediately instead of
queueing on that row's lock. This can only ever produce a *decline*, and the seat really
was taken at the moment it was read, so the decline is valid at that point in time. A
confirm never comes from this read. With 500 users on A12, one wins through the locked
path; nearly all the rest see `confirmed` and leave without touching a lock.

**Multi-seat:** all-or-nothing. If any seat is taken, nothing is reserved.

## 2. Idempotency

* **Where:** table `idempotency_keys`, primary key `(user_id, show_id, key)`, holding a
  SHA-256 of the canonical request (sorted seats), the status code and the response body.
  Keys are scoped per user and show: two users cannot collide, and a client that reuses
  simple keys like `k1` on a new show is not mistaken for a retry.
* **Exactly once:** the first request to insert `(user, key)` owns the decision. Its
  effects (seats, quota, reservation) and the stored response commit in the same
  transaction, so a key never exists without its outcome, and an outcome never exists
  without its key. A concurrent request with the same key blocks on the unique index
  until the owner commits, then finds the row and replays it. If the owner rolls back,
  the waiter proceeds as the new owner.
* **Replays:** same key + same body → the stored response. A replayed success returns
  **200** with `Idempotent-Replayed: true` and the original body (same `reservation_id`),
  so "exactly one 201 per seat" stays true even when the winner retries. **TODO: confirm
  200 vs 201 is the call I want; it is a one-line change in `_replay()`.**
* **Replay after a cancel:** the key lookup joins the reservation's current status. If it
  was cancelled since, the replay is the same reservation with `status: "cancelled"` and
  `cancelled_at`, not a stale `"confirmed"`. A retry never re-books; a new attempt needs a
  new key.
* **Declines are stored too.** A request declined with `seat_taken` replays that same
  409 on retry. The key represents one attempt; a new attempt needs a new key. This makes
  "same key, different body → 409" hold whether the first attempt won or lost.
* **Same key, different body:** hash mismatch → `409 idempotency_key_reused`, nothing changes.

## 3. Holds and expiry

I chose **explicit cancel**: reserve confirms immediately (the brief's 201 says
`status: "confirmed"`; there is no payment step to hold for), and the owner can
`POST /reservations/{id}/cancel`.

Cancel releases only seats that still point at *this* reservation
(`UPDATE seats … WHERE reservation_id = $this`). A seat that was since sold to someone
else carries their id and is untouched, so a late or repeated cancel can never resurrect
another user's seat. It decrements the quota by the number of seats actually released,
and cancelling twice is a no-op 200. The burst races 10 cancels against 300 people trying
to grab the same seats: every cancel succeeds and each released set gets at most one
new owner.

Adding time-boxed holds (the obvious next step, see §7) needs: `held_until` on seats;
reserve writes `status='held'`; a `confirm` endpoint flips `held → confirmed` guarded on
`reservation_id = $r AND held_until > now()`; and expiry is a guarded
`UPDATE … SET status='available' WHERE status='held' AND held_until < now()`. The same
reservation-id guard makes expiry unable to touch a seat that was confirmed or re-sold.

## 4. Consistency vs availability under a partition

Consistency. Postgres is the only place a seat decision is made, and the app never
decides from local state (the only cache holds immutable show metadata: seat list, price,
limit). If an instance cannot reach the database, it refuses: reserves return
`503 database_unavailable`, `/readyz` goes red so the platform stops routing to it, and
`/livez` stays green so it is not restart-looped. Selling nothing for a minute is
recoverable; selling A12 twice is not.

The client side of a partition is a timeout with an unknown outcome. The idempotency key
makes the retry safe: the retry either replays the committed result or performs the
attempt for the first time.

With replicas: reads like `GET /shows` could be served from a replica and be slightly
stale; reserves must go to the primary. Failover needs synchronous replication; with
async replication a failover can lose acknowledged reservations and the seats would
be sold again.

## 5. Observability: what pages me at 2am

| Signal | Why |
|---|---|
| `seats_reconciliation_ok == 0` for any show | Integrity breach: owned seats disagree with active reservations or quotas. Page immediately. |
| Any 5xx on `/shows/{id}/reserve` over 5 min | Declines are 409s by design; a 5xx is a bug or an outage. |
| `/readyz` failing or `db_up == 0` for > 1 min | We are refusing all sales. |
| `db_pool_connections{state="in_use"} == max` with p99 latency over SLO for 10 min during an on-sale | Capacity: requests queue for connections. |
| `reserve_transaction_retries_total` increasing | Should be 0 by design. Ticket alone, page together with 5xx. |

Not paged: a spike in `seat_taken`. That is the product working.

The simple invariant `available + held + confirmed == total` cannot catch a double-sell
on its own, because a seat row has exactly one status no matter what. The cross-checks
in `GET /shows/{id}` and `seats_reconciliation_ok` compare seat ownership with active
reservations and quota rows, and those are what catch it (see §8).

## 6. AI usage

**TODO: rewrite in my own words after reviewing; keep it specific and honest.**

What happened, factually: the first working version of this repository (application
code, burst script, Docker/Render config, README and a draft of this write-up) was
generated by Claude (Anthropic, Opus 5.5) in one session from the assignment text. It
ran the service against a local Postgres 16 and iterated until the 20k burst passed. The
first commits carry a `Co-Authored-By: Claude` trailer.

Decisions proposed by the AI in that first version, for me to review and own:

- Python/FastAPI/asyncpg + Postgres (matches my production background), raw SQL so the locking is visible
- Locked read in label order + guarded update, rather than a bare conditional update
- The hot-seat fast decline path
- Storing declines under the idempotency key; (user, show) key scope
- 200 (not 201) for a replayed success
- Explicit cancel instead of TTL holds; `seat_taken` reported before `per_user_limit`
- Admin-minted JWTs as the identity stand-in; public `/logs` ring buffer
- Single worker per instance

What I reviewed, changed or decided myself: **TODO**

## 7. What I'd do next

1. **Holds with TTL + confirm** (§3), with a sweeper and lazy expiry inside the guarded update.
2. **Idempotency key retention:** expire after 24h and clean up in batches; the table grows with every request today.
3. **Throughput:** the API process, not Postgres, is the bottleneck (~0.8 CPU-seconds per
   second at ~700 req/s locally vs ~0.5 for Postgres). Profile the hot path, then scale out
   instances (correctness lives in the database, so instances are stateless). For real
   on-sales, add an admission queue in front so the database sees a steady rate.
4. **Metrics at scale:** drop `show_id` from counter labels (unbounded cardinality), aggregate across instances, ship logs to a real backend instead of the in-process `/logs` buffer.
5. **CI:** run a small burst against `docker compose` on every push, plus focused tests for the lock-order and replay paths.

## 8. How I know the checks can fail

As a negative control I temporarily removed `FOR UPDATE` and the `status = 'available'`
guard (a classic read-then-write) and ran a 5,000-request burst. The script caught it:
14 seats confirmed to two users, hot seat A4 sold twice, 7 of 8 mid-burst samples failing
the cross-checks, metrics out of line with the API. The simple sum invariant still
passed, which is why the cross-checks exist. With the locking restored, the full 20k burst
passes every check with zero 5xx.
