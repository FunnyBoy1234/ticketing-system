# Seat Reservation at Scale

A small JSON API that sells assigned seats for a show and stays correct when thousands of
buyers hit the same seats in the same second: no seat sold twice, no user over their
limit, no retried request charged twice. It is instrumented so you can watch it do that
live: health probes, Prometheus metrics that reconcile with the API, structured logs with
request ids, and a one-command stampede that checks all of it.

**Stack:** Python 3.12 · FastAPI · asyncpg · PostgreSQL 16 · Prometheus client · Docker

**Live URL:** `<TODO: https://your-service.onrender.com>`  ·  **Write-up:** [WRITEUP.md](WRITEUP.md)

---

## Run it

```bash
docker compose up --build -d          # Postgres + API on http://localhost:8000
curl localhost:8000/readyz            # {"status":"ready",...}
./burst.sh http://localhost:8000      # ~20k-request stampede + correctness checks
```

Against the deployment (the admin key is the `ADMIN_KEY` env var of the service):

```bash
ADMIN_KEY=<admin-key> ./burst.sh https://<live-url>
# or: make burst BASE_URL=https://<live-url> ADMIN_KEY=<admin-key>
```

`burst.sh` uses `uv` if installed, otherwise creates a small venv with `aiohttp`. It can
also run inside Docker: `docker compose --profile burst run --rm burst`.

Without Docker: `pip install -r requirements.txt`, point `DATABASE_URL` at a Postgres,
`uvicorn app.main:app`. The schema is applied automatically on startup.

---

## API

All bodies are JSON. Money is integer **paise**; floats are rejected. Every response
carries an `X-Request-ID` header (send your own to correlate), and every error has a
stable `error` code plus the `request_id`.

### Auth

* **Users** send `Authorization: Bearer <token>`. The user id comes from the token's
  `sub` claim and nothing else; a `user_id` in a request body is ignored (and logged).
* **Admin** sends `X-Admin-Key: <key>`. Admin creates shows and mints user tokens. The
  token endpoint stands in for a real identity provider so a test harness can make
  thousands of users.

```bash
curl -X POST $BASE/auth/token -H "X-Admin-Key: $ADMIN_KEY" -H 'Content-Type: application/json' \
     -d '{"user_id": "alice"}'                       # -> {"access_token": "...", ...}
curl -X POST $BASE/auth/token -H "X-Admin-Key: $ADMIN_KEY" -H 'Content-Type: application/json' \
     -d '{"user_ids": ["u1", "u2", "u3"]}'           # -> {"tokens": {"u1": "...", ...}}
```

### Endpoints

| Method & path | Who | What |
|---|---|---|
| `POST /shows` | admin | `{"name", "seats": [...], "price_paise", "per_user_limit"?}` → 201 with the show and every seat `available`. `per_user_limit` defaults to 4. |
| `GET /shows/{id}` | anyone | Per-seat status, counts, and a `reconciliation` block. With `X-Admin-Key`, each taken seat also shows its `user_id` and `reservation_id`. |
| `POST /shows/{id}/reserve` | user | `{"seats": ["A12"], "idempotency_key": "..."}` (or `Idempotency-Key` header) → 201 reservation, `status: "confirmed"`. |
| `POST /reservations/{id}/cancel` | owner | Releases the reservation's seats back to `available`. |
| `GET /reservations/{id}` | owner / admin | The reservation. |
| `GET /livez` | anyone | Liveness: process is serving. No dependency checks. |
| `GET /readyz` | anyone | Readiness: runs `SELECT 1` on Postgres over a dedicated connection; 503 if it fails. |
| `GET /metrics` | anyone | Prometheus text format. |
| `GET /logs?limit=200&request_id=…&event=…` | anyone* | Tail of this instance's structured logs. *Set `LOGS_ENDPOINT_PUBLIC=false` to require the admin key. |
| `GET /docs` | anyone | OpenAPI UI. |

### Reserve outcomes

| Status | `error` / meaning | When |
|---|---|---|
| **201** | confirmed | You got every seat you asked for. |
| **200** + `Idempotent-Replayed: true` | replay of the original 201 | Same key, same body, after it already succeeded. Same `reservation_id`; nothing new is created. If the reservation was cancelled since, `status` is `"cancelled"` (with `cancelled_at`) – a retry never re-books. |
| **409** | `seat_taken` | At least one requested seat is held/confirmed by someone else. **All-or-nothing:** nothing was reserved. Lists the taken seats. |
| **409** | `per_user_limit` | This would take you over `per_user_limit` seats for the show. |
| **409** | `idempotency_key_reused` | The key was already used on this show with different seats. |
| **409** + `Idempotent-Replayed: true` | replay of an earlier decline | Same key, same body, after it was declined. Use a new key for a new attempt. |
| 400 | `idempotency_key_required`, `invalid_json`, … | Malformed request. |
| 401 / 403 | `unauthorized` / `not_reservation_owner` | Missing/invalid token; cancelling someone else's reservation. |
| 404 / 422 | `show_not_found` / `unknown_seats`, `duplicate_seats`, `validation_error` | Bad references or input. |

Behaviour decisions, stated once:

* **Multi-seat requests are all-or-nothing.** `["A12","A13"]` with A12 taken → 409, A13 untouched.
* **Idempotency keys are scoped per user and show** and bind to the first request body that
  used them, whatever its outcome. Seat order does not matter (`["A13","A12"]` == `["A12","A13"]`).
* **Release model: explicit cancel.** Reserve confirms immediately (the brief's response
  is `status: "confirmed"`), and only the owner can cancel. Cancelling twice is a no-op 200.
  `held` exists in the schema and the counts but is always 0 in this build (see WRITEUP).
* **Decline precedence:** `seat_taken` always wins over `per_user_limit` when both apply,
  on both the fast path and the locked path (seats are checked before the limit in each).

---

## The burst script

`./burst.sh <BASE_URL>` creates a fresh 1,000-seat show (limit 4), mints ~4,500 users,
and releases ~20,000 reserve requests at the same instant:

| Scenario | Default | Checks |
|---|---|---|
| Hot-seat storm | 5 seats × 500 distinct users | exactly one 201 per seat, everyone else 409 (the winner's own retry gets a 200 replay) |
| Concurrent duplicate retries | 10% of the storm re-sent with the same key + body | one decision per key, the twin replays it byte-for-byte |
| Key misuse | 200 pairs, same key, different seats, fired together | exactly one `idempotency_key_reused` per pair |
| Per-user limit | 20 users × 10 parallel requests on their own seats | each ends with ≤ 4 seats |
| Spoofing | 100 requests with someone else's `user_id` in the body | reservation belongs to the token's user |
| General stampede | the rest, 1–3 seats each, half aimed at the front rows | no seat in two 201s; every 201 matches server state; no confirmed seat without a 201 |

During the burst it samples `GET /shows/{id}` every 0.5 s and checks the invariant on
each sample. Afterwards it replays 200 completed requests sequentially (must replay
exactly), reuses 50 winning keys with other seats (must 409), cancels 20 reservations
(non-owner 403 first, then owner 200, then a no-op second cancel), re-storms released
seats (exactly one winner each), races 10 cancels against 300 re-bookers, and finally
reconciles `/metrics` against both the API state and the outcomes it observed.

It prints the outcome distribution, latency percentiles, a metrics reconciliation table
and PASS/FAIL per check, writes `burst-report.json`, and exits non-zero on any failure.
Every scenario size is a flag: `./burst.sh <url> --help`.

`scripts/check_edge_cases.py` covers two paths a burst can't hit on demand: it holds a seat
lock from a separate database connection to force a request onto the locked path, then
checks that `seat_taken` wins over `per_user_limit` there, and that replaying a key after
its reservation was cancelled reports `status: "cancelled"` without re-booking. It needs
`DATABASE_URL`, so run it against `docker compose`, not the deployment.

Local run (2 vCPU box shared by Postgres, the API and the client):

```
[burst] 20000 requests in 28.6s (700 req/s)  latency p50 673ms  p95 1764ms  p99 2485ms

  outcome                       count
  seat_taken                    18655
  confirmed                       771
  idempotent_replay               250
  idempotency_key_mismatch        200
  per_user_limit                  124

  metrics reconciliation              /metrics   observed
  reservations_confirmed_total            ...    ...  ok
  ...
  [PASS] zero 5xx during burst  (0 responses >= 500)
  [PASS] hot seats: exactly one 201 each, rest 409 (500 users per seat)
  ...
ALL CHECKS PASSED
```

---

## Observability

**Metrics** (`/metrics`):

| Metric | Type | Notes |
|---|---|---|
| `reservations_confirmed_total{show_id}` | counter | 201s |
| `reservation_seats_confirmed_total{show_id}` | counter | seats moved to confirmed |
| `reservations_declined_total{show_id,reason}` | counter | `seat_taken`, `per_user_limit`, `idempotent_replay`, `idempotency_key_mismatch` |
| `reservations_cancelled_total`, `reservation_seats_released_total` | counter | per show |
| `seats_available`, `seats_held`, `seats_confirmed`, `seats_total` `{show_id,show_name}` | gauge | read from Postgres at scrape time (latest 50 shows) |
| `seats_reconciliation_ok{show_id}` | gauge | 1 if the invariant and the cross-checks hold |
| `reserve_transaction_retries_total{sqlstate}` | counter | deadlock/serialization retries; expected to stay 0 |
| `http_requests_total`, `http_request_duration_seconds`, `http_requests_in_flight` | | per route & status |
| `db_up`, `db_pool_connections{state}` | gauge | pool saturation is visible as `in_use == max` |

Counters are incremented after the transaction commits. The seat gauges come from the
same rows `GET /shows/{id}` reads, so they cannot drift from the API. Counters are per
process: run one instance (or aggregate with `sum()` across instances in Prometheus).

**Logs:** one JSON object per line on stdout with `ts`, `level`, `event`, `request_id`,
`user_id` and event fields (`reservation_confirmed`, `reservation_cancelled`,
`http_request` with `status`, `duration_ms` and `outcome`). The last 5,000 lines are also
served by `GET /logs` — e.g. `/logs?request_id=<id>` shows everything one request did.

---

## Deploy (Render)

1. Push this repo to GitHub.
2. Render → **New → Blueprint** → pick the repo. `render.yaml` creates the Docker web
   service and a Postgres, wires `DATABASE_URL`, and generates `ADMIN_KEY` / `JWT_SECRET`.
3. Health check path is `/readyz`, so a deploy only goes live once it can reach Postgres.
4. Copy `ADMIN_KEY` from the service's Environment tab, then `ADMIN_KEY=... ./burst.sh <url>`.

Free instances sleep after 15 idle minutes (~1 minute to wake) and have a fraction of a
CPU, so a 20k burst is slow there; `plan: 0.5c-512mb` avoids both. Free Postgres expires
after 30 days.

| Env var | Default | |
|---|---|---|
| `DATABASE_URL` | local Postgres | `postgres://` or `postgresql://` |
| `ADMIN_KEY`, `JWT_SECRET` | dev values (logged as a warning) | **set in production** |
| `DB_POOL_MAX` | 20 | keep under the database's connection limit |
| `DB_ACQUIRE_TIMEOUT_S` | 120 | requests queue for a connection rather than fail |
| `DB_STATEMENT_CACHE_SIZE` | 100 | set `0` behind PgBouncer in transaction mode |
| `LOGS_ENDPOINT_PUBLIC` | true | false → `/logs` needs the admin key |

---

## Layout

```
app/
  main.py          routes, validation, auth wiring, health/metrics/logs endpoints
  reservations.py  reserve + cancel: the atomic decision, idempotency, per-user quota
  shows.py         create show, show state with reconciliation, metrics snapshot
  schema.sql       tables and constraints (applied on startup)
  db.py            pool, startup retry, migrations, dedicated probe/observer connections
  observability.py JSON logs, request ids, Prometheus metrics, ASGI middleware
  auth.py          JWT (HS256) users, admin key
scripts/burst.py   the stampede + checks
scripts/check_edge_cases.py  forced-race checks for replay-after-cancel and seat_taken precedence (local, needs DB access)
burst.sh           one-command wrapper
```
