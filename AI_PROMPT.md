# Prompt that reproduces this service
---

Build a seat-reservation service for the attached take-home. Python 3.12, FastAPI, asyncpg (raw SQL),
PostgreSQL 16, one uvicorn worker, prometheus-client, PyJWT. Dependencies in `pyproject.toml` + `uv.lock`. Commit incrementally as you go and run what you build.

## Design (already decided)

- **Single source of truth:** one Postgres. No Redis, no queue.
- **A seat is a row:** `seats(show_id, label)` primary key, with `status` (`available|held|confirmed`) and
  the owning `reservation_id` / `user_id`. A DB `CHECK` ties status to ownership.
- **Atomic decision:** in one transaction, `SELECT ... ORDER BY label FOR UPDATE` on the requested seats,
  then `UPDATE seats SET status='confirmed' ... WHERE status='available'`, and roll back unless it
  touched every requested seat. Locking in label order means multi-seat requests cannot deadlock.
- **Multi-seat requests are all-or-nothing:** every seat in one reservation, or nothing changes.
- **Per-user limit:** a `user_show_quota(show_id, user_id, seats_held)` table updated with one
  conditional upsert (`... ON CONFLICT DO UPDATE ... WHERE seats_held + n <= limit`), so the check and
  the increment are a single atomic statement.
- **Idempotency:** table `idempotency_keys` keyed by `(user_id, show_id, key)` storing a hash of the
  request and the response. The first request inserts the key inside the same transaction as its
  effects; later requests replay the stored response. Same key with different seats gives 409.
  A replayed success returns 200 (not 201), so "exactly one 201 per seat" holds.
- **Release:** explicit `POST /reservations/{id}/cancel`, owner only. It releases only seats still
  pointing at that reservation, so it can never take back a seat sold to someone else.
- **Identity:** a signed JWT (`Authorization: Bearer`) carries the user; the body is never trusted for
  identity. Admin endpoints use an `X-Admin-Key` header. Add a small admin-only `POST /auth/token` to
  mint test user tokens.
- **Money:** integer paise only.

## API

- `POST /shows` (admin) `{name, seats[], price_paise}` -> 201, all seats `available`
- `GET /shows/{id}` -> per-seat status and counts, plus a reconciliation check
  (`available + held + confirmed == total_seats`), read in one consistent snapshot
- `POST /shows/{id}/reserve` (user) `{seats[], idempotency_key}` (or `Idempotency-Key` header) -> 201;
  409 for `seat_taken`, `per_user_limit`, or a reused key with a different body. Never a 5xx for a
  domain decline.
- `POST /reservations/{id}/cancel` (owner) -> 200
- `GET /livez`, `GET /readyz` (checks the DB, fails closed with 503), `GET /metrics`, `GET /logs`

All errors are JSON `{error, message, request_id}`.

## Observability

- Structured JSON logs, with a request id on every line (accept or generate `X-Request-ID`), and a
  recent-log ring buffer served at `/logs`.
- Prometheus metrics: `reservations_confirmed` (counter), `reservations_declined{reason}` with reasons
  `seat_taken | per_user_limit | idempotent_replay`, and `seats_available` / `seats_held` /
  `seats_confirmed` gauges read from Postgres at scrape time so they always agree with `GET /shows/{id}`.

## Deliverables

- `scripts/burst.py` + `burst.sh <BASE_URL>`: a one-command on-sale stampede (~20k concurrent requests)
  including a hot-seat storm, duplicate retries with the same key, key misuse, a per-user-limit test, and
  a spoofed-identity test. It prints the outcome distribution (confirmed / declined by reason / 5xx) and
  the final reconciliation, and exits non-zero if any check fails.
- Dockerfile + docker-compose (app + Postgres), a Makefile with `up`, `down`, `burst`, and a
  `render.yaml` for deploying to Render.
- README (how to run, the endpoints, how to run the burst, where metrics and logs are) and a draft
  WRITEUP.md covering: the atomic decision, idempotency, holds and expiry, consistency vs availability
  under a partition, observability, and what I'd do next. Leave the "AI usage" section for me.

Start by laying out the plan, then build it in small commits.
