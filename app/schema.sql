-- Schema is applied on startup (idempotent) under an advisory lock, so several
-- instances booting at once do not race each other.

CREATE TABLE IF NOT EXISTS shows (
    id              uuid        PRIMARY KEY,
    name            text        NOT NULL,
    price_paise     bigint      NOT NULL CHECK (price_paise >= 0),
    per_user_limit  int         NOT NULL CHECK (per_user_limit > 0),
    total_seats     int         NOT NULL CHECK (total_seats > 0),
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS shows_created_at_idx ON shows (created_at DESC);

-- One row per physical seat. The row IS the seat: the primary key makes a second
-- copy of a seat impossible, and the row can point at exactly one reservation.
-- Selling a seat twice would need two rows or an overwrite; the PK rules out the
-- first and the guarded UPDATE (WHERE status = 'available') rules out the second.
CREATE TABLE IF NOT EXISTS seats (
    show_id         uuid        NOT NULL REFERENCES shows (id),
    label           text        NOT NULL,
    position        int         NOT NULL,           -- creation order, for display
    status          text        NOT NULL DEFAULT 'available'
                                CHECK (status IN ('available', 'held', 'confirmed')),
    reservation_id  uuid,
    user_id         text,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (show_id, label),
    -- An available seat has no owner; a held/confirmed seat has exactly one.
    CONSTRAINT seat_owner_matches_status CHECK ((status = 'available') = (reservation_id IS NULL)),
    CONSTRAINT seat_owner_complete       CHECK ((reservation_id IS NULL) = (user_id IS NULL))
);
CREATE INDEX IF NOT EXISTS seats_reservation_idx ON seats (reservation_id) WHERE reservation_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS reservations (
    id              uuid        PRIMARY KEY,
    show_id         uuid        NOT NULL REFERENCES shows (id),
    user_id         text        NOT NULL,
    seats           text[]      NOT NULL,
    amount_paise    bigint      NOT NULL CHECK (amount_paise >= 0),
    status          text        NOT NULL CHECK (status IN ('confirmed', 'cancelled')),
    created_at      timestamptz NOT NULL DEFAULT now(),
    cancelled_at    timestamptz
);
CREATE INDEX IF NOT EXISTS reservations_show_user_idx ON reservations (show_id, user_id);

-- Per (show, user) running count of seats owned. Incremented with a conditional
-- upsert, so the limit check and the increment are one atomic statement, and the
-- row lock serialises one user's parallel requests for one show.
CREATE TABLE IF NOT EXISTS user_show_quota (
    show_id     uuid NOT NULL REFERENCES shows (id),
    user_id     text NOT NULL,
    seats_held  int  NOT NULL CHECK (seats_held >= 0),
    PRIMARY KEY (show_id, user_id)
);

-- Idempotency: one row per (user, key). The first request to insert the row owns
-- the decision; every later request with the same key replays the stored response
-- (or gets 409 if its body hashes differently). status_code is NULL only inside the
-- transaction that claimed the key - it is filled in before that transaction commits.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    user_id         text        NOT NULL,
    key             text        NOT NULL,
    request_hash    text        NOT NULL,
    show_id         uuid        NOT NULL,
    status_code     int,
    response        jsonb,
    reservation_id  uuid,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, key)
);
