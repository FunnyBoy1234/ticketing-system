# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
"""Malformed and hostile input against every endpoint: nothing may return a 5xx.

Standard library only, so it runs anywhere and against any deployment:

    python3 scripts/check_bad_input.py http://localhost:8000
    ADMIN_KEY=... python3 scripts/check_bad_input.py https://<live-url>

Covers invalid JSON, wrong types, floats/strings/bools for money, oversized and empty
inputs, characters Postgres cannot store (NUL) or that are not valid Unicode (lone
surrogates), forged and alg=none tokens, bad UUIDs and unknown routes. Exit code 0 only
if no case returned >= 500 and every case got the status class it should.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid


def main() -> int:
    base = (sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000").rstrip("/")
    admin = {"X-Admin-Key": os.environ.get("ADMIN_KEY", "dev-admin-key")}
    run = uuid.uuid4().hex[:6]

    def call(method, path, body=None, headers=None, raw=None):
        data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
        req = urllib.request.Request(base + path, data=data, method=method,
                                     headers={"Content-Type": "application/json", **(headers or {})})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.status, r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode("utf-8", "replace")

    def b64(d):
        return base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()

    deadline = time.time() + 120  # wait out a cold start before judging anything
    while True:
        try:
            if call("GET", "/readyz")[0] == 200:
                break
        except OSError:
            pass
        if time.time() > deadline:
            print(f"{base}/readyz did not return 200 within 120 s")
            return 2
        time.sleep(2)

    st, body = call("POST", "/shows", {"name": f"bad-input-{run}", "seats": ["A1", "A2", "A3"], "price_paise": 100}, admin)
    if st != 201:
        print(f"could not create a show ({st}): {body[:200]} - is ADMIN_KEY right?")
        return 2
    sid = json.loads(body)["id"]
    st, body = call("POST", "/auth/token", {"user_id": f"bad-input-{run}"}, admin)
    tok = json.loads(body)["access_token"]
    user = {"Authorization": f"Bearer {tok}"}
    now = int(time.time())
    unsigned = b64({"alg": "HS256", "typ": "JWT"}) + "." + b64({"sub": "x", "iss": "seat-reservation", "exp": now + 99})
    forged = unsigned + "." + base64.urlsafe_b64encode(
        hmac.new(b"wrong-secret", unsigned.encode(), hashlib.sha256).digest()).rstrip(b"=").decode()
    none_alg = b64({"alg": "none", "typ": "JWT"}) + "." + b64({"sub": "x", "iss": "seat-reservation", "exp": now + 99}) + "."
    r = f"/shows/{sid}/reserve"
    zero = "00000000-0000-0000-0000-000000000000"
    lone = "\ud800"  # json.dumps sends it as the escape \ud800; the server decodes a lone surrogate

    # (expected status, method, path, json body, headers, raw bytes)
    cases = [
        (400, "POST", "/shows", None, admin, b"not json"),
        (400, "POST", "/shows", None, admin, b""),
        (422, "POST", "/shows", [], admin, None),
        (422, "POST", "/shows", {}, admin, None),
        (422, "POST", "/shows", {"name": "x", "seats": [], "price_paise": 1}, admin, None),
        (422, "POST", "/shows", {"name": "x", "seats": ["A1"], "price_paise": 1.5}, admin, None),
        (422, "POST", "/shows", {"name": "x", "seats": ["A1"], "price_paise": "100"}, admin, None),
        (422, "POST", "/shows", {"name": "x", "seats": ["A1"], "price_paise": True}, admin, None),
        (422, "POST", "/shows", {"name": "x", "seats": ["A1"], "price_paise": 1e30}, admin, None),
        (422, "POST", "/shows", {"name": "x", "seats": [1, 2], "price_paise": 1}, admin, None),
        (422, "POST", "/shows", {"name": "x", "seats": ["A1\n"], "price_paise": 1}, admin, None),
        (422, "POST", "/shows", {"name": "x", "seats": [lone], "price_paise": 1}, admin, None),
        (422, "POST", "/shows", {"name": "x\u0000y", "seats": ["A1"], "price_paise": 1}, admin, None),
        (422, "POST", "/shows", {"name": lone, "seats": ["A1"], "price_paise": 1}, admin, None),
        (422, "POST", "/shows", {"name": "x", "seats": ["A1"], "price_paise": 1, lone: 1}, admin, None),
        (422, "POST", "/shows", {"name": "x", "seats": ["A1"], "price_paise": 1, "per_user_limit": 0}, admin, None),
        (422, "POST", "/shows", {"name": "x", "seats": ["A1", "A1"], "price_paise": 1}, admin, None),
        (403, "POST", "/shows", {"name": "x", "seats": ["A1"], "price_paise": 1}, {"X-Admin-Key": "nope"}, None),
        (404, "GET", "/shows/not-a-uuid", None, None, None),
        (404, "GET", f"/shows/{zero}", None, None, None),
        (401, "POST", r, {"seats": ["A1"], "idempotency_key": "k"}, None, None),
        (401, "POST", r, {"seats": ["A1"], "idempotency_key": "k"}, {"Authorization": "Bearer garbage"}, None),
        (401, "POST", r, {"seats": ["A1"], "idempotency_key": "k"}, {"Authorization": f"Bearer {forged}"}, None),
        (401, "POST", r, {"seats": ["A1"], "idempotency_key": "k"}, {"Authorization": f"Bearer {none_alg}"}, None),
        (400, "POST", r, None, user, b"{bad"),
        (422, "POST", r, ["A1"], user, None),
        (422, "POST", r, {"seats": [], "idempotency_key": "k"}, user, None),
        (422, "POST", r, {"seats": None, "idempotency_key": "k"}, user, None),
        (422, "POST", r, {"seats": ["A1"] * 51, "idempotency_key": "k"}, user, None),
        (422, "POST", r, {"seats": ["A1"], "idempotency_key": 123}, user, None),
        (400, "POST", r, {"seats": ["A1"], "idempotency_key": ""}, user, None),
        (400, "POST", r, {"seats": ["A1"], "idempotency_key": "x" * 129}, user, None),
        (400, "POST", r, {"seats": ["A1"]}, {**user, "Idempotency-Key": "x" * 129}, None),
        (400, "POST", r, {"seats": ["A1"], "idempotency_key": "nul\u0000key"}, user, None),
        (400, "POST", r, {"seats": ["A1"], "idempotency_key": "tab\tkey"}, user, None),
        (400, "POST", r, {"seats": ["A1"], "idempotency_key": lone}, user, None),
        (400, "POST", r, {"seats": ["A1"], "idempotency_key": "a"}, {**user, "Idempotency-Key": "b"}, None),
        (422, "POST", r, {"seats": ["ZZ9"], "idempotency_key": "k2"}, user, None),
        (422, "POST", r, {"seats": ["A1", "A1"], "idempotency_key": "k5"}, user, None),
        (404, "POST", "/shows/not-a-uuid/reserve", {"seats": ["A1"], "idempotency_key": "k3"}, user, None),
        (404, "POST", f"/shows/{zero}/reserve", {"seats": ["A1"], "idempotency_key": "k4"}, user, None),
        (201, "POST", r, {"seats": ["A1"], "idempotency_key": "emoji-🎟️-ünïcode"}, user, None),
        (404, "POST", "/reservations/not-a-uuid/cancel", None, user, None),
        (404, "POST", f"/reservations/{zero}/cancel", None, user, None),
        (404, "GET", f"/reservations/{zero}", None, user, None),
        (422, "GET", "/logs?limit=abc", None, None, None),
        (200, "GET", "/logs?limit=-5", None, None, None),
        (422, "POST", "/auth/token", {"user_id": "a\u0000b"}, admin, None),
        (422, "POST", "/auth/token", {"user_id": "alice\n"}, admin, None),
        (422, "POST", "/auth/token", {"user_id": lone}, admin, None),
        (404, "GET", "/nope", None, None, None),
    ]

    failures = 0
    for want, method, path, body, headers, raw in cases:
        st, text = call(method, path, body, headers, raw)
        shown = json.dumps(body)[:70] if body is not None else repr((raw or b"")[:20])
        ok = st < 500 and st == want
        failures += not ok
        tag = "ok  " if ok else ("5XX " if st >= 500 else "FAIL")
        print(f"{tag} {st} (want {want})  {method} {path[:40]}  {shown}")
        if not ok:
            print(f"       -> {text[:200]}")
    print(f"\n{len(cases)} cases, {failures} failed")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
