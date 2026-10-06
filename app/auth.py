"""Authentication Module.

Users: HS256 JWT in `Authorization: Bearer <token>`. The user id is the `sub` claim
and nothing else - request bodies never carry identity.

Admin: `X-Admin-Key` header. Admin creates shows and mints user tokens (standing
in for a real identity provider so a test harness can create thousands of users).
"""

from __future__ import annotations

import hmac
import re
import time

import jwt
from fastapi import Request

from .config import settings
from .errors import forbidden, unauthorized
from .observability import request_context

_ISSUER = "seat-reservation"
USER_ID_RE = re.compile(r"^[A-Za-z0-9_.@:-]{1,64}$")


def mint_token(user_id: str) -> str:
    now = int(time.time())
    claims = {"sub": user_id, "iss": _ISSUER, "iat": now, "exp": now + settings.token_ttl_seconds}
    return jwt.encode(claims, settings.jwt_secret, algorithm="HS256")


def user_from_token(token: str) -> str:
    try:
        claims = jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=["HS256"],
            issuer=_ISSUER,
            options={"require": ["sub", "exp", "iss"]},
        )
    except jwt.PyJWTError as exc:
        raise unauthorized(f"invalid token: {exc.__class__.__name__}") from exc
    sub = claims.get("sub")
    if not isinstance(sub, str) or not USER_ID_RE.fullmatch(sub):
        raise unauthorized("invalid token subject")
    return sub


def is_admin(request: Request) -> bool:
    supplied = request.headers.get("x-admin-key", "")
    return bool(supplied) and hmac.compare_digest(supplied.encode(), settings.admin_key.encode())


def require_admin(request: Request) -> None:
    if not is_admin(request):
        raise forbidden("admin_required", "this endpoint requires a valid X-Admin-Key header")


def require_user(request: Request) -> str:
    header = request.headers.get("authorization", "")
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise unauthorized()
    user_id = user_from_token(token.strip())
    request_context()["user_id"] = user_id
    return user_id
