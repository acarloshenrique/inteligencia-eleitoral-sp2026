from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Callable

from fastapi import Depends, Header, HTTPException, Request

from config.settings import get_settings
from infrastructure.secret_factory import build_secret_provider


@dataclass
class AuthContext:
    actor: str
    role: str
    token_fingerprint: str


def _tokens_map() -> dict:
    settings = get_settings()
    provider = build_secret_provider(settings)
    mapping = provider.get_json("API_TOKENS_JSON")
    if not mapping:
        # fallback para ambiente de desenvolvimento
        return {"dev-admin-token": {"role": "admin", "actor": "dev-admin"}}
    return mapping


def _token_fp(token: str) -> str:
    return sha256(token.encode("utf-8")).hexdigest()[:12]


def get_auth_context(
    authorization: str | None = Header(default=None, alias="Authorization"),
) -> AuthContext:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    mapping = _tokens_map()
    entry = mapping.get(token)
    if not entry:
        raise HTTPException(status_code=403, detail="invalid token")
    role = str(entry.get("role", "viewer"))
    actor = str(entry.get("actor", "unknown"))
    return AuthContext(actor=actor, role=role, token_fingerprint=_token_fp(token))


def require_roles(*roles: str) -> Callable:
    role_set = {r.lower() for r in roles}

    def _dep(ctx: AuthContext = Depends(get_auth_context)) -> AuthContext:
        if ctx.role.lower() not in role_set:
            raise HTTPException(status_code=403, detail=f"role '{ctx.role}' not allowed")
        return ctx

    return _dep


def audit_metadata_from_request(request: Request) -> dict:
    return {"path": request.url.path, "method": request.method}
