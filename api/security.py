from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any, Callable

from fastapi import Depends, Header, HTTPException, Request

from config.settings import get_settings
from infrastructure.secret_factory import build_secret_provider


ALLOWED_ROLES = {"admin", "operator", "viewer"}
DEV_FALLBACK_TOKEN = "dev-admin-token"


class AuthConfigurationError(RuntimeError):
    pass


@dataclass
class AuthContext:
    actor: str
    role: str
    token_fingerprint: str


def _parse_expiry(value: Any) -> datetime | None:
    if value is None or str(value).strip() == "":
        return None
    raw = str(value).strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise AuthConfigurationError("API_TOKENS_JSON contem expires_at invalido") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _validate_token_entry(token: str, entry: Any, *, app_env: str) -> dict[str, Any]:
    if not isinstance(entry, dict):
        raise AuthConfigurationError("API_TOKENS_JSON deve mapear token para objeto de credencial")
    role = str(entry.get("role", "")).strip().lower()
    actor = str(entry.get("actor", "")).strip()
    if role not in ALLOWED_ROLES:
        raise AuthConfigurationError("API_TOKENS_JSON contem role invalida")
    if not actor:
        raise AuthConfigurationError("API_TOKENS_JSON contem actor vazio")
    if app_env != "dev" and token == DEV_FALLBACK_TOKEN:
        raise AuthConfigurationError("dev-admin-token e proibido fora de APP_ENV=dev")
    expires_at = _parse_expiry(entry.get("expires_at"))
    if app_env != "dev" and expires_at is None:
        raise AuthConfigurationError("Tokens de API em staging/prod exigem expires_at para rotacao")
    if expires_at is not None and expires_at <= datetime.now(UTC):
        raise AuthConfigurationError("API_TOKENS_JSON contem token expirado")
    normalized = dict(entry)
    normalized["role"] = role
    normalized["actor"] = actor
    normalized["expires_at"] = expires_at.isoformat() if expires_at else None
    return normalized


def _tokens_map() -> dict[str, dict[str, Any]]:
    settings = get_settings()
    app_env = str(getattr(settings, "app_env", "dev") or "dev").lower()
    provider = build_secret_provider(settings)
    mapping = provider.get_json("API_TOKENS_JSON")
    if not mapping:
        if app_env == "dev":
            return {DEV_FALLBACK_TOKEN: {"role": "admin", "actor": "dev-admin", "expires_at": None}}
        raise AuthConfigurationError("API_TOKENS_JSON deve estar configurado fora de APP_ENV=dev")

    normalized: dict[str, dict[str, Any]] = {}
    for token, entry in mapping.items():
        token_value = str(token).strip()
        if not token_value:
            raise AuthConfigurationError("API_TOKENS_JSON contem token vazio")
        normalized[token_value] = _validate_token_entry(token_value, entry, app_env=app_env)
    return normalized


def validate_auth_configuration() -> None:
    _tokens_map()


def _token_fp(token: str) -> str:
    return sha256(token.encode("utf-8")).hexdigest()[:12]


def get_auth_context(
    authorization: str | None = Header(default=None, alias="Authorization"),
) -> AuthContext:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    try:
        mapping = _tokens_map()
    except AuthConfigurationError as exc:
        raise HTTPException(status_code=500, detail="auth configuration invalid") from exc
    entry = mapping.get(token)
    if not entry:
        raise HTTPException(status_code=403, detail="invalid token")
    return AuthContext(actor=str(entry["actor"]), role=str(entry["role"]), token_fingerprint=_token_fp(token))


def require_roles(*roles: str) -> Callable:
    role_set = {r.lower() for r in roles}

    def _dep(ctx: AuthContext = Depends(get_auth_context)) -> AuthContext:
        if ctx.role.lower() not in role_set:
            raise HTTPException(status_code=403, detail=f"role '{ctx.role}' not allowed")
        return ctx

    return _dep


def audit_metadata_from_request(request: Request) -> dict:
    return {"path": request.url.path, "method": request.method}
