from __future__ import annotations

import time
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Protocol

from fastapi import FastAPI, Request
from starlette.responses import JSONResponse, Response

from config.settings import get_settings


class RateLimiter(Protocol):
    def hit(self, key: str, *, limit: int, window_seconds: int) -> tuple[bool, int, int]: ...


@dataclass
class InMemoryRateLimiter:
    _buckets: dict[str, tuple[int, int]]

    def __init__(self) -> None:
        self._buckets = {}

    def hit(self, key: str, *, limit: int, window_seconds: int) -> tuple[bool, int, int]:
        now = int(time.time())
        window_start = now - (now % window_seconds)
        current_window, count = self._buckets.get(key, (window_start, 0))
        if current_window != window_start:
            current_window, count = window_start, 0
        count += 1
        self._buckets[key] = (current_window, count)
        reset_seconds = max(1, window_seconds - (now - current_window))
        remaining = max(0, limit - count)
        return count <= limit, remaining, reset_seconds


class RedisRateLimiter:
    def __init__(self, redis_url: str):
        import redis

        self._client = redis.Redis.from_url(redis_url, decode_responses=True)
        self._client.ping()

    def hit(self, key: str, *, limit: int, window_seconds: int) -> tuple[bool, int, int]:
        now = int(time.time())
        window_start = now - (now % window_seconds)
        redis_key = f"rate_limit:{key}:{window_start}"
        pipe = self._client.pipeline()
        pipe.incr(redis_key)
        pipe.expire(redis_key, window_seconds + 1)
        count = int(pipe.execute()[0])
        reset_seconds = max(1, window_seconds - (now - window_start))
        remaining = max(0, limit - count)
        return count <= limit, remaining, reset_seconds


def _exempt_paths(settings: Any) -> set[str]:
    raw = str(getattr(settings, "api_rate_limit_exempt_paths", "/health") or "")
    return {item.strip() for item in raw.split(",") if item.strip()}


def _identity(request: Request) -> str:
    authorization = request.headers.get("authorization", "")
    if authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1].strip()
        if token:
            return "token:" + sha256(token.encode("utf-8")).hexdigest()[:16]
    host = request.client.host if request.client else "unknown"
    return f"ip:{host}"


def build_rate_limiter(settings: Any) -> RateLimiter:
    backend = str(getattr(settings, "api_rate_limit_backend", "redis") or "redis").strip().lower()
    if backend == "memory":
        return InMemoryRateLimiter()
    try:
        return RedisRateLimiter(str(getattr(settings, "redis_url", "redis://redis:6379/0")))
    except Exception:
        if str(getattr(settings, "app_env", "dev") or "dev").lower() == "prod":
            raise
        return InMemoryRateLimiter()


def _headers(*, limit: int, remaining: int, reset_seconds: int) -> dict[str, str]:
    return {
        "X-RateLimit-Limit": str(limit),
        "X-RateLimit-Remaining": str(max(0, remaining)),
        "X-RateLimit-Reset": str(reset_seconds),
    }


def install_rate_limit_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def rate_limit_middleware(request: Request, call_next) -> Response:
        settings = get_settings()
        if not bool(getattr(settings, "api_rate_limit_enabled", True)):
            return await call_next(request)
        if request.url.path in _exempt_paths(settings):
            return await call_next(request)

        limit = int(getattr(settings, "api_rate_limit_requests", 120) or 120)
        window_seconds = int(getattr(settings, "api_rate_limit_window_seconds", 60) or 60)
        limiter = getattr(app.state, "rate_limiter", None)
        if limiter is None:
            limiter = build_rate_limiter(settings)
            app.state.rate_limiter = limiter

        tenant_id = str(getattr(settings, "tenant_id", "default") or "default")
        key = f"{tenant_id}:{request.url.path}:{_identity(request)}"
        allowed, remaining, reset_seconds = limiter.hit(key, limit=limit, window_seconds=window_seconds)
        headers = _headers(limit=limit, remaining=remaining, reset_seconds=reset_seconds)
        if not allowed:
            headers["Retry-After"] = str(reset_seconds)
            return JSONResponse(
                status_code=429,
                content={"detail": "rate limit exceeded"},
                headers=headers,
            )

        response = await call_next(request)
        for name, value in headers.items():
            response.headers[name] = value
        return response
