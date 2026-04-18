from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.rate_limit import InMemoryRateLimiter, install_rate_limit_middleware


class _Settings:
    app_env = "dev"
    tenant_id = "cliente-a"
    redis_url = "redis://redis:6379/0"
    api_rate_limit_enabled = True
    api_rate_limit_backend = "memory"
    api_rate_limit_requests = 2
    api_rate_limit_window_seconds = 60
    api_rate_limit_exempt_paths = "/health"


def _app(monkeypatch, settings=None):
    import api.rate_limit as rate_limit

    monkeypatch.setattr(rate_limit, "get_settings", lambda: settings or _Settings(), raising=False)
    app = FastAPI()
    install_rate_limit_middleware(app)

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/limited")
    def limited():
        return {"ok": True}

    return app


def test_in_memory_rate_limiter_blocks_after_limit():
    limiter = InMemoryRateLimiter()

    assert limiter.hit("key", limit=2, window_seconds=60)[0] is True
    assert limiter.hit("key", limit=2, window_seconds=60)[0] is True
    allowed, remaining, reset = limiter.hit("key", limit=2, window_seconds=60)

    assert allowed is False
    assert remaining == 0
    assert reset > 0


def test_rate_limit_middleware_returns_429_and_headers(monkeypatch):
    client = TestClient(_app(monkeypatch))

    first = client.get("/limited", headers={"Authorization": "Bearer token-a"})
    second = client.get("/limited", headers={"Authorization": "Bearer token-a"})
    third = client.get("/limited", headers={"Authorization": "Bearer token-a"})

    assert first.status_code == 200
    assert second.status_code == 200
    assert third.status_code == 429
    assert third.json() == {"detail": "rate limit exceeded"}
    assert third.headers["Retry-After"]
    assert third.headers["X-RateLimit-Limit"] == "2"


def test_rate_limit_uses_token_identity_and_exempts_health(monkeypatch):
    client = TestClient(_app(monkeypatch))

    assert client.get("/limited", headers={"Authorization": "Bearer token-a"}).status_code == 200
    assert client.get("/limited", headers={"Authorization": "Bearer token-a"}).status_code == 200
    assert client.get("/limited", headers={"Authorization": "Bearer token-b"}).status_code == 200
    assert client.get("/health", headers={"Authorization": "Bearer token-a"}).status_code == 200
