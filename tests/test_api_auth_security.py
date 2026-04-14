from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from fastapi import HTTPException

from api import security


class _Provider:
    def __init__(self, mapping):
        self._mapping = mapping

    def get_json(self, key: str):
        assert key == "API_TOKENS_JSON"
        return self._mapping


def _settings(app_env: str):
    return type("_Settings", (), {"app_env": app_env, "secret_backend": "vault"})()


def _patch_auth(monkeypatch, *, app_env: str, mapping):
    monkeypatch.setattr(security, "get_settings", lambda: _settings(app_env))
    monkeypatch.setattr(security, "build_secret_provider", lambda _settings: _Provider(mapping))


def test_dev_fallback_token_exists_only_in_dev(monkeypatch):
    _patch_auth(monkeypatch, app_env="dev", mapping={})
    ctx = security.get_auth_context("Bearer dev-admin-token")
    assert ctx.actor == "dev-admin"
    assert ctx.role == "admin"


def test_missing_token_map_fails_fast_outside_dev(monkeypatch):
    _patch_auth(monkeypatch, app_env="prod", mapping={})
    with pytest.raises(security.AuthConfigurationError):
        security.validate_auth_configuration()
    with pytest.raises(HTTPException) as exc:
        security.get_auth_context("Bearer anything")
    assert exc.value.status_code == 500


def test_dev_admin_token_is_forbidden_outside_dev(monkeypatch):
    expires = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    _patch_auth(
        monkeypatch,
        app_env="staging",
        mapping={"dev-admin-token": {"actor": "ops", "role": "admin", "expires_at": expires}},
    )
    with pytest.raises(security.AuthConfigurationError):
        security.validate_auth_configuration()


def test_non_dev_tokens_require_expiry(monkeypatch):
    _patch_auth(monkeypatch, app_env="prod", mapping={"token": {"actor": "ops", "role": "admin"}})
    with pytest.raises(security.AuthConfigurationError):
        security.validate_auth_configuration()


def test_expired_token_fails_configuration(monkeypatch):
    expires = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    _patch_auth(monkeypatch, app_env="prod", mapping={"token": {"actor": "ops", "role": "admin", "expires_at": expires}})
    with pytest.raises(security.AuthConfigurationError):
        security.validate_auth_configuration()


def test_valid_secret_backed_token_authenticates(monkeypatch):
    expires = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    _patch_auth(monkeypatch, app_env="prod", mapping={"token": {"actor": "ops", "role": "operator", "expires_at": expires}})
    ctx = security.get_auth_context("Bearer token")
    assert ctx.actor == "ops"
    assert ctx.role == "operator"
    assert ctx.token_fingerprint


def test_invalid_role_fails_configuration(monkeypatch):
    expires = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    _patch_auth(monkeypatch, app_env="prod", mapping={"token": {"actor": "ops", "role": "root", "expires_at": expires}})
    with pytest.raises(security.AuthConfigurationError):
        security.validate_auth_configuration()
