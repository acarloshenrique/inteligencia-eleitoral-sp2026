import importlib.util
from pathlib import Path

import pytest
from pydantic import ValidationError

MODULE_PATH = Path(__file__).resolve().parents[1] / "config" / "settings.py"
SPEC = importlib.util.spec_from_file_location("settings_module", MODULE_PATH)
settings_module = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
SPEC.loader.exec_module(settings_module)
Settings = settings_module.Settings

ENV_MODULE_PATH = Path(__file__).resolve().parents[1] / "infrastructure" / "env.py"
ENV_SPEC = importlib.util.spec_from_file_location("env_module", ENV_MODULE_PATH)
env_module = importlib.util.module_from_spec(ENV_SPEC)
assert ENV_SPEC is not None and ENV_SPEC.loader is not None
ENV_SPEC.loader.exec_module(env_module)


def test_settings_env_alias_development_maps_to_dev():
    s = Settings(APP_ENV="development")
    assert s.app_env == "dev"


def test_settings_accepts_prod():
    s = Settings(APP_ENV="prod")
    assert s.app_env == "prod"


def test_settings_rejects_invalid_env():
    try:
        Settings(APP_ENV="invalid")
        raise AssertionError("Expected ValidationError for invalid APP_ENV")
    except ValidationError:
        assert True


def test_build_paths_creates_governed_data_lake(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    s = Settings(DATA_ROOT=str(tmp_path / "data"))
    paths = s.build_paths()
    assert paths.lake_root == (tmp_path / "data" / "data_lake").resolve()
    assert paths.bronze_root.exists()
    assert paths.silver_root.exists()
    assert paths.gold_root.exists()
    assert paths.catalog_root.exists()
    assert paths.catalog_root.name == "catalog"
    assert paths.features_root.exists()
    assert paths.features_root.name == "features"


def test_build_paths_accepts_explicit_data_lake_root(tmp_path):
    explicit = tmp_path / "enterprise_lake"
    s = Settings(DATA_ROOT=str(tmp_path / "data"), DATA_LAKE_ROOT=str(explicit))
    paths = s.build_paths()
    assert paths.lake_root == explicit.resolve()
    assert (explicit / "bronze").exists()
    assert (explicit / "silver").exists()
    assert (explicit / "gold").exists()
    assert (explicit / "catalog").exists()
    assert (explicit / "features").exists()
    assert paths.features_root == (explicit / "features").resolve()


def test_prod_bootstrap_requires_redis_tls_auth_and_tenant_chroma(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    settings = Settings(APP_ENV="prod", DATA_ROOT=str(tmp_path / "data"), REDIS_URL="redis://redis:6379/0")
    paths = settings.build_paths()
    errors = env_module.validate_prod_runtime_hardening(settings, paths)
    assert any("Redis com TLS" in item for item in errors)
    assert any("Redis com senha" in item for item in errors)
    assert any("TENANT_ID dedicado" in item for item in errors)


def test_prod_bootstrap_accepts_hardened_redis_and_tenant_chroma(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    settings = Settings(
        APP_ENV="prod",
        DATA_ROOT=str(tmp_path / "data"),
        TENANT_ID="cliente-a",
        REDIS_URL="rediss://:strong-redis-secret@redis:6379/0",
        LGPD_ANONYMIZATION_SALT="prod-long-random-salt",
    )
    paths = settings.build_paths()
    assert env_module.validate_prod_runtime_hardening(settings, paths) == []


def test_prod_bootstrap_allows_external_vector_backend(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    settings = Settings(
        APP_ENV="prod",
        DATA_ROOT=str(tmp_path / "data"),
        REDIS_URL="rediss://:strong-redis-secret@redis:6379/0",
        CHROMA_VECTOR_BACKEND="external",
        LGPD_ANONYMIZATION_SALT="prod-long-random-salt",
    )
    paths = settings.build_paths()
    errors = env_module.validate_prod_runtime_hardening(settings, paths)
    assert not errors


def test_prod_bootstrap_blocks_weak_infrastructure_placeholders(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    settings = Settings(
        APP_ENV="prod",
        DATA_ROOT=str(tmp_path / "data"),
        TENANT_ID="cliente-a",
        REDIS_URL="rediss://:strong-redis-secret@redis:6379/0",
        ARTIFACT_BACKEND="s3",
        S3_ACCESS_KEY="minioadmin",
        S3_SECRET_KEY="change-me",
        LGPD_ANONYMIZATION_SALT="change-me",
    )
    paths = settings.build_paths()
    errors = env_module.validate_prod_runtime_hardening(settings, paths)

    assert any("LGPD_ANONYMIZATION_SALT" in item for item in errors)
    assert any("S3_ACCESS_KEY" in item for item in errors)
    assert any("S3_SECRET_KEY" in item for item in errors)


def test_prod_bootstrap_accepts_strong_s3_and_lgpd_secrets(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    settings = Settings(
        APP_ENV="prod",
        DATA_ROOT=str(tmp_path / "data"),
        TENANT_ID="cliente-a",
        REDIS_URL="rediss://:strong-redis-secret@redis:6379/0",
        ARTIFACT_BACKEND="s3",
        S3_ACCESS_KEY="tenant-a-access-key",
        S3_SECRET_KEY="tenant-a-very-long-secret-key",
        LGPD_ANONYMIZATION_SALT="prod-long-random-salt",
    )
    paths = settings.build_paths()

    assert env_module.validate_prod_runtime_hardening(settings, paths) == []


def test_worker_runner_fails_fast_on_prod_hardening_errors(monkeypatch):
    from workers import runner

    class _Settings:
        redis_url = "redis://redis:6379/0"
        rq_queue_name = "jobs"

        def build_paths(self):
            return object()

    monkeypatch.setattr(runner, "get_settings", lambda: _Settings())
    monkeypatch.setattr(runner, "validate_prod_runtime_hardening", lambda settings, paths: ["weak secret"])

    with pytest.raises(RuntimeError, match="weak secret"):
        runner.main()
