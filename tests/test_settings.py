import importlib.util
from pathlib import Path

from pydantic import ValidationError


MODULE_PATH = Path(__file__).resolve().parents[1] / "config" / "settings.py"
SPEC = importlib.util.spec_from_file_location("settings_module", MODULE_PATH)
settings_module = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
SPEC.loader.exec_module(settings_module)
Settings = settings_module.Settings


def test_settings_env_alias_development_maps_to_dev():
    s = Settings(APP_ENV="development")
    assert s.app_env == "dev"


def test_settings_accepts_prod():
    s = Settings(APP_ENV="prod")
    assert s.app_env == "prod"


def test_settings_rejects_invalid_env():
    try:
        Settings(APP_ENV="invalid")
        assert False, "Expected ValidationError for invalid APP_ENV"
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
