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
