import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "healthcheck.py"
SPEC = importlib.util.spec_from_file_location("healthcheck_module", MODULE_PATH)
healthcheck = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
SPEC.loader.exec_module(healthcheck)


def test_env_bool_variants(monkeypatch):
    monkeypatch.setenv("X_TEST_BOOL", "true")
    assert healthcheck.env_bool("X_TEST_BOOL") is True
    monkeypatch.setenv("X_TEST_BOOL", "0")
    assert healthcheck.env_bool("X_TEST_BOOL") is False
    monkeypatch.delenv("X_TEST_BOOL", raising=False)
    assert healthcheck.env_bool("X_TEST_BOOL", default=True) is True


def test_find_df_mun_prefers_fixed_timestamp(tmp_path):
    base = tmp_path
    estado = base / "outputs" / "estado_sessao"
    estado.mkdir(parents=True, exist_ok=True)
    fixed = estado / "df_mun_20260316_1855.parquet"
    newer = estado / "df_mun_20260401_1010.parquet"
    fixed.write_text("x", encoding="utf-8")
    newer.write_text("x", encoding="utf-8")
    assert healthcheck.find_df_mun(base, "20260316_1855") == fixed


def test_check_readiness_requires_data(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("REQUIRE_DATA", "true")
    monkeypatch.delenv("REQUIRE_GROQ_API_KEY", raising=False)
    ok, messages = healthcheck.check_readiness()
    assert ok is False
    assert any("required dataset missing" in m for m in messages)
