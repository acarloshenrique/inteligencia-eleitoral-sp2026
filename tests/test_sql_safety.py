import importlib.util
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

SQL_SAFETY_PATH = ROOT / "infrastructure" / "sql_safety.py"
SQL_SPEC = importlib.util.spec_from_file_location("sql_safety_module", SQL_SAFETY_PATH)
sql_safety_module = importlib.util.module_from_spec(SQL_SPEC)
assert SQL_SPEC is not None and SQL_SPEC.loader is not None
SQL_SPEC.loader.exec_module(sql_safety_module)

is_allowed_table_name = sql_safety_module.is_allowed_table_name


def test_is_allowed_table_name_blocks_injection_payload():
    assert not is_allowed_table_name("municipios; DROP TABLE municipios")
    assert not is_allowed_table_name("municipios where 1=1")
    assert not is_allowed_table_name("municipios--")
    assert is_allowed_table_name("municipios")


def test_is_allowed_table_name_accepts_only_known_tables():
    assert is_allowed_table_name("municipios")
    assert is_allowed_table_name("alocacao")
    assert is_allowed_table_name("secoes")
    assert is_allowed_table_name("mapa_tatico")
    assert is_allowed_table_name("mart_score_alocacao_modular")
    assert is_allowed_table_name("mart_recomendacao_alocacao")
    assert is_allowed_table_name("mart_midia_paga_municipio")
    assert is_allowed_table_name("mart_social_mensagem_territorial")
    assert not is_allowed_table_name("usuarios")
