import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

CONTRACTS_PATH = ROOT / "domain" / "contracts.py"
SPEC = importlib.util.spec_from_file_location("contracts_module", CONTRACTS_PATH)
contracts_module = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
SPEC.loader.exec_module(contracts_module)

validate_municipios_input = contracts_module.validate_municipios_input
validate_alocacao_output = contracts_module.validate_alocacao_output
DataContractError = contracts_module.DataContractError


def test_validate_municipios_input_accepts_valid_dataframe():
    df = pd.DataFrame(
        [
            {
                "ranking_final": 1,
                "municipio": "Cidade A",
                "cluster": "Diamante",
                "indice_final": 95.0,
                "PD_qt": 70.0,
                "pop_censo2022": 100000,
            }
        ]
    )

    result = validate_municipios_input(df)
    assert len(result) == 1


def test_validate_municipios_input_rejects_missing_required_column():
    df = pd.DataFrame([{"ranking_final": 1, "municipio": "Cidade A", "cluster": "Diamante"}])

    with pytest.raises(DataContractError):
        validate_municipios_input(df)


def test_validate_alocacao_output_rejects_negative_budget():
    df = pd.DataFrame(
        [
            {
                "municipio": "Cidade A",
                "cluster": "Diamante",
                "ranking": 1,
                "indice": 95.0,
                "PD_qt": 70.0,
                "pop": 100000,
                "budget": -1.0,
                "digital": 10.0,
                "offline": 10.0,
                "meta_fb_ig": 1.0,
                "youtube": 1.0,
                "tiktok": 1.0,
                "whatsapp": 1.0,
                "google_ads": 1.0,
                "evento_presencial": 1.0,
                "radio_local": 1.0,
                "impresso": 1.0,
            }
        ]
    )

    with pytest.raises(DataContractError):
        validate_alocacao_output(df)


def test_validate_municipios_input_repairs_legacy_mojibake_cluster():
    legacy_cluster = "Consolida\u00e7\u00e3o".encode("utf-8").decode("latin1")
    df = pd.DataFrame(
        [
            {
                "ranking_final": 1,
                "municipio": "Cidade A",
                "cluster": legacy_cluster,
                "indice_final": 80.0,
            }
        ]
    )

    result = validate_municipios_input(df)
    assert len(result) == 1
