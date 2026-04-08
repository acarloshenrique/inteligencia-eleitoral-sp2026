from pathlib import Path
import sys

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from domain.open_data_contracts import OpenDataContractError, validate_municipio_dimension, validate_municipio_enriched


def test_validate_municipio_dimension_ok():
    df = pd.DataFrame(
        [
            {
                "municipio_id_ibge7": "3550308",
                "codigo_tse": "1001",
                "codigo_ibge": "3550308",
                "nome_municipio": "Sao Paulo",
                "municipio_norm": "sao paulo",
            }
        ]
    )
    out = validate_municipio_dimension(df)
    assert len(out) == 1


def test_validate_municipio_enriched_rejects_invalid_status():
    df = pd.DataFrame(
        [
            {
                "municipio": "A",
                "ranking_final": 1,
                "municipio_id_ibge7": "3550308",
                "ano": 2026,
                "mes": 3,
                "turno": 1,
                "canonical_key": "3550308:2026:03:1",
                "join_status": "unknown",
            }
        ]
    )
    try:
        validate_municipio_enriched(df)
        assert False, "Esperava OpenDataContractError"
    except OpenDataContractError:
        assert True
