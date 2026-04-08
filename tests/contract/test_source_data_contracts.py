from pathlib import Path
import sys

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from infrastructure.source_contracts import SourceContractError, validate_input_contracts


def test_validate_input_contracts_accepts_minimal_valid_sources():
    report = validate_input_contracts(
        base_df=pd.DataFrame([{"municipio": "Sao Paulo", "ranking_final": 1, "ano": 2026, "turno": 1}]),
        mapping_df=pd.DataFrame([{"codigo_tse": "71072", "codigo_ibge": "3550308", "nome_municipio": "Sao Paulo"}]),
        socio_df=pd.DataFrame([{"codigo_ibge": "3550308"}]),
        secao_df=pd.DataFrame([{"municipio": "Sao Paulo", "votos_validos": 10}]),
        ibge_df=pd.DataFrame([{"codigo_ibge": "3550308", "pop_total": 10}]),
        seade_df=pd.DataFrame([{"codigo_ibge": "3550308", "ipvs": 0.5}]),
        fiscal_df=pd.DataFrame([{"codigo_ibge": "3550308", "transferencias": 100, "emendas": 10}]),
    )
    assert report["validated_sources"]["base_eleitoral"] == 1


def test_validate_input_contracts_rejects_invalid_mapping():
    try:
        validate_input_contracts(
            base_df=pd.DataFrame([{"municipio": "Sao Paulo", "ranking_final": 1}]),
            mapping_df=pd.DataFrame([{"codigo_tse": "", "codigo_ibge": "3550308", "nome_municipio": "Sao Paulo"}]),
            socio_df=pd.DataFrame(),
            secao_df=pd.DataFrame(),
            ibge_df=pd.DataFrame(),
            seade_df=pd.DataFrame(),
            fiscal_df=pd.DataFrame(),
        )
        assert False, "esperava SourceContractError"
    except SourceContractError:
        assert True
