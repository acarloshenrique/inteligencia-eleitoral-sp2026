from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from application.territorial_master_service import (
    MASTER_COLUMNS,
    MunicipioCrosswalkBuilder,
    TerritorialMasterIndexBuilder,
    build_municipio_crosswalk,
    normalize_name,
)


def test_municipio_crosswalk_exact_declared_and_normalized_name():
    tse = pd.DataFrame(
        [
            {"SIGLA_UF": "SP", "COD_MUN_TSE": "71072", "COD_MUN_IBGE": "3550308", "MUNICIPIO": "SAO PAULO"},
            {"SIGLA_UF": "SP", "COD_MUN_TSE": "70173", "MUNICIPIO": "Santa Bárbara D'Oeste"},
        ]
    )
    ibge = pd.DataFrame(
        [
            {"SIGLA_UF": "SP", "COD_MUN_IBGE": "3550308", "MUNICIPIO": "São Paulo"},
            {"SIGLA_UF": "SP", "COD_MUN_IBGE": "3545803", "MUNICIPIO": "Santa Barbara D Oeste"},
        ]
    )

    result = build_municipio_crosswalk(tse, ibge)

    assert result.exact_matches == 1
    assert result.normalized_name_matches == 1
    assert result.unmatched == 0
    row = result.dataframe[result.dataframe["COD_MUN_TSE"].eq("70173")].iloc[0]
    assert row["COD_MUN_IBGE_MATCH"] == "3545803"
    assert row["join_method"] == "normalized_name_unique"
    assert normalize_name("Santa Bárbara D'Oeste") == "SANTA BARBARA D OESTE"


def test_municipio_crosswalk_controls_ambiguity_and_does_not_force_join():
    tse = pd.DataFrame([{"SIGLA_UF": "SP", "COD_MUN_TSE": "1", "MUNICIPIO": "Bom Jardim"}])
    ibge = pd.DataFrame(
        [
            {"SIGLA_UF": "SP", "COD_MUN_IBGE": "111", "MUNICIPIO": "Bom Jardim"},
            {"SIGLA_UF": "RJ", "COD_MUN_IBGE": "222", "MUNICIPIO": "Bom Jardim"},
        ]
    )

    result = MunicipioCrosswalkBuilder().build(tse, ibge)
    row = result.dataframe.iloc[0]

    assert result.ambiguous == 1
    assert row["join_method"] == "ambiguous"
    assert bool(row["join_ambiguity_flag"]) is True
    assert row["COD_MUN_IBGE_MATCH"] == ""
    assert row["join_candidates"] == "111,222"


def test_municipio_crosswalk_fuzzy_match_uses_lower_confidence():
    tse = pd.DataFrame([{"SIGLA_UF": "SP", "COD_MUN_TSE": "123", "MUNICIPIO": "Sao Luiz do Paraitnga"}])
    ibge = pd.DataFrame([{"SIGLA_UF": "SP", "COD_MUN_IBGE": "3550001", "MUNICIPIO": "São Luiz do Paraitinga"}])

    result = MunicipioCrosswalkBuilder().build(tse, ibge, fuzzy_threshold=0.85)
    row = result.dataframe.iloc[0]

    assert result.fuzzy_matches == 1
    assert row["join_method"] == "fuzzy_name"
    assert row["COD_MUN_IBGE_MATCH"] == "3550001"
    assert 0 < row["join_confidence"] < 0.9


def test_territorial_master_index_applies_crosswalk_and_preserves_required_columns(tmp_path: Path):
    zone = pd.DataFrame(
        [
            {
                "ano_eleicao": 2024,
                "uf": "SP",
                "cod_tse_municipio": "70173",
                "municipio": "Santa Bárbara D'Oeste",
                "zona_eleitoral": 186,
                "join_confidence": 0.96,
            }
        ]
    )
    section = pd.DataFrame(
        [
            {
                "ano_eleicao": 2024,
                "uf": "SP",
                "cod_tse_municipio": "70173",
                "municipio": "Santa Bárbara D'Oeste",
                "zona_eleitoral": 186,
                "secao_eleitoral": 10,
                "local_votacao": "Escola A",
                "join_confidence": 0.96,
            }
        ]
    )
    ibge = pd.DataFrame([{"SIGLA_UF": "SP", "COD_MUN_IBGE": "3545803", "MUNICIPIO": "Santa Barbara D Oeste"}])
    builder = TerritorialMasterIndexBuilder()

    master = builder.build_master_index(zone_fact=zone, section_fact=section, ibge_municipios=ibge, candidate_id="cand")
    result = builder.publish_master_index(master, tmp_path, dataset_version="test")

    assert list(master.columns) == MASTER_COLUMNS
    assert set(master["COD_MUN_IBGE"]) == {"3545803"}
    assert set(master["join_method"]) == {"normalized_name_unique"}
    assert master["territorio_id"].nunique() == 2
    assert result.parquet_path is not None and result.parquet_path.exists()
    assert result.manifest_path is not None and result.manifest_path.exists()
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["quality"]["coverage_ibge"] == 1.0
    assert manifest["join_policy"]["ambiguous"]


def test_territorial_master_quality_flags_unmatched_ibge():
    master = TerritorialMasterIndexBuilder().build_master_index(
        zone_fact=pd.DataFrame(
            [
                {
                    "ano_eleicao": 2024,
                    "uf": "SP",
                    "cod_tse_municipio": "999",
                    "municipio": "Cidade Sem Match",
                    "zona_eleitoral": 1,
                }
            ]
        ),
        ibge_municipios=pd.DataFrame([{"SIGLA_UF": "SP", "COD_MUN_IBGE": "123", "MUNICIPIO": "Outra Cidade"}]),
    )
    quality = TerritorialMasterIndexBuilder().quality_report(master)

    assert quality["coverage_ibge"] == 0.0
    assert quality["unmatched"] == 1
    assert master.loc[0, "join_method"] == "unmatched"
    assert master.loc[0, "join_confidence"] == 0.0
