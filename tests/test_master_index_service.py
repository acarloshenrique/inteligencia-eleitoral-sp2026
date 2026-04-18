from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from application.master_index_service import MASTER_INDEX_COLUMNS, MasterIndexBuilder, MasterIndexPipeline


def _resultados() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ano_eleicao": 2024,
                "uf": "sp",
                "cod_municipio_tse": "71072",
                "municipio_nome": "Sao Paulo",
                "zona": "1",
                "secao": "25",
                "candidate_id": "123",
                "numero_candidato": "13",
                "partido": "PT",
            }
        ]
    )


def test_master_index_builds_canonical_schema_with_exact_joins() -> None:
    builder = MasterIndexBuilder()
    master = builder.build(
        resultados_secao=_resultados(),
        eleitorado_secao=pd.DataFrame(
            [{"ano_eleicao": 2024, "uf": "SP", "cod_municipio_tse": "71072", "zona": "0001", "secao": "0025"}]
        ),
        locais_votacao=pd.DataFrame(
            [
                {
                    "ano_eleicao": 2024,
                    "uf": "SP",
                    "cod_municipio_tse": "71072",
                    "zona": "0001",
                    "secao": "0025",
                    "local_votacao": "Escola Estadual A",
                }
            ]
        ),
        candidatos=pd.DataFrame(
            [{"ano_eleicao": 2024, "uf": "SP", "candidate_id": "123", "numero_candidato": "13", "partido": "PT"}]
        ),
        prestacao_contas=pd.DataFrame([{"ano_eleicao": 2024, "uf": "SP", "candidate_id": "123"}]),
        municipio_crosswalk=pd.DataFrame(
            [
                {
                    "uf": "SP",
                    "cod_municipio_tse": "71072",
                    "cod_municipio_ibge": "3550308",
                    "municipio_nome": "Sao Paulo",
                }
            ]
        ),
        setores_censitarios=pd.DataFrame([{"cod_municipio_ibge": "3550308", "cd_setor": "355030801000001"}]),
    )

    assert list(master.columns) == MASTER_INDEX_COLUMNS
    assert master.loc[0, "cod_municipio_tse"] == "71072"
    assert master.loc[0, "cod_municipio_ibge"] == "3550308"
    assert master.loc[0, "zona"] == "0001"
    assert master.loc[0, "secao"] == "0025"
    assert master.loc[0, "local_votacao"] == "Escola Estadual A"
    assert master.loc[0, "cd_setor"] == "355030801000001"
    assert "exact_section_profile" in master.loc[0, "join_strategy"]
    assert "exact_candidate" in master.loc[0, "join_strategy"]
    assert "exact_tse_ibge_code" in master.loc[0, "join_strategy"]
    assert master.loc[0, "source_coverage_score"] == 1.0
    assert 0.9 <= master.loc[0, "join_confidence"] <= 1.0


def test_master_index_documents_approximate_name_join_and_missing_sector() -> None:
    master = MasterIndexBuilder().build(
        resultados_secao=pd.DataFrame(
            [
                {
                    "ano_eleicao": 2024,
                    "uf": "SP",
                    "cod_municipio_tse": "99999",
                    "municipio_nome": "Santa Barbara D Oeste",
                    "zona": "5",
                    "secao": "10",
                    "candidate_id": "321",
                }
            ]
        ),
        municipio_crosswalk=pd.DataFrame(
            [
                {
                    "uf": "SP",
                    "cod_municipio_tse": "70173",
                    "cod_municipio_ibge": "3545803",
                    "municipio_nome": "Santa Bárbara D'Oeste",
                }
            ]
        ),
        setores_censitarios=pd.DataFrame(
            [
                {"cod_municipio_ibge": "3545803", "cd_setor": "354580301000001"},
                {"cod_municipio_ibge": "3545803", "cd_setor": "354580301000002"},
            ]
        ),
    )

    assert master.loc[0, "cod_municipio_ibge"] == "3545803"
    assert "normalized_name_unique" in master.loc[0, "join_strategy"]
    assert "no_sector_match" in master.loc[0, "join_strategy"]
    assert master.loc[0, "cd_setor"] == ""
    assert master.loc[0, "source_coverage_score"] == 2 / 6


def test_master_index_quality_report_and_gold_writer(tmp_path: Path) -> None:
    builder = MasterIndexBuilder()
    master = builder.build(
        resultados_secao=_resultados(),
        municipio_crosswalk=pd.DataFrame(
            [{"uf": "SP", "cod_municipio_tse": "71072", "cod_municipio_ibge": "3550308", "municipio_nome": "Sao Paulo"}]
        ),
    )

    result = builder.write_gold(master, tmp_path, dataset_version="2024_sp_v1")

    assert result.parquet_path is not None and result.parquet_path.exists()
    assert result.manifest_path is not None and result.manifest_path.exists()
    assert result.quality.rows == 1
    assert result.quality.coverage_ibge == 1.0
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["dataset_id"] == "gold_territorial_electoral_master_index"
    assert "join_documentation" in manifest
    assert manifest["quality"]["rows"] == 1


def test_master_index_pipeline_reads_paths(tmp_path: Path) -> None:
    resultados_path = tmp_path / "resultados.parquet"
    crosswalk_path = tmp_path / "crosswalk.parquet"
    _resultados().to_parquet(resultados_path, index=False)
    pd.DataFrame(
        [{"uf": "SP", "cod_municipio_tse": "71072", "cod_municipio_ibge": "3550308", "municipio_nome": "Sao Paulo"}]
    ).to_parquet(crosswalk_path, index=False)

    result = MasterIndexPipeline().run_from_paths(
        resultados_secao_path=resultados_path,
        municipio_crosswalk_path=crosswalk_path,
        output_dir=tmp_path / "gold",
        dataset_version="test",
    )

    assert result.dataframe.loc[0, "cod_municipio_ibge"] == "3550308"
    assert result.parquet_path is not None and result.parquet_path.exists()


def test_master_index_quality_flags_unmatched_ibge() -> None:
    master = MasterIndexBuilder().build(
        resultados_secao=pd.DataFrame(
            [
                {
                    "ano_eleicao": 2024,
                    "uf": "SP",
                    "cod_municipio_tse": "99999",
                    "municipio_nome": "Cidade Sem Match",
                    "zona": "1",
                    "secao": "1",
                }
            ]
        ),
        municipio_crosswalk=pd.DataFrame(
            [{"uf": "SP", "cod_municipio_tse": "71072", "cod_municipio_ibge": "3550308", "municipio_nome": "Sao Paulo"}]
        ),
    )
    quality = MasterIndexBuilder().quality_report(master, dataset_version="test")

    assert master.loc[0, "cod_municipio_ibge"] == ""
    assert "unmatched_tse_ibge" in master.loc[0, "join_strategy"]
    assert quality.coverage_ibge == 0.0
    assert "some rows have no reliable TSE-IBGE municipal match" in quality.limitations
