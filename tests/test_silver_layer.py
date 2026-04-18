from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ingestion.silver import (
    MunicipalCrosswalk,
    SilverDatasetTransformer,
    SilverDatasetWriter,
    fix_mojibake,
    normalize_column_name,
    normalize_money,
    normalize_territory_name,
)
from ingestion.silver_contracts import SILVER_CONTRACTS


def test_column_and_text_normalization_handles_encoding_and_accents() -> None:
    assert normalize_column_name("NÃºmero Candidato") == "numero_candidato"
    assert fix_mojibake("SeÃ§Ã£o eleitoral") == "Seção eleitoral"
    assert normalize_territory_name("São José do Rio Preto") == "SAO JOSE DO RIO PRETO"


def test_silver_transformer_harmonizes_tse_keys_and_crosswalk() -> None:
    raw = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": "2024",
                "SG_UF": "sp",
                "CD_MUNICIPIO": "71072",
                "NM_MUNICIPIO": "SÃ£o Paulo",
                "NR_ZONA": "1",
                "NR_SECAO": "25",
                "SQ_CANDIDATO": "123",
                "NR_CANDIDATO": "13",
                "SG_PARTIDO": "pt",
            },
            {
                "ANO_ELEICAO": "2024",
                "SG_UF": "SP",
                "CD_MUNICIPIO": "71072",
                "NM_MUNICIPIO": "São Paulo",
                "NR_ZONA": "0001",
                "NR_SECAO": "0025",
                "SQ_CANDIDATO": "123",
                "NR_CANDIDATO": "13",
                "SG_PARTIDO": "PT",
            },
        ]
    )
    crosswalk = MunicipalCrosswalk.from_dataframe(
        pd.DataFrame(
            [
                {
                    "uf": "SP",
                    "cod_municipio_tse": "71072",
                    "cod_municipio_ibge": "3550308",
                    "municipio_nome": "São Paulo",
                }
            ]
        )
    )

    result = SilverDatasetTransformer.for_dataset("tse_resultados_secao", crosswalk=crosswalk).transform(
        raw,
        source_dataset="tse.boletim_urna",
        source_file="bronze.zip",
        ingestion_timestamp="2026-01-01T00:00:00+00:00",
    )

    df = result.dataframe
    assert result.quality.status == "ok"
    assert result.quality.rows_input == 2
    assert result.quality.rows_output == 1
    assert result.quality.duplicates_removed == 1
    assert df.loc[0, "uf"] == "SP"
    assert df.loc[0, "cod_municipio_tse"] == "71072"
    assert df.loc[0, "cod_municipio_ibge"] == "3550308"
    assert df.loc[0, "municipio_nome_normalizado"] == "SAO PAULO"
    assert df.loc[0, "zona"] == "0001"
    assert df.loc[0, "secao"] == "0025"
    assert df.loc[0, "candidate_id"] == "123"
    assert df.loc[0, "partido"] == "PT"
    assert df.loc[0, "join_confidence"] == 0.98
    assert {"source_dataset", "source_file", "ingestion_timestamp", "transform_timestamp"}.issubset(df.columns)


def test_crosswalk_falls_back_to_unique_normalized_name() -> None:
    raw = pd.DataFrame([{"ano_eleicao": "2024", "uf": "SP", "municipio_nome": "Sao Paulo", "zona": "1", "secao": "1"}])
    crosswalk = MunicipalCrosswalk.from_dataframe(
        pd.DataFrame(
            [{"uf": "SP", "cod_municipio_tse": "71072", "cod_municipio_ibge": "3550308", "municipio_nome": "São Paulo"}]
        )
    )

    enriched = crosswalk.enrich(raw)

    assert enriched.loc[0, "cod_municipio_ibge"] == "3550308"
    assert enriched.loc[0, "join_confidence"] == 0.86


def test_money_and_date_fields_are_normalized_for_campaign_finance() -> None:
    raw = pd.DataFrame(
        [
            {
                "ANO": "2024",
                "SG_UF": "SP",
                "SQ_CANDIDATO": "999",
                "SG_PARTIDO": "MDB",
                "VR_RECEITA": "1.234,56",
                "DT_RECEITA": "31/10/2024",
            }
        ]
    )

    result = SilverDatasetTransformer.for_dataset("tse_prestacao_contas").transform(
        raw,
        source_dataset="tse.prestacao_contas",
        source_file="contas.zip",
        ingestion_timestamp="2026-01-01T00:00:00+00:00",
    )

    df = result.dataframe
    assert result.quality.status == "ok"
    assert normalize_money("R$ 1.234,56") == 1234.56
    assert float(df.loc[0, "valor_receita"]) == 1234.56
    assert df.loc[0, "data_receita"] == "2024-10-31"


def test_schema_validation_reports_missing_required_columns() -> None:
    raw = pd.DataFrame([{"ano_eleicao": "2024", "uf": "SP"}])

    result = SilverDatasetTransformer.for_dataset("tse_candidatos").transform(
        raw,
        source_dataset="tse.candidatos",
        source_file="cand.zip",
        ingestion_timestamp="2026-01-01T00:00:00+00:00",
    )

    assert result.quality.status == "failed"
    assert "candidate_id" in result.quality.missing_required_columns
    assert result.quality.schema_errors


def test_silver_writer_persists_parquet_and_quality_report(tmp_path: Path) -> None:
    raw = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": "2024",
                "SG_UF": "SP",
                "SQ_CANDIDATO": "123",
                "NR_CANDIDATO": "45",
                "SG_PARTIDO": "PSD",
                "DS_CARGO": "Vereador",
            }
        ]
    )
    result = SilverDatasetTransformer.for_dataset("tse_candidatos").transform(
        raw,
        source_dataset="tse.candidatos",
        source_file="cand.zip",
        ingestion_timestamp="2026-01-01T00:00:00+00:00",
    )

    parquet_path, quality_path = SilverDatasetWriter().write(
        result,
        destination_dir=tmp_path / "silver" / "tse_candidatos",
        dataset_id="tse_candidatos",
    )

    assert parquet_path.exists()
    assert quality_path.exists()
    persisted = pd.read_parquet(parquet_path)
    report = json.loads(quality_path.read_text(encoding="utf-8"))
    assert len(persisted) == 1
    assert report["dataset_id"] == "tse_candidatos"


def test_silver_contracts_cover_core_datasets() -> None:
    assert {
        "tse_resultados_secao",
        "tse_eleitorado_secao",
        "tse_eleitorado_local_votacao",
        "tse_candidatos",
        "tse_prestacao_contas",
        "ibge_malha_setores",
        "ibge_agregados_censo",
    }.issubset(SILVER_CONTRACTS)
