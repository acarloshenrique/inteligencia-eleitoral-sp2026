from __future__ import annotations

import pandas as pd


class OpenDataContractError(ValueError):
    pass


def _missing_columns(df: pd.DataFrame, required: list[str]) -> list[str]:
    return [col for col in required if col not in df.columns]


def validate_municipio_dimension(df: pd.DataFrame) -> pd.DataFrame:
    required = ["municipio_id_ibge7", "codigo_tse", "codigo_ibge", "nome_municipio", "municipio_norm"]
    missing = _missing_columns(df, required)
    if missing:
        raise OpenDataContractError(f"dimensao municipio invalida: colunas ausentes {missing}")

    if df.empty:
        raise OpenDataContractError("dimensao municipio invalida: dataset vazio")

    municipio_id_ibge7 = df["municipio_id_ibge7"].astype(str).str.strip()
    if (municipio_id_ibge7 == "").any():
        raise OpenDataContractError("dimensao municipio invalida: municipio_id_ibge7 vazio")

    codigo_ibge = df["codigo_ibge"].astype(str).str.strip()
    if (codigo_ibge == "").any():
        raise OpenDataContractError("dimensao municipio invalida: codigo_ibge vazio")

    return df


def validate_municipio_enriched(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "municipio",
        "ranking_final",
        "municipio_id_ibge7",
        "ano",
        "turno",
        "canonical_key",
        "join_status",
        "join_method",
        "join_confidence",
        "needs_review",
    ]
    missing = _missing_columns(df, required)
    if missing:
        raise OpenDataContractError(f"dataset enriquecido invalido: colunas ausentes {missing}")

    allowed_status = {"matched", "no_match", "manual_review"}
    status_values = {str(v) for v in df["join_status"].dropna().unique().tolist()}
    if not status_values.issubset(allowed_status):
        raise OpenDataContractError("dataset enriquecido invalido: join_status fora do padrao")

    return df


def validate_silver_fato_municipio(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "municipio_id_ibge7",
        "ano",
        "mes",
        "turno",
        "canonical_key",
        "municipio",
        "ranking_final",
        "join_status",
        "join_method",
        "join_confidence",
        "needs_review",
    ]
    missing = _missing_columns(df, required)
    if missing:
        raise OpenDataContractError(f"silver.fato_eleitoral_municipio invalido: colunas ausentes {missing}")
    if df.empty:
        raise OpenDataContractError("silver.fato_eleitoral_municipio invalido: dataset vazio")
    return df


def validate_silver_dim_municipio(df: pd.DataFrame) -> pd.DataFrame:
    return validate_municipio_dimension(df)


def validate_silver_dim_territorio(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "territorio_id",
        "cod_tse_municipio",
        "cod_ibge_municipio",
        "uf",
        "nome_padronizado",
        "zona_eleitoral",
        "secao_eleitoral",
        "latitude",
        "longitude",
        "geohash",
        "vigencia_inicio",
        "vigencia_fim",
    ]
    missing = _missing_columns(df, required)
    if missing:
        raise OpenDataContractError(f"silver.dim_territorio invalido: colunas ausentes {missing}")
    if df.empty:
        raise OpenDataContractError("silver.dim_territorio invalido: dataset vazio")
    territorio_id = df["territorio_id"].astype(str).str.strip()
    if (territorio_id == "").any():
        raise OpenDataContractError("silver.dim_territorio invalido: territorio_id vazio")
    return df


def validate_silver_dim_tempo(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "tempo_id",
        "data",
        "ano",
        "mes",
        "dia",
        "semana_iso",
        "dia_semana",
        "ciclo_eleitoral",
        "fase_calendario",
        "is_historico_eleitoral",
        "is_pre_campanha",
        "is_janela_campanha",
        "is_evento",
        "evento",
        "tipo_evento",
        "pulso_midia",
        "is_pulso_midia",
    ]
    missing = _missing_columns(df, required)
    if missing:
        raise OpenDataContractError(f"silver.dim_tempo invalida: colunas ausentes {missing}")
    if df.empty:
        raise OpenDataContractError("silver.dim_tempo invalida: dataset vazio")
    tempo_id = df["tempo_id"].astype(str).str.strip()
    if (tempo_id == "").any():
        raise OpenDataContractError("silver.dim_tempo invalida: tempo_id vazio")
    return df


def validate_gold_mart_municipio_eleitoral(df: pd.DataFrame) -> pd.DataFrame:
    required = ["canonical_key", "municipio_id_ibge7", "ano", "turno", "ranking_final"]
    missing = _missing_columns(df, required)
    if missing:
        raise OpenDataContractError(f"gold.mart_municipio_eleitoral invalido: colunas ausentes {missing}")
    return df
