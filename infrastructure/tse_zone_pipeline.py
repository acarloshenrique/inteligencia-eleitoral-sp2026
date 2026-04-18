from __future__ import annotations

import json
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from config.settings import AppPaths
from infrastructure.dataset_catalog import build_dataset_metadata, register_dataset_version
from infrastructure.load_manifest import build_load_manifest


class TSEZoneIngestionError(RuntimeError):
    pass


@dataclass(frozen=True)
class TSEZoneInputs:
    eleitorado_path: Path
    resultados_path: Path | None = None
    uf: str = "SP"
    ano_eleicao: int = 2024
    turno: int = 1


def _ts_now_compact() -> str:
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


def _read_csv_or_zip(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise TSEZoneIngestionError(f"arquivo TSE nao encontrado: {path}")
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as archive:
            names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if not names:
                raise TSEZoneIngestionError(f"zip TSE sem CSV: {path}")
            with archive.open(names[0]) as handle:
                return pd.read_csv(handle, sep=";", encoding="latin1", dtype=str, low_memory=False)
    return pd.read_csv(path, sep=";", encoding="latin1", dtype=str, low_memory=False)


def _clean_col(value: Any) -> str:
    return str(value).strip().lower().replace(" ", "_").replace("-", "_")


def _pick_column(df: pd.DataFrame, candidates: list[str], *, required: bool = True) -> str | None:
    normalized = {_clean_col(col): str(col) for col in df.columns}
    for candidate in candidates:
        key = _clean_col(candidate)
        if key in normalized:
            return normalized[key]
    if required:
        raise TSEZoneIngestionError(
            f"coluna TSE obrigatoria nao encontrada: {candidates}. Disponiveis: {list(df.columns)}"
        )
    return None


def _digits(value: Any) -> str:
    return "".join(ch for ch in str(value) if ch.isdigit())


def _to_int(value: Any, default: int = 0) -> int:
    digits = _digits(value)
    return int(digits) if digits else default


def _to_float_pct(value: Any, default: float = 0.0) -> float:
    text = str(value).strip().replace("%", "").replace(".", "").replace(",", ".")
    try:
        number = float(text)
    except ValueError:
        return default
    return number / 100 if number > 1 else number


def _to_float(value: Any, default: float = 0.0) -> float:
    text = str(value).strip().replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return default


def normalize_tse_eleitorado_zona(
    df: pd.DataFrame, *, uf: str, ano_eleicao: int, turno: int
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        raise TSEZoneIngestionError("arquivo de eleitorado TSE vazio")

    uf_col = _pick_column(df, ["SG_UF", "UF"], required=False)
    municipio_col = _pick_column(df, ["NM_MUNICIPIO", "NOME_MUNICIPIO", "MUNICIPIO"])
    cod_tse_col = _pick_column(df, ["CD_MUNICIPIO", "CODIGO_MUNICIPIO", "COD_MUN_TSE"], required=False)
    zona_col = _pick_column(df, ["NR_ZONA", "ZONA", "ZONA_ELEITORAL"])
    secao_col = _pick_column(df, ["NR_SECAO", "SECAO", "SECAO_ELEITORAL"], required=False)
    local_col = _pick_column(df, ["NM_LOCAL_VOTACAO", "LOCAL_VOTACAO", "NOME_LOCAL"], required=False)
    endereco_col = _pick_column(df, ["DS_ENDERECO", "ENDERECO"], required=False)
    bairro_col = _pick_column(df, ["NM_BAIRRO", "BAIRRO"], required=False)
    cep_col = _pick_column(df, ["NR_CEP", "CEP"], required=False)
    latitude_col = _pick_column(df, ["NR_LATITUDE", "LATITUDE"], required=False)
    longitude_col = _pick_column(df, ["NR_LONGITUDE", "LONGITUDE"], required=False)
    eleitores_col = _pick_column(
        df,
        [
            "QT_ELEITOR",
            "QT_ELEITORES",
            "QT_ELEITORES_PERFIL",
            "QT_ELEITOR_SECAO",
            "QTD_ELEITORES",
            "ELEITORES_APTOS",
            "QT_APTOS",
        ],
    )

    raw = df.copy()
    if uf_col is not None:
        raw = raw[raw[uf_col].astype(str).str.upper().eq(uf.upper())]

    secao_series = raw[secao_col].map(_to_int) if secao_col else pd.Series([0] * len(raw), index=raw.index)
    cod_tse_series = raw[cod_tse_col].map(_digits) if cod_tse_col else pd.Series([""] * len(raw), index=raw.index)
    section = pd.DataFrame(
        {
            "uf": uf.upper(),
            "ano_eleicao": int(ano_eleicao),
            "turno": int(turno),
            "municipio": raw[municipio_col].astype(str).str.strip(),
            "cod_tse_municipio": cod_tse_series,
            "zona_eleitoral": raw[zona_col].map(_to_int),
            "secao_eleitoral": secao_series,
            "local_votacao": raw[local_col].astype(str).str.strip() if local_col else "",
            "endereco": raw[endereco_col].astype(str).str.strip() if endereco_col else "",
            "bairro": raw[bairro_col].astype(str).str.strip() if bairro_col else "",
            "cep": raw[cep_col].astype(str).str.strip() if cep_col else "",
            "latitude": raw[latitude_col].map(_to_float) if latitude_col else 0.0,
            "longitude": raw[longitude_col].map(_to_float) if longitude_col else 0.0,
            "eleitores_aptos": raw[eleitores_col].map(_to_int),
        }
    )
    section = section[section["municipio"].astype(str).str.len().gt(0) & section["zona_eleitoral"].gt(0)]
    section_cols = [
        "uf",
        "ano_eleicao",
        "turno",
        "municipio",
        "cod_tse_municipio",
        "zona_eleitoral",
        "secao_eleitoral",
        "local_votacao",
        "endereco",
        "bairro",
        "cep",
        "latitude",
        "longitude",
    ]
    section = section.groupby(section_cols, dropna=False).agg(eleitores_aptos=("eleitores_aptos", "sum")).reset_index()
    section["fonte"] = "tse_eleitorado_zona_secao"
    section["data_referencia"] = str(ano_eleicao)
    section["data_quality_score"] = 0.94
    section["join_confidence"] = section["cod_tse_municipio"].astype(str).str.len().gt(0).map({True: 0.96, False: 0.82})

    grouped = section.groupby(
        ["uf", "ano_eleicao", "turno", "municipio", "cod_tse_municipio", "zona_eleitoral"], dropna=False
    )
    zone = grouped.agg(
        eleitores_aptos=("eleitores_aptos", "sum"),
        secoes_total=("secao_eleitoral", "nunique"),
        latitude=("latitude", "mean"),
        longitude=("longitude", "mean"),
    ).reset_index()
    zone["municipio_id_ibge7"] = ""
    zone["territorio_id"] = (
        zone["uf"].astype(str)
        + ":"
        + zone["cod_tse_municipio"].replace("", "SEM_COD").astype(str)
        + ":ZE"
        + zone["zona_eleitoral"].astype(str)
    )
    zone["zona_id"] = zone["uf"].astype(str) + "-" + zone["zona_eleitoral"].astype(str)
    zone["fonte"] = "tse_eleitorado_zona_secao"
    zone["data_referencia"] = str(ano_eleicao)
    zone["match_method"] = "exact_tse_municipio_zone"
    zone["data_quality_score"] = 0.94
    zone["join_confidence"] = zone["cod_tse_municipio"].astype(str).str.len().gt(0).map({True: 0.96, False: 0.82})
    zone["abstencao_pct"] = 0.0
    zone["competitividade"] = 0.5
    zone["volatilidade_historica"] = 0.25
    return zone, section


def normalize_tse_resultados_zona(df: pd.DataFrame, *, uf: str, ano_eleicao: int, turno: int) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    uf_col = _pick_column(df, ["SG_UF", "UF"], required=False)
    ano_col = _pick_column(df, ["ANO_ELEICAO", "ANO"], required=False)
    turno_col = _pick_column(df, ["NR_TURNO", "TURNO"], required=False)
    election_code_col = _pick_column(df, ["CD_ELEICAO", "CODIGO_ELEICAO"], required=False)
    election_name_col = _pick_column(df, ["DS_ELEICAO", "ELEICAO"], required=False)
    municipio_col = _pick_column(df, ["NM_MUNICIPIO", "NOME_MUNICIPIO", "MUNICIPIO"])
    cod_tse_col = _pick_column(df, ["CD_MUNICIPIO", "CODIGO_MUNICIPIO", "COD_MUN_TSE"], required=False)
    zona_col = _pick_column(df, ["NR_ZONA", "ZONA", "ZONA_ELEITORAL"])
    secao_col = _pick_column(df, ["NR_SECAO", "SECAO", "SECAO_ELEITORAL"], required=False)
    votos_col = _pick_column(df, ["QT_VOTOS", "VOTOS", "QT_VOTOS_NOMINAIS", "VOTOS_VALIDOS"])
    cargo_code_col = _pick_column(df, ["CD_CARGO", "CODIGO_CARGO"], required=False)
    cargo_name_col = _pick_column(df, ["DS_CARGO", "CARGO"], required=False)
    votavel_col = _pick_column(df, ["SQ_CANDIDATO", "NR_VOTAVEL", "NM_VOTAVEL"], required=False)

    raw = df.copy()
    if uf_col is not None:
        raw = raw[raw[uf_col].astype(str).str.upper().eq(uf.upper())]
    if ano_col is not None:
        raw = raw[raw[ano_col].map(_to_int).eq(int(ano_eleicao))]
    if turno_col is not None:
        raw = raw[raw[turno_col].map(_to_int).eq(int(turno))]
    if election_code_col is not None:
        eleicao_regular = raw[election_code_col].astype(str).str.strip().eq("619")
        if eleicao_regular.any():
            raw = raw[eleicao_regular]
    elif election_name_col is not None:
        eleicao_regular = raw[election_name_col].astype(str).str.contains(
            "Elei??es Municipais 2024", case=False, na=False
        )
        if eleicao_regular.any():
            raw = raw[eleicao_regular]
    if cargo_code_col is not None:
        prefeito = raw[cargo_code_col].astype(str).str.strip().eq("11")
        if prefeito.any():
            raw = raw[prefeito]
    elif cargo_name_col is not None:
        prefeito = raw[cargo_name_col].astype(str).str.contains("prefeito", case=False, na=False)
        if prefeito.any():
            raw = raw[prefeito]

    cod_tse_series = raw[cod_tse_col].map(_digits) if cod_tse_col else pd.Series([""] * len(raw), index=raw.index)
    secao_series = raw[secao_col].map(_to_int) if secao_col else pd.Series([0] * len(raw), index=raw.index)
    votavel_series = raw[votavel_col].astype(str).str.strip() if votavel_col else pd.Series([""] * len(raw), index=raw.index)
    out = pd.DataFrame(
        {
            "uf": uf.upper(),
            "ano_eleicao": int(ano_eleicao),
            "turno": int(turno),
            "municipio": raw[municipio_col].astype(str).str.strip(),
            "cod_tse_municipio": cod_tse_series,
            "zona_eleitoral": raw[zona_col].map(_to_int),
            "secao_eleitoral": secao_series,
            "votavel_id": votavel_series,
            "votos_validos": raw[votos_col].map(_to_int),
        }
    )
    out = out[out["zona_eleitoral"].gt(0)]
    return (
        out.groupby(
            [
                "uf",
                "ano_eleicao",
                "turno",
                "municipio",
                "cod_tse_municipio",
                "zona_eleitoral",
                "secao_eleitoral",
                "votavel_id",
            ],
            dropna=False,
        )
        .agg(votos_validos=("votos_validos", "sum"))
        .reset_index()
    )


def _competitividade_por_zona(result_sections: pd.DataFrame) -> pd.DataFrame:
    keys = ["uf", "ano_eleicao", "turno", "cod_tse_municipio", "zona_eleitoral"]
    if result_sections.empty or "votavel_id" not in result_sections.columns:
        return pd.DataFrame(columns=[*keys, "competitividade"])

    candidate_votes = (
        result_sections[result_sections["votavel_id"].astype(str).str.len().gt(0)]
        .groupby([*keys, "votavel_id"], dropna=False)
        .agg(votos_candidato=("votos_validos", "sum"))
        .reset_index()
    )
    if candidate_votes.empty:
        return pd.DataFrame(columns=[*keys, "competitividade"])

    def score(group: pd.DataFrame) -> float:
        votes = group["votos_candidato"].sort_values(ascending=False).tolist()
        total = float(sum(votes))
        if total <= 0 or len(votes) < 2:
            return 0.0
        margin = (votes[0] - votes[1]) / total
        return max(0.0, min(1.0, 1.0 - margin))

    return candidate_votes.groupby(keys, dropna=False).apply(score, include_groups=False).reset_index(name="competitividade")


def _merge_results(zone: pd.DataFrame, result_sections: pd.DataFrame) -> pd.DataFrame:
    if result_sections.empty:
        zone["votos_validos"] = 0
        zone["abstencoes"] = 0
        zone["comparecimento"] = 0.0
        return zone
    votes = (
        result_sections.groupby(
            ["uf", "ano_eleicao", "turno", "cod_tse_municipio", "zona_eleitoral"], dropna=False
        )
        .agg(votos_validos=("votos_validos", "sum"))
        .reset_index()
    )
    merged = zone.merge(
        votes, on=["uf", "ano_eleicao", "turno", "cod_tse_municipio", "zona_eleitoral"], how="left"
    )
    competitiveness = _competitividade_por_zona(result_sections)
    if not competitiveness.empty:
        merged = merged.merge(
            competitiveness,
            on=["uf", "ano_eleicao", "turno", "cod_tse_municipio", "zona_eleitoral"],
            how="left",
            suffixes=("", "_real"),
        )
        merged["competitividade"] = pd.to_numeric(
            merged["competitividade_real"], errors="coerce"
        ).fillna(merged["competitividade"])
        merged = merged.drop(columns=["competitividade_real"])
    merged["votos_validos"] = pd.to_numeric(merged["votos_validos"], errors="coerce").fillna(0).astype(int)
    merged["comparecimento"] = (merged["votos_validos"] / merged["eleitores_aptos"].replace(0, 1)).clip(0, 1)
    merged["abstencao_pct"] = (1 - merged["comparecimento"]).clip(0, 1)
    merged["abstencoes"] = (merged["eleitores_aptos"] * merged["abstencao_pct"]).round(0).astype(int)
    return merged


def run_tse_zone_section_pipeline(
    *,
    paths: AppPaths,
    inputs: TSEZoneInputs,
    pipeline_version: str = "tse_zone_section_v1",
) -> dict[str, Any]:
    run_id = _ts_now_compact()
    runs_root = paths.ingestion_root / "pipeline_runs" / pipeline_version / run_id
    runs_root.mkdir(parents=True, exist_ok=True)
    paths.silver_root.mkdir(parents=True, exist_ok=True)
    paths.gold_root.mkdir(parents=True, exist_ok=True)

    eleitorado_raw = _read_csv_or_zip(inputs.eleitorado_path)
    zone, section = normalize_tse_eleitorado_zona(
        eleitorado_raw, uf=inputs.uf, ano_eleicao=inputs.ano_eleicao, turno=inputs.turno
    )
    resultados = pd.DataFrame()
    if inputs.resultados_path is not None and inputs.resultados_path.exists():
        resultados = normalize_tse_resultados_zona(
            _read_csv_or_zip(inputs.resultados_path), uf=inputs.uf, ano_eleicao=inputs.ano_eleicao, turno=inputs.turno
        )
    fact = _merge_results(zone, resultados)

    dim_cols = [
        "territorio_id",
        "uf",
        "municipio",
        "municipio_id_ibge7",
        "cod_tse_municipio",
        "zona_eleitoral",
        "zona_id",
        "latitude",
        "longitude",
        "fonte",
        "data_referencia",
        "match_method",
        "data_quality_score",
        "join_confidence",
    ]
    feature_cols = [
        "territorio_id",
        "municipio",
        "municipio_id_ibge7",
        "zona_eleitoral",
        "eleitores_aptos",
        "abstencao_pct",
        "competitividade",
        "volatilidade_historica",
        "data_quality_score",
        "join_confidence",
    ]
    dim = fact[dim_cols].drop_duplicates("territorio_id").reset_index(drop=True)
    features = fact[feature_cols].copy()

    section_path = paths.silver_root / f"fact_secao_eleitoral_{run_id}.parquet"
    dim_path = paths.silver_root / f"dim_territorio_eleitoral_{run_id}.parquet"
    fact_path = paths.gold_root / f"fact_zona_eleitoral_{run_id}.parquet"
    features_path = paths.gold_root / f"features_zona_eleitoral_{run_id}.parquet"
    section.to_parquet(section_path, index=False)
    dim.to_parquet(dim_path, index=False)
    fact.to_parquet(fact_path, index=False)
    features.to_parquet(features_path, index=False)

    catalog_refs = []
    for name, output_path in [
        ("fact_secao_eleitoral", section_path),
        ("dim_territorio_eleitoral", dim_path),
        ("fact_zona_eleitoral", fact_path),
        ("features_zona_eleitoral", features_path),
    ]:
        catalog_refs.append(
            register_dataset_version(
                paths,
                build_dataset_metadata(
                    dataset_name=name,
                    dataset_version=run_id,
                    dataset_path=output_path,
                    pipeline_version=pipeline_version,
                    run_id=run_id,
                ),
            )
        )

    quality = {
        "status": "ok" if len(fact) else "warning",
        "rows_zona": int(len(fact)),
        "rows_secao": int(len(section)),
        "coverage_zonas": int(fact["territorio_id"].nunique()) if not fact.empty else 0,
        "mean_data_quality_score": round(float(fact["data_quality_score"].mean()), 6) if not fact.empty else 0.0,
        "mean_join_confidence": round(float(fact["join_confidence"].mean()), 6) if not fact.empty else 0.0,
    }
    manifest = {
        "pipeline_version": pipeline_version,
        "run_id": run_id,
        "created_at_utc": datetime.now(UTC).isoformat(),
        "inputs": {
            "eleitorado_path": str(inputs.eleitorado_path),
            "resultados_path": str(inputs.resultados_path) if inputs.resultados_path else None,
            "uf": inputs.uf,
            "ano_eleicao": inputs.ano_eleicao,
            "turno": inputs.turno,
        },
        "outputs": {
            "fact_secao_eleitoral_path": str(section_path),
            "dim_territorio_eleitoral_path": str(dim_path),
            "fact_zona_eleitoral_path": str(fact_path),
            "features_zona_eleitoral_path": str(features_path),
        },
        "quality": quality,
        "dataset_manifest": build_load_manifest(
            source_name="tse_zone_section",
            collected_at_utc=datetime.now(UTC).isoformat(),
            dataset_path=fact_path,
            df=fact,
            parser_version=pipeline_version,
            quality=quality,
        ),
        "catalog_refs": catalog_refs,
        "lgpd_classification": "public_open_data_aggregated",
    }
    manifest_path = runs_root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "run_id": run_id,
        "manifest_path": str(manifest_path),
        "fact_zona_eleitoral_path": str(fact_path),
        "dim_territorio_eleitoral_path": str(dim_path),
        "features_zona_eleitoral_path": str(features_path),
        "quality": quality,
    }
