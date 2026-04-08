from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any
import unicodedata

import pandas as pd

from config.settings import AppPaths
from domain.open_data_contracts import validate_municipio_dimension, validate_municipio_enriched
from infrastructure.dataset_catalog import build_dataset_metadata, register_dataset_version


class OpenDataPipelineError(RuntimeError):
    pass


@dataclass(frozen=True)
class OpenDataInputs:
    base_parquet_path: Path
    mapping_csv_path: Path
    socio_csv_path: Path | None = None
    ano: int | None = None
    mes: int | None = None
    turno: int | None = None


def _ts_now_compact() -> str:
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


def _normalize_text(value: str) -> str:
    ascii_text = (
        unicodedata.normalize("NFKD", str(value))
        .encode("ascii", "ignore")
        .decode("ascii")
        .strip()
        .lower()
    )
    return " ".join(ascii_text.split())


def _pick_column(df: pd.DataFrame, candidates: list[str], *, label: str) -> str:
    lowered = {str(col).lower(): str(col) for col in df.columns}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    raise OpenDataPipelineError(f"coluna obrigatoria '{label}' nao encontrada em {list(df.columns)}")


def _normalize_ibge7(value: Any) -> str:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits.zfill(7) if digits else ""


def _parse_aliases(value: Any) -> list[str]:
    if value is None:
        return []
    parts = [segment.strip() for segment in str(value).split(";")]
    return [part for part in parts if part]


def _build_dim_municipio(mapping_csv_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not mapping_csv_path.exists():
        raise OpenDataPipelineError(f"arquivo de mapeamento nao encontrado: {mapping_csv_path}")
    df = pd.read_csv(mapping_csv_path)
    codigo_tse_col = _pick_column(df, ["codigo_tse", "cod_tse", "cd_mun_tse"], label="codigo_tse")
    codigo_ibge_col = _pick_column(df, ["codigo_ibge", "cod_ibge", "id_municipio_ibge"], label="codigo_ibge")
    nome_col = _pick_column(df, ["nome_municipio", "municipio", "nm_municipio"], label="nome_municipio")

    alias_col = None
    lowered = {str(col).lower(): str(col) for col in df.columns}
    for candidate in ["aliases", "alias", "nome_alias", "municipio_aliases"]:
        if candidate in lowered:
            alias_col = lowered[candidate]
            break

    dim = pd.DataFrame(
        {
            "codigo_tse": df[codigo_tse_col].astype(str).str.strip(),
            "codigo_ibge": df[codigo_ibge_col].astype(str).str.strip(),
            "nome_municipio": df[nome_col].astype(str).str.strip(),
        }
    )
    dim["municipio_id_ibge7"] = dim["codigo_ibge"].map(_normalize_ibge7)
    dim["codigo_ibge"] = dim["municipio_id_ibge7"]
    dim["municipio_norm"] = dim["nome_municipio"].map(_normalize_text)
    dim["aliases"] = (
        df[alias_col].map(_parse_aliases)
        if alias_col
        else dim["nome_municipio"].map(lambda _: [])
    )
    dim = dim.drop_duplicates(subset=["municipio_id_ibge7"]).reset_index(drop=True)
    dim = validate_municipio_dimension(dim)

    alias_rows: list[dict[str, str]] = []
    for _, row in dim.iterrows():
        official = str(row["nome_municipio"]).strip()
        alias_set = {official}
        alias_set.update(row["aliases"] if isinstance(row["aliases"], list) else [])
        for alias in alias_set:
            alias_rows.append(
                {
                    "municipio_id_ibge7": str(row["municipio_id_ibge7"]),
                    "alias_nome": alias,
                    "alias_norm": _normalize_text(alias),
                }
            )
    dim_alias = pd.DataFrame(alias_rows).drop_duplicates(subset=["municipio_id_ibge7", "alias_norm"]).reset_index(drop=True)
    return dim, dim_alias


def _load_socio(socio_csv_path: Path | None) -> pd.DataFrame:
    if socio_csv_path is None or not socio_csv_path.exists():
        return pd.DataFrame(columns=["codigo_ibge"])
    df = pd.read_csv(socio_csv_path)
    if "codigo_ibge" not in {str(c) for c in df.columns}:
        try:
            code_col = _pick_column(df, ["cod_ibge", "id_municipio_ibge"], label="codigo_ibge")
            df = df.rename(columns={code_col: "codigo_ibge"})
        except OpenDataPipelineError:
            return pd.DataFrame(columns=["codigo_ibge"])
    out = df.copy()
    out["codigo_ibge"] = out["codigo_ibge"].map(_normalize_ibge7)
    return out


def _resolve_temporal_fields(base: pd.DataFrame, inputs: OpenDataInputs) -> tuple[pd.Series, pd.Series, pd.Series]:
    ano_series = (
        base["ano"]
        if "ano" in base.columns
        else pd.Series([inputs.ano] * len(base), index=base.index, dtype="Int64")
    )
    if "ano" not in base.columns:
        ano_series = pd.to_numeric(ano_series, errors="coerce").astype("Int64")
    mes_series = (
        base["mes"]
        if "mes" in base.columns
        else pd.Series([inputs.mes] * len(base), index=base.index, dtype="Int64")
    )
    if "mes" not in base.columns:
        mes_series = pd.to_numeric(mes_series, errors="coerce").astype("Int64")
    turno_series = (
        base["turno"]
        if "turno" in base.columns
        else pd.Series([inputs.turno] * len(base), index=base.index, dtype="Int64")
    )
    if "turno" not in base.columns:
        turno_series = pd.to_numeric(turno_series, errors="coerce").astype("Int64")
    return ano_series, mes_series, turno_series


def _build_canonical_key(row: pd.Series) -> str | None:
    municipio_id = str(row.get("municipio_id_ibge7", "") or "").strip()
    ano = row.get("ano")
    mes = row.get("mes")
    turno = row.get("turno")
    if not municipio_id or pd.isna(ano):
        return None
    mes_value = "00" if pd.isna(mes) else str(int(mes)).zfill(2)
    turno_value = "0" if pd.isna(turno) else str(int(turno))
    return f"{municipio_id}:{int(ano)}:{mes_value}:{turno_value}"


def _enrich_base(base_df: pd.DataFrame, dim_municipio: pd.DataFrame, dim_alias: pd.DataFrame, socio_df: pd.DataFrame, inputs: OpenDataInputs) -> pd.DataFrame:
    if "municipio" not in base_df.columns:
        raise OpenDataPipelineError("dataset base sem coluna 'municipio'")
    if "ranking_final" not in base_df.columns:
        raise OpenDataPipelineError("dataset base sem coluna 'ranking_final'")

    base = base_df.copy()
    base["municipio_norm_input"] = base["municipio"].astype(str).map(_normalize_text)
    ano_series, mes_series, turno_series = _resolve_temporal_fields(base, inputs)
    base["ano"] = ano_series
    base["mes"] = mes_series
    base["turno"] = turno_series

    merged = base.merge(
        dim_alias,
        left_on="municipio_norm_input",
        right_on="alias_norm",
        how="left",
        suffixes=("", "_alias"),
    )
    merged = merged.merge(
        dim_municipio[["municipio_id_ibge7", "codigo_tse", "codigo_ibge", "nome_municipio", "municipio_norm"]],
        on="municipio_id_ibge7",
        how="left",
    )
    if not socio_df.empty:
        merged = merged.merge(socio_df, on="codigo_ibge", how="left", suffixes=("", "_socio"))
    merged["join_status"] = merged["municipio_id_ibge7"].map(
        lambda v: "matched" if pd.notna(v) and str(v).strip() else "no_match"
    )
    merged["canonical_key"] = merged.apply(_build_canonical_key, axis=1)
    return validate_municipio_enriched(merged)


def run_open_data_crosswalk_pipeline(
    *,
    paths: AppPaths,
    inputs: OpenDataInputs,
    pipeline_version: str = "open_data_v1",
) -> dict:
    run_id = _ts_now_compact()
    runs_root = paths.data_root / "outputs" / "pipeline_runs" / pipeline_version / run_id
    runs_root.mkdir(parents=True, exist_ok=True)

    if not inputs.base_parquet_path.exists():
        raise OpenDataPipelineError(f"dataset base nao encontrado: {inputs.base_parquet_path}")

    base_df = pd.read_parquet(inputs.base_parquet_path)
    dim_municipio, dim_alias = _build_dim_municipio(inputs.mapping_csv_path)
    socio_df = _load_socio(inputs.socio_csv_path)
    enriched_df = _enrich_base(base_df, dim_municipio, dim_alias, socio_df, inputs)

    outputs_dir = paths.pasta_est
    outputs_dir.mkdir(parents=True, exist_ok=True)
    out_path = outputs_dir / f"df_mun_enriched_{run_id}.parquet"
    enriched_df.to_parquet(out_path, index=False)
    dim_path = outputs_dir / f"dim_municipio_{run_id}.parquet"
    dim_alias_path = outputs_dir / f"dim_municipio_aliases_{run_id}.parquet"
    dim_municipio.to_parquet(dim_path, index=False)
    dim_alias.to_parquet(dim_alias_path, index=False)

    metadata = build_dataset_metadata(
        dataset_name="df_municipios_enriched",
        dataset_version=run_id,
        dataset_path=out_path,
        pipeline_version=pipeline_version,
        run_id=run_id,
    )
    catalog_refs = register_dataset_version(paths, metadata)
    dim_metadata = build_dataset_metadata(
        dataset_name="dim_municipio",
        dataset_version=run_id,
        dataset_path=dim_path,
        pipeline_version=pipeline_version,
        run_id=run_id,
    )
    dim_refs = register_dataset_version(paths, dim_metadata)
    join_rate = float((enriched_df["join_status"] == "matched").mean()) if len(enriched_df) else 0.0

    manifest = {
        "pipeline_version": pipeline_version,
        "run_id": run_id,
        "created_at_utc": datetime.now(UTC).isoformat(),
        "inputs": {
            "base_parquet_path": str(inputs.base_parquet_path),
            "mapping_csv_path": str(inputs.mapping_csv_path),
            "socio_csv_path": str(inputs.socio_csv_path) if inputs.socio_csv_path else None,
        },
        "outputs": {
            "enriched_path": str(out_path),
            "catalog_path": catalog_refs["catalog_path"],
            "catalog_latest_index_path": catalog_refs["latest_index_path"],
            "dim_municipio_path": str(dim_path),
            "dim_municipio_aliases_path": str(dim_alias_path),
            "dim_catalog_path": dim_refs["catalog_path"],
            "dim_catalog_latest_index_path": dim_refs["latest_index_path"],
        },
        "quality": {
            "rows": int(len(enriched_df)),
            "matched_rows": int((enriched_df["join_status"] == "matched").sum()),
            "join_rate": join_rate,
        },
        "source_of_truth": {
            "mapping_dataset": str(inputs.mapping_csv_path),
            "join_key": "municipio_id_ibge7",
        },
    }
    manifest_path = runs_root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "run_id": run_id,
        "manifest_path": str(manifest_path),
        "published_path": str(out_path),
        "dim_municipio_path": str(dim_path),
        "join_rate": join_rate,
    }
