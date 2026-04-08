from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
from typing import Any
import unicodedata

import pandas as pd

from config.settings import AppPaths, get_settings
from domain.open_data_contracts import (
    validate_gold_mart_municipio_eleitoral,
    validate_silver_dim_municipio,
    validate_silver_fato_municipio,
)
from infrastructure.data_quality import (
    compute_drift_score,
    compute_join_success,
    compute_null_critical,
    compute_update_delay_days,
    find_previous_dataset_path,
)
from infrastructure.dataset_catalog import build_dataset_metadata, register_dataset_version
from infrastructure.lgpd_compliance import apply_lgpd_purpose_policy, enforce_retention_policy
from infrastructure.source_contracts import validate_input_contracts


class MedallionPipelineError(RuntimeError):
    pass


@dataclass(frozen=True)
class MedallionInputs:
    base_parquet_path: Path
    mapping_csv_path: Path
    socio_csv_path: Path | None = None
    secao_csv_path: Path | None = None
    ibge_csv_path: Path | None = None
    seade_csv_path: Path | None = None
    fiscal_csv_path: Path | None = None
    ano: int | None = None
    mes: int | None = None
    turno: int | None = None
    window_cycles: int = 3
    uf: str = "SP"


def _ts_now_compact() -> str:
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fp:
        while True:
            chunk = fp.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_text(value: str) -> str:
    ascii_text = (
        unicodedata.normalize("NFKD", str(value))
        .encode("ascii", "ignore")
        .decode("ascii")
        .strip()
        .lower()
    )
    return " ".join(ascii_text.split())


def _normalize_ibge7(value: Any) -> str:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits.zfill(7) if digits else ""


def _pick_column(df: pd.DataFrame, candidates: list[str], *, label: str) -> str:
    lowered = {str(col).lower(): str(col) for col in df.columns}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    raise MedallionPipelineError(f"coluna obrigatoria '{label}' nao encontrada em {list(df.columns)}")


def _resolve_temporal_fields(base: pd.DataFrame, inputs: MedallionInputs) -> tuple[pd.Series, pd.Series, pd.Series]:
    ano = base["ano"] if "ano" in base.columns else pd.Series([inputs.ano] * len(base), index=base.index, dtype="Int64")
    mes = base["mes"] if "mes" in base.columns else pd.Series([inputs.mes] * len(base), index=base.index, dtype="Int64")
    turno = (
        base["turno"] if "turno" in base.columns else pd.Series([inputs.turno] * len(base), index=base.index, dtype="Int64")
    )
    return (
        pd.to_numeric(ano, errors="coerce").astype("Int64"),
        pd.to_numeric(mes, errors="coerce").astype("Int64"),
        pd.to_numeric(turno, errors="coerce").astype("Int64"),
    )


def _build_canonical_key(df: pd.DataFrame) -> pd.Series:
    def _row_key(row: pd.Series) -> str | None:
        municipio_id = str(row.get("municipio_id_ibge7", "") or "").strip()
        ano = row.get("ano")
        mes = row.get("mes")
        turno = row.get("turno")
        if not municipio_id or pd.isna(ano):
            return None
        mes_value = "00" if pd.isna(mes) else str(int(mes)).zfill(2)
        turno_value = "0" if pd.isna(turno) else str(int(turno))
        return f"{municipio_id}:{int(ano)}:{mes_value}:{turno_value}"

    return df.apply(_row_key, axis=1)


def _safe_read(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _state_path(lake_root: Path) -> Path:
    return lake_root / "_incremental_state.json"


def _load_incremental_state(lake_root: Path) -> dict[str, Any]:
    state_file = _state_path(lake_root)
    if not state_file.exists():
        return {}
    try:
        return json.loads(state_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _save_incremental_state(lake_root: Path, state: dict[str, Any]) -> None:
    state_file = _state_path(lake_root)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_partitioned_parquet(
    df: pd.DataFrame,
    *,
    layer_root: Path,
    source_name: str,
    run_id: str,
    default_ano: int | None,
    default_uf: str,
) -> list[str]:
    if df.empty:
        return []
    out = df.copy()
    if "ano" not in out.columns:
        out["ano"] = default_ano if default_ano is not None else 0
    out["ano"] = pd.to_numeric(out["ano"], errors="coerce").fillna(0).astype(int)
    if "uf" not in out.columns:
        out["uf"] = default_uf
    out["uf"] = out["uf"].astype(str).str.upper().str.strip().replace("", default_uf)
    out["fonte"] = source_name

    written: list[str] = []
    for (ano, uf), part in out.groupby(["ano", "uf"], dropna=False):
        target_dir = layer_root / f"fonte={source_name}" / f"ano={int(ano)}" / f"uf={str(uf)}"
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = target_dir / f"part-{run_id}.parquet"
        part.to_parquet(file_path, index=False)
        written.append(str(file_path))
    return written


def _write_bronze_asset(
    source_name: str,
    source_path: Path,
    bronze_dir: Path,
    *,
    lake_bronze_root: Path,
    run_id: str,
    default_ano: int | None,
    default_uf: str,
) -> dict[str, Any]:
    bronze_dir.mkdir(parents=True, exist_ok=True)
    target = bronze_dir / f"{source_name}{source_path.suffix.lower()}"
    payload = source_path.read_bytes()
    target.write_bytes(payload)
    rows = len(_safe_read(target))
    hash_sha256 = _sha256_file(target)
    source_last_modified_utc = datetime.fromtimestamp(source_path.stat().st_mtime, UTC).isoformat()
    part_paths = _write_partitioned_parquet(
        _safe_read(target),
        layer_root=lake_bronze_root,
        source_name=source_name,
        run_id=run_id,
        default_ano=default_ano,
        default_uf=default_uf,
    )
    return {
        "source": source_name,
        "path": str(target),
        "dt_coleta_utc": datetime.now(UTC).isoformat(),
        "hash_sha256": hash_sha256,
        "rows": int(rows),
        "partitions": part_paths,
        "source_last_modified_utc": source_last_modified_utc,
    }


def _build_dim_municipio(mapping_csv_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    mapping_df = pd.read_csv(mapping_csv_path)
    codigo_tse_col = _pick_column(mapping_df, ["codigo_tse", "cod_tse", "cd_mun_tse"], label="codigo_tse")
    codigo_ibge_col = _pick_column(mapping_df, ["codigo_ibge", "cod_ibge", "id_municipio_ibge"], label="codigo_ibge")
    nome_col = _pick_column(mapping_df, ["nome_municipio", "municipio", "nm_municipio"], label="nome_municipio")
    alias_col = None
    lowered = {str(col).lower(): str(col) for col in mapping_df.columns}
    for candidate in ["aliases", "alias", "nome_alias", "municipio_aliases"]:
        if candidate in lowered:
            alias_col = lowered[candidate]
            break

    dim = pd.DataFrame(
        {
            "codigo_tse": mapping_df[codigo_tse_col].astype(str).str.strip(),
            "codigo_ibge": mapping_df[codigo_ibge_col].map(_normalize_ibge7),
            "nome_municipio": mapping_df[nome_col].astype(str).str.strip(),
        }
    )
    dim["municipio_id_ibge7"] = dim["codigo_ibge"]
    dim["municipio_norm"] = dim["nome_municipio"].map(_normalize_text)
    dim = dim.drop_duplicates(subset=["municipio_id_ibge7"]).reset_index(drop=True)
    dim = validate_silver_dim_municipio(dim)

    alias_rows: list[dict[str, str]] = []
    for _, row in dim.iterrows():
        aliases = {str(row["nome_municipio"]).strip()}
        if alias_col:
            src = mapping_df.loc[mapping_df[codigo_ibge_col].map(_normalize_ibge7) == row["municipio_id_ibge7"], alias_col]
            if not src.empty:
                raw_aliases = str(src.iloc[0]).split(";")
                aliases.update([a.strip() for a in raw_aliases if a.strip()])
        for alias in aliases:
            alias_rows.append(
                {
                    "municipio_id_ibge7": str(row["municipio_id_ibge7"]),
                    "alias_nome": alias,
                    "alias_norm": _normalize_text(alias),
                }
            )
    dim_alias = pd.DataFrame(alias_rows).drop_duplicates(subset=["municipio_id_ibge7", "alias_norm"]).reset_index(drop=True)
    return dim, dim_alias


def _build_silver_fato_municipio(base_df: pd.DataFrame, dim_alias: pd.DataFrame, dim_municipio: pd.DataFrame, inputs: MedallionInputs) -> pd.DataFrame:
    if "municipio" not in base_df.columns or "ranking_final" not in base_df.columns:
        raise MedallionPipelineError("base eleitoral deve conter colunas 'municipio' e 'ranking_final'")
    silver = base_df.copy()
    silver["municipio_norm_input"] = silver["municipio"].astype(str).map(_normalize_text)
    ano, mes, turno = _resolve_temporal_fields(silver, inputs)
    silver["ano"] = ano
    silver["mes"] = mes
    silver["turno"] = turno
    silver = silver.merge(dim_alias, left_on="municipio_norm_input", right_on="alias_norm", how="left")
    silver = silver.merge(
        dim_municipio[["municipio_id_ibge7", "codigo_tse", "codigo_ibge", "nome_municipio"]],
        on="municipio_id_ibge7",
        how="left",
    )
    silver["canonical_key"] = _build_canonical_key(silver)
    silver["join_status"] = silver["municipio_id_ibge7"].map(lambda v: "matched" if pd.notna(v) and str(v).strip() else "no_match")

    sort_cols = ["canonical_key"]
    if "indice_final" in silver.columns:
        silver["indice_final"] = pd.to_numeric(silver["indice_final"], errors="coerce")
        sort_cols.append("indice_final")
    silver = silver.sort_values(sort_cols, ascending=[True, False] if len(sort_cols) > 1 else [True])
    silver = silver.drop_duplicates(subset=["canonical_key"], keep="first")
    silver = validate_silver_fato_municipio(silver)
    return silver.reset_index(drop=True)


def _build_silver_fato_secao(secao_df: pd.DataFrame, dim_alias: pd.DataFrame, inputs: MedallionInputs) -> pd.DataFrame:
    if secao_df.empty:
        return secao_df
    municipio_col = _pick_column(secao_df, ["municipio", "nome_municipio"], label="municipio")
    out = secao_df.copy()
    out["municipio_norm_input"] = out[municipio_col].astype(str).map(_normalize_text)
    ano, mes, turno = _resolve_temporal_fields(out, inputs)
    out["ano"] = ano
    out["mes"] = mes
    out["turno"] = turno
    out = out.merge(dim_alias, left_on="municipio_norm_input", right_on="alias_norm", how="left")
    for candidate in ["secao", "numero_secao", "codigo_secao"]:
        if candidate in {str(c).lower() for c in out.columns}:
            secao_col = next(c for c in out.columns if str(c).lower() == candidate)
            out = out.rename(columns={secao_col: "secao"})
            break
    if "secao" not in out.columns:
        out["secao"] = -1
    if "zona" not in out.columns:
        out["zona"] = -1
    if "votos_validos" in out.columns:
        out["votos_validos"] = pd.to_numeric(out["votos_validos"], errors="coerce").fillna(0.0)
    else:
        out["votos_validos"] = 0.0
    out = out.drop_duplicates(subset=["municipio_id_ibge7", "ano", "turno", "zona", "secao"])
    return out.reset_index(drop=True)


def _build_silver_socio(socio_df: pd.DataFrame) -> pd.DataFrame:
    if socio_df.empty:
        return pd.DataFrame(columns=["municipio_id_ibge7"])
    out = socio_df.copy()
    ibge_col = _pick_column(out, ["codigo_ibge", "cod_ibge", "id_municipio_ibge"], label="codigo_ibge")
    out["municipio_id_ibge7"] = out[ibge_col].map(_normalize_ibge7)
    out = out.drop_duplicates(subset=["municipio_id_ibge7"]).reset_index(drop=True)
    return out


def _build_silver_context_df(df: pd.DataFrame, code_candidates: list[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["municipio_id_ibge7"])
    ibge_col = _pick_column(df, code_candidates, label="codigo_ibge")
    out = df.copy()
    out["municipio_id_ibge7"] = out[ibge_col].map(_normalize_ibge7)
    out = out.drop_duplicates(subset=["municipio_id_ibge7"]).reset_index(drop=True)
    return out


def _normalize_metric(df: pd.DataFrame, column: str) -> pd.Series:
    values = pd.to_numeric(df[column], errors="coerce")
    min_v = values.min()
    max_v = values.max()
    if pd.isna(min_v) or pd.isna(max_v) or max_v == min_v:
        return pd.Series([0.0] * len(df), index=df.index, dtype=float)
    return (values - min_v) / (max_v - min_v)


def _metric_column(df: pd.DataFrame, candidates: list[str], fallback: float = 0.0) -> pd.Series:
    lowered = {str(c).lower(): str(c) for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return pd.to_numeric(df[lowered[candidate.lower()]], errors="coerce").fillna(fallback)
    return pd.Series([fallback] * len(df), index=df.index, dtype=float)


def _select_window(df: pd.DataFrame, window_cycles: int) -> tuple[pd.DataFrame, list[int]]:
    if "ano" not in df.columns:
        return df, []
    years = (
        pd.to_numeric(df["ano"], errors="coerce")
        .dropna()
        .astype(int)
        .drop_duplicates()
        .sort_values(ascending=False)
        .tolist()
    )
    selected = years[: max(1, int(window_cycles))]
    if not selected:
        return df, []
    out = df[pd.to_numeric(df["ano"], errors="coerce").astype("Int64").isin(selected)].copy()
    return out, selected


def _build_gold_marts(
    silver_municipio: pd.DataFrame,
    silver_secao: pd.DataFrame,
    silver_socio: pd.DataFrame,
    silver_ibge: pd.DataFrame,
    silver_seade: pd.DataFrame,
    silver_fiscal: pd.DataFrame,
    window_cycles: int,
) -> dict[str, pd.DataFrame]:
    window_df, years_selected = _select_window(silver_municipio, window_cycles)
    scope_df = window_df if not window_df.empty else silver_municipio

    base_metrics = (
        scope_df.groupby(["municipio_id_ibge7"], dropna=False)
        .agg(
            ranking_medio_3ciclos=("ranking_final", "mean"),
            indice_medio_3ciclos=("indice_final", "mean") if "indice_final" in scope_df.columns else ("ranking_final", "mean"),
            anos_observados=("ano", "nunique"),
        )
        .reset_index()
    )

    ibge_merge = silver_ibge.copy()
    if not ibge_merge.empty:
        ibge_merge["pop_total"] = _metric_column(ibge_merge, ["pop_total", "populacao", "pop_censo2022"])
        ibge_merge["renda_media"] = _metric_column(ibge_merge, ["renda_media", "renda", "renda_per_capita"])
        ibge_merge["educacao_indice"] = _metric_column(ibge_merge, ["educacao_indice", "indice_educacao", "ideb"])
    mart_potencial = base_metrics.merge(
        ibge_merge[["municipio_id_ibge7", "pop_total", "renda_media", "educacao_indice"]] if not ibge_merge.empty else pd.DataFrame(columns=["municipio_id_ibge7", "pop_total", "renda_media", "educacao_indice"]),
        on="municipio_id_ibge7",
        how="left",
    )
    mart_potencial["pop_norm"] = _normalize_metric(mart_potencial.fillna({"pop_total": 0.0}), "pop_total")
    mart_potencial["renda_norm"] = _normalize_metric(mart_potencial.fillna({"renda_media": 0.0}), "renda_media")
    mart_potencial["educacao_norm"] = _normalize_metric(mart_potencial.fillna({"educacao_indice": 0.0}), "educacao_indice")
    mart_potencial["potencial_eleitoral_ajustado_social"] = (
        pd.to_numeric(mart_potencial["indice_medio_3ciclos"], errors="coerce").fillna(0.0)
        * (0.4 * mart_potencial["pop_norm"] + 0.3 * mart_potencial["renda_norm"] + 0.3 * mart_potencial["educacao_norm"] + 1.0)
    )
    mart_potencial["janela_anos"] = ",".join(str(y) for y in sorted(years_selected)) if years_selected else ""

    seade_merge = silver_seade.copy()
    if not seade_merge.empty:
        seade_merge["ipvs"] = _metric_column(seade_merge, ["ipvs", "indice_ipvs"])
        seade_merge["emprego"] = _metric_column(seade_merge, ["emprego", "taxa_emprego", "emprego_formal"])
        seade_merge["saude"] = _metric_column(seade_merge, ["saude", "indice_saude", "cobertura_saude"])
    mart_territorial = base_metrics.merge(
        seade_merge[["municipio_id_ibge7", "ipvs", "emprego", "saude"]] if not seade_merge.empty else pd.DataFrame(columns=["municipio_id_ibge7", "ipvs", "emprego", "saude"]),
        on="municipio_id_ibge7",
        how="left",
    )
    mart_territorial["ipvs_norm"] = _normalize_metric(mart_territorial.fillna({"ipvs": 0.0}), "ipvs")
    mart_territorial["emprego_norm"] = _normalize_metric(mart_territorial.fillna({"emprego": 0.0}), "emprego")
    mart_territorial["saude_norm"] = _normalize_metric(mart_territorial.fillna({"saude": 0.0}), "saude")
    mart_territorial["score_priorizacao_territorial_sp"] = (
        pd.to_numeric(mart_territorial["indice_medio_3ciclos"], errors="coerce").fillna(0.0)
        * (0.45 * mart_territorial["ipvs_norm"] + 0.30 * mart_territorial["emprego_norm"] + 0.25 * mart_territorial["saude_norm"] + 1.0)
    )
    mart_territorial["janela_anos"] = ",".join(str(y) for y in sorted(years_selected)) if years_selected else ""

    fiscal = silver_fiscal.copy()
    if not fiscal.empty:
        fiscal["transferencias"] = _metric_column(fiscal, ["transferencias", "transferencias_total"])
        fiscal["emendas"] = _metric_column(fiscal, ["emendas", "emendas_total"])
        fiscal["investimento_publico"] = fiscal["transferencias"] + fiscal["emendas"]
        if "ano" not in fiscal.columns:
            fiscal["ano"] = pd.NA
    fiscal_window, _ = _select_window(fiscal, window_cycles) if not fiscal.empty else (fiscal, [])
    fiscal_scope = fiscal_window if not fiscal_window.empty else fiscal
    if not fiscal_scope.empty:
        fiscal_agg = (
            fiscal_scope.groupby(["municipio_id_ibge7"], dropna=False)
            .agg(
                transferencias_3ciclos=("transferencias", "sum"),
                emendas_3ciclos=("emendas", "sum"),
                investimento_publico_3ciclos=("investimento_publico", "sum"),
            )
            .reset_index()
        )
    else:
        fiscal_agg = pd.DataFrame(
            columns=[
                "municipio_id_ibge7",
                "transferencias_3ciclos",
                "emendas_3ciclos",
                "investimento_publico_3ciclos",
            ]
        )
    mart_sensibilidade = base_metrics.merge(fiscal_agg, on="municipio_id_ibge7", how="left")
    mart_sensibilidade["transferencias_3ciclos"] = pd.to_numeric(
        mart_sensibilidade.get("transferencias_3ciclos"), errors="coerce"
    ).fillna(0.0)
    mart_sensibilidade["emendas_3ciclos"] = pd.to_numeric(mart_sensibilidade.get("emendas_3ciclos"), errors="coerce").fillna(0.0)
    mart_sensibilidade["investimento_publico_3ciclos"] = pd.to_numeric(
        mart_sensibilidade.get("investimento_publico_3ciclos"), errors="coerce"
    ).fillna(0.0)
    mart_sensibilidade["sensibilidade_investimento_publico"] = (
        pd.to_numeric(mart_sensibilidade["indice_medio_3ciclos"], errors="coerce").fillna(0.0)
        / (mart_sensibilidade["investimento_publico_3ciclos"] + 1.0)
    )
    mart_sensibilidade["janela_anos"] = ",".join(str(y) for y in sorted(years_selected)) if years_selected else ""

    mart_municipio = base_metrics.merge(
        silver_socio.drop(columns=["codigo_ibge"], errors="ignore") if not silver_socio.empty else pd.DataFrame(columns=["municipio_id_ibge7"]),
        on="municipio_id_ibge7",
        how="left",
    )
    mart_municipio["ano"] = max(years_selected) if years_selected else pd.NA
    mart_municipio["turno"] = 0
    mart_municipio["canonical_key"] = mart_municipio["municipio_id_ibge7"].map(
        lambda v: f"{v}:{max(years_selected)}:00:0" if years_selected and str(v).strip() else None
    )
    mart_municipio["ranking_final"] = mart_municipio["ranking_medio_3ciclos"]
    mart_municipio["janela_anos"] = ",".join(str(y) for y in sorted(years_selected)) if years_selected else ""
    mart_municipio = validate_gold_mart_municipio_eleitoral(mart_municipio)

    if not silver_secao.empty:
        secao_window, _ = _select_window(silver_secao, window_cycles)
        secao_scope = secao_window if not secao_window.empty else silver_secao
        trend_base = (
            secao_scope.groupby(["municipio_id_ibge7", "ano", "turno"], dropna=False)
            .agg(votos_validos_total=("votos_validos", "sum"))
            .reset_index()
        )
        mart_tendencia = (
            trend_base.groupby(["municipio_id_ibge7"], dropna=False)
            .agg(
                votos_validos_medio_3ciclos=("votos_validos_total", "mean"),
                votos_validos_std_3ciclos=("votos_validos_total", "std"),
                observacoes=("votos_validos_total", "count"),
            )
            .reset_index()
        )
    else:
        trend_base = (
            scope_df.groupby(["municipio_id_ibge7", "ano", "turno"], dropna=False)
            .agg(indice_medio=("indice_final", "mean") if "indice_final" in scope_df.columns else ("ranking_final", "mean"))
            .reset_index()
        )
        mart_tendencia = (
            trend_base.groupby(["municipio_id_ibge7"], dropna=False)
            .agg(
                indice_medio_3ciclos=("indice_medio", "mean"),
                indice_std_3ciclos=("indice_medio", "std"),
                observacoes=("indice_medio", "count"),
            )
            .reset_index()
        )
    mart_tendencia["janela_anos"] = ",".join(str(y) for y in sorted(years_selected)) if years_selected else ""

    mart_contexto = silver_socio.copy()
    if not mart_contexto.empty:
        mart_contexto = mart_contexto.drop_duplicates(subset=["municipio_id_ibge7"]).reset_index(drop=True)
    mart_contexto["janela_anos"] = ",".join(str(y) for y in sorted(years_selected)) if years_selected else ""
    return {
        "mart_municipio_eleitoral": mart_municipio,
        "mart_tendencia_turno": mart_tendencia,
        "mart_contexto_socioeconomico": mart_contexto,
        "mart_potencial_eleitoral_social": mart_potencial,
        "mart_priorizacao_territorial_sp": mart_territorial,
        "mart_sensibilidade_investimento_publico": mart_sensibilidade,
    }


def _build_gold_marts_duckdb(
    silver_municipio: pd.DataFrame,
    silver_secao: pd.DataFrame,
    silver_socio: pd.DataFrame,
    silver_ibge: pd.DataFrame,
    silver_seade: pd.DataFrame,
    silver_fiscal: pd.DataFrame,
    window_cycles: int,
) -> dict[str, pd.DataFrame]:
    # mantém lógica de janela e métricas, mas executa joins pesados via DuckDB
    try:
        import duckdb  # type: ignore
    except ModuleNotFoundError:
        return _build_gold_marts(
            silver_municipio,
            silver_secao,
            silver_socio,
            silver_ibge,
            silver_seade,
            silver_fiscal,
            window_cycles,
        )

    _, years_selected = _select_window(silver_municipio, window_cycles)
    years_selected = years_selected or []
    scope_sql = silver_municipio.copy()
    if years_selected:
        scope_sql = scope_sql[pd.to_numeric(scope_sql["ano"], errors="coerce").astype("Int64").isin(years_selected)]
    if scope_sql.empty:
        scope_sql = silver_municipio.copy()

    conn = duckdb.connect(database=":memory:")
    try:
        conn.register("silver_municipio", scope_sql)
        conn.register("silver_secao", silver_secao)
        conn.register("silver_socio", silver_socio)
        conn.register("silver_ibge", silver_ibge)
        conn.register("silver_seade", silver_seade)
        conn.register("silver_fiscal", silver_fiscal)

        mart_municipio = conn.execute(
            """
            WITH base AS (
              SELECT
                municipio_id_ibge7,
                AVG(CAST(ranking_final AS DOUBLE)) AS ranking_medio_3ciclos,
                AVG(CAST(COALESCE(indice_final, ranking_final) AS DOUBLE)) AS indice_medio_3ciclos,
                COUNT(DISTINCT ano) AS anos_observados
              FROM silver_municipio
              GROUP BY municipio_id_ibge7
            )
            SELECT
              b.municipio_id_ibge7,
              b.ranking_medio_3ciclos,
              b.indice_medio_3ciclos,
              b.anos_observados,
              s.* EXCLUDE(municipio_id_ibge7)
            FROM base b
            LEFT JOIN silver_socio s USING (municipio_id_ibge7)
            """
        ).df()

        if not silver_secao.empty:
            mart_tendencia = conn.execute(
                """
                WITH secao_agg AS (
                  SELECT
                    municipio_id_ibge7,
                    ano,
                    turno,
                    SUM(CAST(votos_validos AS DOUBLE)) AS votos_validos_total
                  FROM silver_secao
                  GROUP BY municipio_id_ibge7, ano, turno
                )
                SELECT
                  municipio_id_ibge7,
                  AVG(votos_validos_total) AS votos_validos_medio_3ciclos,
                  STDDEV_SAMP(votos_validos_total) AS votos_validos_std_3ciclos,
                  COUNT(*) AS observacoes
                FROM secao_agg
                GROUP BY municipio_id_ibge7
                """
            ).df()
        else:
            mart_tendencia = conn.execute(
                """
                WITH base AS (
                  SELECT
                    municipio_id_ibge7,
                    ano,
                    turno,
                    AVG(CAST(COALESCE(indice_final, ranking_final) AS DOUBLE)) AS indice_medio
                  FROM silver_municipio
                  GROUP BY municipio_id_ibge7, ano, turno
                )
                SELECT
                  municipio_id_ibge7,
                  AVG(indice_medio) AS indice_medio_3ciclos,
                  STDDEV_SAMP(indice_medio) AS indice_std_3ciclos,
                  COUNT(*) AS observacoes
                FROM base
                GROUP BY municipio_id_ibge7
                """
            ).df()

        mart_contexto = conn.execute(
            "SELECT DISTINCT * FROM silver_socio"
        ).df()

        mart_potencial = conn.execute(
            """
            WITH base AS (
              SELECT
                municipio_id_ibge7,
                AVG(CAST(COALESCE(indice_final, ranking_final) AS DOUBLE)) AS indice_medio_3ciclos
              FROM silver_municipio
              GROUP BY municipio_id_ibge7
            )
            SELECT
              b.municipio_id_ibge7,
              b.indice_medio_3ciclos,
              i.* EXCLUDE(municipio_id_ibge7)
            FROM base b
            LEFT JOIN silver_ibge i USING (municipio_id_ibge7)
            """
        ).df()

        mart_territorial = conn.execute(
            """
            WITH base AS (
              SELECT
                municipio_id_ibge7,
                AVG(CAST(COALESCE(indice_final, ranking_final) AS DOUBLE)) AS indice_medio_3ciclos
              FROM silver_municipio
              GROUP BY municipio_id_ibge7
            )
            SELECT
              b.municipio_id_ibge7,
              b.indice_medio_3ciclos,
              s.* EXCLUDE(municipio_id_ibge7)
            FROM base b
            LEFT JOIN silver_seade s USING (municipio_id_ibge7)
            """
        ).df()

        fiscal_scope = silver_fiscal.copy()
        if years_selected and "ano" in fiscal_scope.columns:
            fiscal_scope = fiscal_scope[pd.to_numeric(fiscal_scope["ano"], errors="coerce").astype("Int64").isin(years_selected)]
        conn.register("silver_fiscal_scope", fiscal_scope)
        mart_sensibilidade = conn.execute(
            """
            WITH base AS (
              SELECT
                municipio_id_ibge7,
                AVG(CAST(COALESCE(indice_final, ranking_final) AS DOUBLE)) AS indice_medio_3ciclos
              FROM silver_municipio
              GROUP BY municipio_id_ibge7
            ),
            fiscal AS (
              SELECT
                municipio_id_ibge7,
                SUM(CAST(COALESCE(transferencias, transferencias_total, 0) AS DOUBLE)) AS transferencias_3ciclos,
                SUM(CAST(COALESCE(emendas, emendas_total, 0) AS DOUBLE)) AS emendas_3ciclos
              FROM silver_fiscal_scope
              GROUP BY municipio_id_ibge7
            )
            SELECT
              b.municipio_id_ibge7,
              b.indice_medio_3ciclos,
              COALESCE(f.transferencias_3ciclos, 0) AS transferencias_3ciclos,
              COALESCE(f.emendas_3ciclos, 0) AS emendas_3ciclos
            FROM base b
            LEFT JOIN fiscal f USING (municipio_id_ibge7)
            """
        ).df()
    finally:
        conn.close()

    # complementa features finais e colunas de janela fora do SQL para evitar repetição
    janela = ",".join(str(y) for y in sorted(years_selected)) if years_selected else ""
    if not mart_municipio.empty:
        mart_municipio["ano"] = max(years_selected) if years_selected else pd.NA
        mart_municipio["turno"] = 0
        mart_municipio["canonical_key"] = mart_municipio["municipio_id_ibge7"].map(
            lambda v: f"{v}:{max(years_selected)}:00:0" if years_selected and str(v).strip() else None
        )
        mart_municipio["ranking_final"] = pd.to_numeric(
            mart_municipio["ranking_medio_3ciclos"], errors="coerce"
        )
        mart_municipio["janela_anos"] = janela
        mart_municipio = validate_gold_mart_municipio_eleitoral(mart_municipio)
    mart_tendencia["janela_anos"] = janela
    mart_contexto["janela_anos"] = janela

    if not mart_potencial.empty:
        mart_potencial["pop_total"] = _metric_column(mart_potencial, ["pop_total", "populacao", "pop_censo2022"])
        mart_potencial["renda_media"] = _metric_column(mart_potencial, ["renda_media", "renda", "renda_per_capita"])
        mart_potencial["educacao_indice"] = _metric_column(mart_potencial, ["educacao_indice", "indice_educacao", "ideb"])
        mart_potencial["pop_norm"] = _normalize_metric(mart_potencial.fillna({"pop_total": 0.0}), "pop_total")
        mart_potencial["renda_norm"] = _normalize_metric(mart_potencial.fillna({"renda_media": 0.0}), "renda_media")
        mart_potencial["educacao_norm"] = _normalize_metric(mart_potencial.fillna({"educacao_indice": 0.0}), "educacao_indice")
        mart_potencial["potencial_eleitoral_ajustado_social"] = (
            pd.to_numeric(mart_potencial["indice_medio_3ciclos"], errors="coerce").fillna(0.0)
            * (0.4 * mart_potencial["pop_norm"] + 0.3 * mart_potencial["renda_norm"] + 0.3 * mart_potencial["educacao_norm"] + 1.0)
        )
        mart_potencial["janela_anos"] = janela

    if not mart_territorial.empty:
        mart_territorial["ipvs"] = _metric_column(mart_territorial, ["ipvs", "indice_ipvs"])
        mart_territorial["emprego"] = _metric_column(mart_territorial, ["emprego", "taxa_emprego", "emprego_formal"])
        mart_territorial["saude"] = _metric_column(mart_territorial, ["saude", "indice_saude", "cobertura_saude"])
        mart_territorial["ipvs_norm"] = _normalize_metric(mart_territorial.fillna({"ipvs": 0.0}), "ipvs")
        mart_territorial["emprego_norm"] = _normalize_metric(mart_territorial.fillna({"emprego": 0.0}), "emprego")
        mart_territorial["saude_norm"] = _normalize_metric(mart_territorial.fillna({"saude": 0.0}), "saude")
        mart_territorial["score_priorizacao_territorial_sp"] = (
            pd.to_numeric(mart_territorial["indice_medio_3ciclos"], errors="coerce").fillna(0.0)
            * (0.45 * mart_territorial["ipvs_norm"] + 0.30 * mart_territorial["emprego_norm"] + 0.25 * mart_territorial["saude_norm"] + 1.0)
        )
        mart_territorial["janela_anos"] = janela

    mart_sensibilidade["investimento_publico_3ciclos"] = (
        pd.to_numeric(mart_sensibilidade["transferencias_3ciclos"], errors="coerce").fillna(0.0)
        + pd.to_numeric(mart_sensibilidade["emendas_3ciclos"], errors="coerce").fillna(0.0)
    )
    mart_sensibilidade["sensibilidade_investimento_publico"] = (
        pd.to_numeric(mart_sensibilidade["indice_medio_3ciclos"], errors="coerce").fillna(0.0)
        / (mart_sensibilidade["investimento_publico_3ciclos"] + 1.0)
    )
    mart_sensibilidade["janela_anos"] = janela

    return {
        "mart_municipio_eleitoral": mart_municipio,
        "mart_tendencia_turno": mart_tendencia,
        "mart_contexto_socioeconomico": mart_contexto,
        "mart_potencial_eleitoral_social": mart_potencial,
        "mart_priorizacao_territorial_sp": mart_territorial,
        "mart_sensibilidade_investimento_publico": mart_sensibilidade,
    }


def _materialize_serving_layer(
    *,
    paths: AppPaths,
    run_id: str,
    marts: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    serving_root = paths.data_root / "outputs" / "serving"
    serving_root.mkdir(parents=True, exist_ok=True)
    serving_db_path = serving_root / "serving.duckdb"
    stats_path = serving_root / f"serving_stats_{run_id}.json"
    cache_path = serving_root / "query_cache.parquet"

    table_stats: list[dict[str, Any]] = []
    try:
        import duckdb  # type: ignore

        conn = duckdb.connect(str(serving_db_path))
        try:
            for table_name, df in marts.items():
                conn.register("_tmp_df", df)
                conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM _tmp_df")
                if "municipio_id_ibge7" in df.columns:
                    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_municipio ON {table_name}(municipio_id_ibge7)")
                if "canonical_key" in df.columns:
                    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_ckey ON {table_name}(canonical_key)")
                conn.execute(f"ANALYZE {table_name}")
                row_count = int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
                table_stats.append({"table": table_name, "rows": row_count})

            cache_sql = """
                CREATE OR REPLACE TABLE query_cache AS
                SELECT
                  m.municipio_id_ibge7,
                  m.ranking_medio_3ciclos,
                  p.potencial_eleitoral_ajustado_social,
                  t.score_priorizacao_territorial_sp,
                  s.sensibilidade_investimento_publico
                FROM mart_municipio_eleitoral m
                LEFT JOIN mart_potencial_eleitoral_social p USING (municipio_id_ibge7)
                LEFT JOIN mart_priorizacao_territorial_sp t USING (municipio_id_ibge7)
                LEFT JOIN mart_sensibilidade_investimento_publico s USING (municipio_id_ibge7)
            """
            conn.execute(cache_sql)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_query_cache_municipio ON query_cache(municipio_id_ibge7)")
            conn.execute("ANALYZE query_cache")
            query_cache_df = conn.execute("SELECT * FROM query_cache").df()
            query_cache_df, _ = apply_lgpd_purpose_policy(
                query_cache_df,
                purpose="serving_cache",
                salt=get_settings().lgpd_anonymization_salt,
            )
            query_cache_df.to_parquet(cache_path, index=False)
        finally:
            conn.close()
    except ModuleNotFoundError:
        # fallback local para ambiente sem duckdb instalado
        serving_db_path.write_text("duckdb unavailable in local environment", encoding="utf-8")
        for table_name, df in marts.items():
            table_stats.append({"table": table_name, "rows": int(len(df))})
        cache_df = (
            marts.get("mart_municipio_eleitoral", pd.DataFrame())
            .merge(
                marts.get("mart_potencial_eleitoral_social", pd.DataFrame())[["municipio_id_ibge7", "potencial_eleitoral_ajustado_social"]]
                if "mart_potencial_eleitoral_social" in marts
                else pd.DataFrame(columns=["municipio_id_ibge7", "potencial_eleitoral_ajustado_social"]),
                on="municipio_id_ibge7",
                how="left",
            )
            .merge(
                marts.get("mart_priorizacao_territorial_sp", pd.DataFrame())[["municipio_id_ibge7", "score_priorizacao_territorial_sp"]]
                if "mart_priorizacao_territorial_sp" in marts
                else pd.DataFrame(columns=["municipio_id_ibge7", "score_priorizacao_territorial_sp"]),
                on="municipio_id_ibge7",
                how="left",
            )
            .merge(
                marts.get("mart_sensibilidade_investimento_publico", pd.DataFrame())[["municipio_id_ibge7", "sensibilidade_investimento_publico"]]
                if "mart_sensibilidade_investimento_publico" in marts
                else pd.DataFrame(columns=["municipio_id_ibge7", "sensibilidade_investimento_publico"]),
                on="municipio_id_ibge7",
                how="left",
            )
        )
        cache_df, _ = apply_lgpd_purpose_policy(
            cache_df,
            purpose="serving_cache",
            salt=get_settings().lgpd_anonymization_salt,
        )
        cache_df.to_parquet(cache_path, index=False)

    stats_payload = {
        "run_id": run_id,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "serving_db_path": str(serving_db_path),
        "cache_path": str(cache_path),
        "tables": table_stats,
    }
    stats_path.write_text(json.dumps(stats_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "serving_db_path": str(serving_db_path),
        "cache_path": str(cache_path),
        "stats_path": str(stats_path),
    }


def run_medallion_pipeline(paths: AppPaths, inputs: MedallionInputs, pipeline_version: str = "medallion_v1") -> dict[str, Any]:
    run_id = _ts_now_compact()
    run_dir = paths.data_root / "outputs" / "pipeline_runs" / pipeline_version / run_id
    bronze_dir = run_dir / "bronze"
    silver_dir = run_dir / "silver"
    gold_dir = run_dir / "gold"
    lake_root = paths.data_root / "outputs" / "lake"
    lake_bronze_root = lake_root / "bronze"
    lake_silver_root = lake_root / "silver"
    lake_gold_root = lake_root / "gold"
    for folder in [bronze_dir, silver_dir, gold_dir]:
        folder.mkdir(parents=True, exist_ok=True)

    if not inputs.base_parquet_path.exists():
        raise MedallionPipelineError(f"dataset base nao encontrado: {inputs.base_parquet_path}")
    if not inputs.mapping_csv_path.exists():
        raise MedallionPipelineError(f"mapping nao encontrado: {inputs.mapping_csv_path}")

    incremental_state = _load_incremental_state(lake_root)

    sources_to_load: list[tuple[str, Path]] = [
        ("base_eleitoral", inputs.base_parquet_path),
        ("mapping_tse_ibge", inputs.mapping_csv_path),
    ]
    if inputs.socio_csv_path and inputs.socio_csv_path.exists():
        sources_to_load.append(("contexto_socio", inputs.socio_csv_path))
    if inputs.secao_csv_path and inputs.secao_csv_path.exists():
        sources_to_load.append(("resultado_secao", inputs.secao_csv_path))
    if inputs.ibge_csv_path and inputs.ibge_csv_path.exists():
        sources_to_load.append(("ibge_indicadores", inputs.ibge_csv_path))
    if inputs.seade_csv_path and inputs.seade_csv_path.exists():
        sources_to_load.append(("seade_indicadores", inputs.seade_csv_path))
    if inputs.fiscal_csv_path and inputs.fiscal_csv_path.exists():
        sources_to_load.append(("transparencia_fiscal", inputs.fiscal_csv_path))

    bronze_assets: list[dict[str, Any]] = []
    for source_name, source_path in sources_to_load:
        src_hash = _sha256_file(source_path)
        prev = incremental_state.get(source_name, {})
        if prev.get("hash_sha256") == src_hash:
            source_last_modified_utc = datetime.fromtimestamp(source_path.stat().st_mtime, UTC).isoformat()
            bronze_assets.append(
                {
                    "source": source_name,
                    "path": str(source_path),
                    "dt_coleta_utc": datetime.now(UTC).isoformat(),
                    "hash_sha256": src_hash,
                    "rows": int(len(_safe_read(source_path))),
                    "incremental_status": "skipped_unchanged",
                    "partitions": prev.get("partitions", []),
                    "source_last_modified_utc": source_last_modified_utc,
                }
            )
            continue
        loaded = _write_bronze_asset(
            source_name,
            source_path,
            bronze_dir,
            lake_bronze_root=lake_bronze_root,
            run_id=run_id,
            default_ano=inputs.ano,
            default_uf=inputs.uf,
        )
        loaded["incremental_status"] = "loaded_changed"
        bronze_assets.append(loaded)
        incremental_state[source_name] = {
            "hash_sha256": loaded["hash_sha256"],
            "dt_coleta_utc": loaded["dt_coleta_utc"],
            "partitions": loaded.get("partitions", []),
        }
    _save_incremental_state(lake_root, incremental_state)

    base_df = pd.read_parquet(inputs.base_parquet_path)
    socio_df = pd.read_csv(inputs.socio_csv_path) if inputs.socio_csv_path and inputs.socio_csv_path.exists() else pd.DataFrame()
    secao_df = pd.read_csv(inputs.secao_csv_path) if inputs.secao_csv_path and inputs.secao_csv_path.exists() else pd.DataFrame()
    ibge_df = pd.read_csv(inputs.ibge_csv_path) if inputs.ibge_csv_path and inputs.ibge_csv_path.exists() else pd.DataFrame()
    seade_df = pd.read_csv(inputs.seade_csv_path) if inputs.seade_csv_path and inputs.seade_csv_path.exists() else pd.DataFrame()
    fiscal_df = pd.read_csv(inputs.fiscal_csv_path) if inputs.fiscal_csv_path and inputs.fiscal_csv_path.exists() else pd.DataFrame()
    contract_report = validate_input_contracts(
        base_df=base_df,
        mapping_df=pd.read_csv(inputs.mapping_csv_path),
        socio_df=socio_df,
        secao_df=secao_df,
        ibge_df=ibge_df,
        seade_df=seade_df,
        fiscal_df=fiscal_df,
    )

    dim_municipio, dim_alias = _build_dim_municipio(inputs.mapping_csv_path)
    silver_fato_municipio = _build_silver_fato_municipio(base_df, dim_alias, dim_municipio, inputs)
    silver_fato_secao = _build_silver_fato_secao(secao_df, dim_alias, inputs)
    silver_socio = _build_silver_socio(socio_df)
    silver_ibge = _build_silver_context_df(ibge_df, ["codigo_ibge", "cod_ibge", "id_municipio_ibge"])
    silver_seade = _build_silver_context_df(seade_df, ["codigo_ibge", "cod_ibge", "id_municipio_ibge"])
    silver_fiscal = _build_silver_context_df(fiscal_df, ["codigo_ibge", "cod_ibge", "id_municipio_ibge"])

    silver_outputs = {
        "dim_municipio": dim_municipio,
        "dim_municipio_alias": dim_alias,
        "fato_eleitoral_municipio": silver_fato_municipio,
        "fato_eleitoral_secao": silver_fato_secao,
        "dim_contexto_socioeconomico": silver_socio,
        "dim_ibge_indicadores": silver_ibge,
        "dim_seade_indicadores": silver_seade,
        "fato_fiscal_municipio": silver_fiscal,
    }
    silver_paths: dict[str, str] = {}
    for name, df in silver_outputs.items():
        path = silver_dir / f"{name}.parquet"
        df.to_parquet(path, index=False)
        silver_paths[name] = str(path)
        _write_partitioned_parquet(
            df,
            layer_root=lake_silver_root,
            source_name=name,
            run_id=run_id,
            default_ano=inputs.ano,
            default_uf=inputs.uf,
        )

    gold_outputs = _build_gold_marts_duckdb(
        silver_fato_municipio,
        silver_fato_secao,
        silver_socio,
        silver_ibge,
        silver_seade,
        silver_fiscal,
        inputs.window_cycles,
    )
    gold_paths: dict[str, str] = {}
    published_paths: dict[str, str] = {}
    paths.pasta_est.mkdir(parents=True, exist_ok=True)
    for name, df in gold_outputs.items():
        df_lgpd, _ = apply_lgpd_purpose_policy(
            df,
            purpose="gold_mart",
            salt=get_settings().lgpd_anonymization_salt,
        )
        gold_path = gold_dir / f"{name}.parquet"
        df_lgpd.to_parquet(gold_path, index=False)
        gold_paths[name] = str(gold_path)
        _write_partitioned_parquet(
            df_lgpd,
            layer_root=lake_gold_root,
            source_name=name,
            run_id=run_id,
            default_ano=inputs.ano,
            default_uf=inputs.uf,
        )

        publish_path = paths.pasta_est / f"{name}_{run_id}.parquet"
        df_lgpd.to_parquet(publish_path, index=False)
        published_paths[name] = str(publish_path)

        metadata = build_dataset_metadata(
            dataset_name=name,
            dataset_version=run_id,
            dataset_path=publish_path,
            pipeline_version=pipeline_version,
            run_id=run_id,
        )
        register_dataset_version(paths, metadata)
    serving_refs = _materialize_serving_layer(paths=paths, run_id=run_id, marts=gold_outputs)

    join_success = compute_join_success(silver_fato_municipio)
    null_critical = compute_null_critical(
        silver_fato_municipio,
        critical_columns=["municipio_id_ibge7", "ranking_final", "ano", "turno"],
    )
    update_delay = compute_update_delay_days(bronze_assets)
    prev_path = find_previous_dataset_path(paths, "mart_municipio_eleitoral", run_id)
    drift = compute_drift_score(
        current_df=gold_outputs.get("mart_municipio_eleitoral", pd.DataFrame()),
        previous_path=prev_path,
        feature_columns=["ranking_medio_3ciclos", "indice_medio_3ciclos"],
    )
    quality_metrics = {
        "join_success_pct": round(join_success * 100.0, 3),
        "null_critical_pct": round(null_critical * 100.0, 3),
        "update_delay_days": {k: round(v, 3) for k, v in update_delay.items()},
        "drift_score": round(float(drift.get("drift_score", 0.0)), 6),
        "drift_alert": bool(drift.get("drift_alert", 0.0) >= 1.0),
    }

    settings = get_settings()
    retention_result = {
        "pipeline_runs": enforce_retention_policy(paths.data_root / "outputs" / "pipeline_runs", retention_days=settings.retention_days),
        "lake": enforce_retention_policy(paths.data_root / "outputs" / "lake", retention_days=settings.retention_days),
        "serving": enforce_retention_policy(paths.data_root / "outputs" / "serving", retention_days=settings.retention_days),
    }

    manifest = {
        "pipeline_version": pipeline_version,
        "run_id": run_id,
        "created_at_utc": datetime.now(UTC).isoformat(),
        "layers": {
            "bronze": {
                "assets": bronze_assets,
            },
            "silver": {
                "datasets": silver_paths,
                "contracts": {
                    "tipos_normalizacao_deduplicacao": True,
                },
            },
            "gold": {
                "marts": gold_paths,
                "published": published_paths,
                "aggregation_level": "municipio",
                "window_cycles": int(inputs.window_cycles),
            },
            "serving": serving_refs,
            "contracts": contract_report,
            "quality_metrics": quality_metrics,
            "lgpd": {
                "policy": "minimization_by_purpose + anonymization_if_pii + retention_policy",
                "retention_days": int(settings.retention_days),
                "retention_result": retention_result,
            },
        },
    }
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "run_id": run_id,
        "manifest_path": str(manifest_path),
        "published": published_paths,
        "serving": serving_refs,
    }
