from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import hashlib
import json
from pathlib import Path
from typing import Any
import unicodedata

import pandas as pd

from config.settings import AppPaths, get_settings
from domain.open_data_contracts import (
    validate_gold_mart_municipio_eleitoral,
    validate_silver_dim_tempo,
    validate_silver_dim_territorio,
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
from infrastructure.load_manifest import build_load_manifest
from infrastructure.source_contracts import validate_input_contracts
from infrastructure.territory_matching import build_alias_dimension, layered_match_territory


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


def _normalize_optional_int(value: Any) -> int | None:
    if pd.isna(value):
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if not digits:
        return None
    return int(digits)


def _pick_first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lowered = {str(col).lower(): str(col) for col in df.columns}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    return None


def _encode_geohash(latitude: float | None, longitude: float | None, precision: int = 7) -> str | None:
    if latitude is None or longitude is None or pd.isna(latitude) or pd.isna(longitude):
        return None
    lat_interval = [-90.0, 90.0]
    lon_interval = [-180.0, 180.0]
    geohash_chars = "0123456789bcdefghjkmnpqrstuvwxyz"
    is_even = True
    bit = 0
    ch = 0
    output = []
    bits = [16, 8, 4, 2, 1]
    while len(output) < precision:
        if is_even:
            mid = sum(lon_interval) / 2
            if float(longitude) >= mid:
                ch |= bits[bit]
                lon_interval[0] = mid
            else:
                lon_interval[1] = mid
        else:
            mid = sum(lat_interval) / 2
            if float(latitude) >= mid:
                ch |= bits[bit]
                lat_interval[0] = mid
            else:
                lat_interval[1] = mid
        is_even = not is_even
        if bit < 4:
            bit += 1
        else:
            output.append(geohash_chars[ch])
            bit = 0
            ch = 0
    return "".join(output)


def _first_sunday_of_october(year: int) -> date:
    first = date(int(year), 10, 1)
    return first + timedelta(days=(6 - first.weekday()) % 7)


def _build_dim_tempo(
    *,
    silver_municipio: pd.DataFrame,
    silver_secao: pd.DataFrame,
    silver_fiscal: pd.DataFrame,
    inputs: MedallionInputs,
) -> pd.DataFrame:
    years: list[int] = []
    for frame in [silver_municipio, silver_secao, silver_fiscal]:
        if not frame.empty and "ano" in frame.columns:
            years.extend(pd.to_numeric(frame["ano"], errors="coerce").dropna().astype(int).tolist())
    if inputs.ano is not None:
        years.append(int(inputs.ano))
    if not years:
        years.append(datetime.now(UTC).year)

    min_year = min(years)
    max_year = max(years)
    rows: list[dict[str, Any]] = []
    current = date(min_year, 1, 1)
    end = date(max_year, 12, 31)
    while current <= end:
        election_day = _first_sunday_of_october(current.year)
        campaign_start = election_day - timedelta(days=45)
        pre_campaign_start = date(current.year, 1, 1)
        is_historical = current.year < max_year
        is_pre_campaign = pre_campaign_start <= current < campaign_start
        is_campaign = campaign_start <= current <= election_day
        is_event = current in {pre_campaign_start, campaign_start, election_day}
        if current == election_day:
            event = "eleicao_turno_1"
            event_type = "eleitoral_oficial"
        elif current == campaign_start:
            event = "inicio_janela_campanha"
            event_type = "campanha"
        elif current == pre_campaign_start:
            event = "inicio_pre_campanha"
            event_type = "pre_campanha"
        else:
            event = None
            event_type = None

        if is_event:
            media_pulse = "alto"
        elif is_campaign:
            media_pulse = "medio"
        elif is_pre_campaign:
            media_pulse = "baixo"
        else:
            media_pulse = "base"

        if is_historical:
            phase = "historico_eleitoral"
        elif is_campaign:
            phase = "janela_campanha"
        elif is_pre_campaign:
            phase = "pre_campanha"
        else:
            phase = "pos_eleicao"

        iso = current.isocalendar()
        rows.append(
            {
                "tempo_id": current.strftime("%Y%m%d"),
                "data": current.isoformat(),
                "ano": current.year,
                "mes": current.month,
                "dia": current.day,
                "semana_iso": int(iso.week),
                "ano_semana_iso": f"{iso.year}-W{int(iso.week):02d}",
                "dia_semana": current.weekday() + 1,
                "nome_dia_semana": ["segunda", "terca", "quarta", "quinta", "sexta", "sabado", "domingo"][current.weekday()],
                "trimestre": (current.month - 1) // 3 + 1,
                "ciclo_eleitoral": current.year,
                "fase_calendario": phase,
                "is_historico_eleitoral": bool(is_historical),
                "is_pre_campanha": bool(is_pre_campaign and not is_historical),
                "is_janela_campanha": bool(is_campaign and not is_historical),
                "is_evento": bool(is_event),
                "evento": event,
                "tipo_evento": event_type,
                "pulso_midia": media_pulse,
                "is_pulso_midia": bool(media_pulse in {"alto", "medio"}),
                "dias_ate_eleicao": int((election_day - current).days),
            }
        )
        current += timedelta(days=1)
    return validate_silver_dim_tempo(pd.DataFrame(rows))


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

    alias_map: dict[str, list[str]] = {}
    for _, row in dim.iterrows():
        aliases = []
        if alias_col:
            src = mapping_df.loc[mapping_df[codigo_ibge_col].map(_normalize_ibge7) == row["municipio_id_ibge7"], alias_col]
            if not src.empty:
                aliases.extend([_normalize_text(a.strip()) for a in str(src.iloc[0]).split(";") if a.strip()])
        alias_map[str(row["municipio_id_ibge7"])] = aliases
    dim_alias = build_alias_dimension(dim_municipio=dim, alias_map=alias_map)
    if not dim_alias.empty:
        dim_alias["alias_nome"] = dim_alias["alias_nome"].astype(str).str.strip()
        dim_alias["alias_norm"] = dim_alias["alias_nome"].map(_normalize_text)
        dim_alias = dim_alias.drop_duplicates(subset=["municipio_id_ibge7", "alias_norm"]).reset_index(drop=True)
    return dim, dim_alias


def _build_dim_territorio(
    mapping_csv_path: Path,
    dim_municipio: pd.DataFrame,
    dim_alias: pd.DataFrame,
    secao_df: pd.DataFrame,
    inputs: MedallionInputs,
) -> pd.DataFrame:
    mapping_df = pd.read_csv(mapping_csv_path)
    codigo_ibge_col = _pick_column(mapping_df, ["codigo_ibge", "cod_ibge", "id_municipio_ibge"], label="codigo_ibge")
    codigo_tse_col = _pick_column(mapping_df, ["codigo_tse", "cod_tse", "cd_mun_tse"], label="codigo_tse")
    nome_col = _pick_column(mapping_df, ["nome_municipio", "municipio", "nm_municipio"], label="nome_municipio")
    uf_col = _pick_first_existing_column(mapping_df, ["uf", "sigla_uf"])
    lat_col = _pick_first_existing_column(mapping_df, ["latitude", "lat", "latitude_municipio"])
    lon_col = _pick_first_existing_column(mapping_df, ["longitude", "lon", "lng", "longitude_municipio"])

    years = []
    if not secao_df.empty and "ano" in secao_df.columns:
        years.extend(pd.to_numeric(secao_df["ano"], errors="coerce").dropna().astype(int).tolist())
    vigencia_inicio = f"{min(years)}-01-01" if years else None
    vigencia_fim = f"{max(years)}-12-31" if years else None

    base_rows: list[dict[str, Any]] = []
    for _, row in mapping_df.iterrows():
        codigo_ibge = _normalize_ibge7(row[codigo_ibge_col])
        codigo_tse = str(row[codigo_tse_col]).strip()
        nome = str(row[nome_col]).strip()
        uf = str(row[uf_col]).strip().upper() if uf_col else inputs.uf
        latitude = pd.to_numeric(row[lat_col], errors="coerce") if lat_col else pd.NA
        longitude = pd.to_numeric(row[lon_col], errors="coerce") if lon_col else pd.NA
        geohash = _encode_geohash(None if pd.isna(latitude) else float(latitude), None if pd.isna(longitude) else float(longitude))
        base_rows.append(
            {
                "territorio_id": f"mun:{codigo_ibge}:0:0",
                "cod_tse_municipio": codigo_tse,
                "cod_ibge_municipio": codigo_ibge,
                "uf": uf,
                "nome_padronizado": nome,
                "zona_eleitoral": pd.NA,
                "secao_eleitoral": pd.NA,
                "latitude": latitude,
                "longitude": longitude,
                "geohash": geohash,
                "vigencia_inicio": vigencia_inicio,
                "vigencia_fim": vigencia_fim,
            }
        )

    dim_territorio = pd.DataFrame(base_rows)
    if secao_df.empty:
        return validate_silver_dim_territorio(dim_territorio)

    municipio_col = _pick_column(secao_df, ["municipio", "nome_municipio"], label="municipio")
    secao_work = secao_df.copy()
    secao_work["municipio_norm_input"] = secao_work[municipio_col].astype(str).map(_normalize_text)
    secao_match = layered_match_territory(
        base_df=secao_work,
        dim_municipio=dim_municipio,
        dim_alias=dim_alias,
        input_name_col=municipio_col,
    )
    secao_work = secao_match.matched_df
    if "uf" in secao_work.columns:
        secao_work["uf_resolved"] = secao_work["uf"].astype(str).str.upper().str.strip()
    else:
        secao_work["uf_resolved"] = inputs.uf

    zone_rows = []
    section_rows = []
    for _, row in secao_work.iterrows():
        codigo_ibge = str(row.get("municipio_id_ibge7", "")).strip()
        codigo_tse = str(row.get("codigo_tse", "")).strip()
        if not codigo_ibge:
            continue
        nome = str(row.get("nome_municipio", "")).strip()
        zona = _normalize_optional_int(row.get("zona"))
        secao = _normalize_optional_int(row.get("secao"))
        uf = str(row.get("uf_resolved", inputs.uf)).strip().upper() or inputs.uf
        if zona is not None:
            zone_rows.append(
                {
                    "territorio_id": f"zona:{codigo_ibge}:{zona}:0",
                    "cod_tse_municipio": codigo_tse,
                    "cod_ibge_municipio": codigo_ibge,
                    "uf": uf,
                    "nome_padronizado": nome,
                    "zona_eleitoral": zona,
                    "secao_eleitoral": pd.NA,
                    "latitude": pd.NA,
                    "longitude": pd.NA,
                    "geohash": None,
                    "vigencia_inicio": vigencia_inicio,
                    "vigencia_fim": vigencia_fim,
                }
            )
        if zona is not None and secao is not None:
            section_rows.append(
                {
                    "territorio_id": f"secao:{codigo_ibge}:{zona}:{secao}",
                    "cod_tse_municipio": codigo_tse,
                    "cod_ibge_municipio": codigo_ibge,
                    "uf": uf,
                    "nome_padronizado": nome,
                    "zona_eleitoral": zona,
                    "secao_eleitoral": secao,
                    "latitude": pd.NA,
                    "longitude": pd.NA,
                    "geohash": None,
                    "vigencia_inicio": vigencia_inicio,
                    "vigencia_fim": vigencia_fim,
                }
            )

    dim_territorio = pd.concat(
        [dim_territorio, pd.DataFrame(zone_rows), pd.DataFrame(section_rows)],
        ignore_index=True,
    )
    dim_territorio = dim_territorio.drop_duplicates(subset=["territorio_id"]).reset_index(drop=True)
    return validate_silver_dim_territorio(dim_territorio)


def _build_silver_fato_municipio(base_df: pd.DataFrame, dim_alias: pd.DataFrame, dim_municipio: pd.DataFrame, inputs: MedallionInputs) -> pd.DataFrame:
    if "municipio" not in base_df.columns or "ranking_final" not in base_df.columns:
        raise MedallionPipelineError("base eleitoral deve conter colunas 'municipio' e 'ranking_final'")
    silver = base_df.copy()
    silver["municipio_norm_input"] = silver["municipio"].astype(str).map(_normalize_text)
    ano, mes, turno = _resolve_temporal_fields(silver, inputs)
    silver["ano"] = ano
    silver["mes"] = mes
    silver["turno"] = turno
    code_col = None
    for candidate in ("codigo_tse", "cod_tse", "cd_mun_tse"):
        if candidate in silver.columns:
            code_col = candidate
            break
    match_result = layered_match_territory(
        base_df=silver,
        dim_municipio=dim_municipio,
        dim_alias=dim_alias,
        input_name_col="municipio",
        input_code_col=code_col,
    )
    silver = match_result.matched_df
    silver["canonical_key"] = _build_canonical_key(silver)
    silver["join_confidence"] = pd.to_numeric(silver.get("join_confidence"), errors="coerce")

    sort_cols = ["canonical_key"]
    if "indice_final" in silver.columns:
        silver["indice_final"] = pd.to_numeric(silver["indice_final"], errors="coerce")
        sort_cols.append("indice_final")
    silver = silver.sort_values(sort_cols, ascending=[True, False] if len(sort_cols) > 1 else [True])
    silver = silver.drop_duplicates(subset=["canonical_key"], keep="first")
    silver = validate_silver_fato_municipio(silver)
    silver.attrs["review_queue_df"] = match_result.review_queue_df
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


def _enrich_ibge_metrics(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["pop_total"] = _metric_column(out, ["pop_total", "populacao", "pop_censo2022"])
    out["densidade_demografica"] = _metric_column(out, ["densidade", "densidade_demografica"])
    out["renda_media"] = _metric_column(out, ["renda_media", "renda", "renda_per_capita"])
    out["educacao_indice"] = _metric_column(out, ["educacao_indice", "indice_educacao", "ideb", "escolaridade"])
    out["urbanizacao_pct"] = _metric_column(out, ["urbanizacao", "taxa_urbanizacao", "urbanizacao_pct"])
    out["idade_mediana"] = _metric_column(out, ["idade_mediana", "mediana_idade", "idade"])
    out["acesso_internet_pct"] = _metric_column(out, ["acesso_internet", "internet_pct", "domicilios_internet"])
    out["estrutura_urbana_indice"] = _metric_column(
        out,
        ["estrutura_urbana", "infraestrutura_urbana", "estrutura_urbana_indice"],
    )
    out["ruralidade_pct"] = _metric_column(out, ["ruralidade", "ruralidade_pct", "populacao_rural_pct"])
    return out


def _enrich_seade_metrics(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["ipvs"] = _metric_column(out, ["ipvs", "indice_ipvs"])
    out["emprego_formal"] = _metric_column(out, ["emprego_formal", "emprego", "taxa_emprego"])
    out["urbanizacao_pct"] = _metric_column(out, ["urbanizacao", "taxa_urbanizacao", "urbanizacao_pct"])
    out["acesso_internet_pct"] = _metric_column(out, ["acesso_internet", "internet_pct", "domicilios_internet"])
    out["estrutura_urbana_indice"] = _metric_column(
        out,
        ["estrutura_urbana", "infraestrutura_urbana", "estrutura_urbana_indice"],
    )
    out["ruralidade_pct"] = _metric_column(out, ["ruralidade", "ruralidade_pct", "populacao_rural_pct"])
    out["saude"] = _metric_column(out, ["saude", "indice_saude", "cobertura_saude"])
    return out


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
        ibge_merge = _enrich_ibge_metrics(ibge_merge)
    mart_potencial = base_metrics.merge(
        ibge_merge[
            [
                "municipio_id_ibge7",
                "pop_total",
                "densidade_demografica",
                "renda_media",
                "educacao_indice",
                "urbanizacao_pct",
                "idade_mediana",
                "acesso_internet_pct",
                "estrutura_urbana_indice",
                "ruralidade_pct",
            ]
        ]
        if not ibge_merge.empty
        else pd.DataFrame(
            columns=[
                "municipio_id_ibge7",
                "pop_total",
                "densidade_demografica",
                "renda_media",
                "educacao_indice",
                "urbanizacao_pct",
                "idade_mediana",
                "acesso_internet_pct",
                "estrutura_urbana_indice",
                "ruralidade_pct",
            ]
        ),
        on="municipio_id_ibge7",
        how="left",
    )
    mart_potencial["pop_norm"] = _normalize_metric(mart_potencial.fillna({"pop_total": 0.0}), "pop_total")
    mart_potencial["renda_norm"] = _normalize_metric(mart_potencial.fillna({"renda_media": 0.0}), "renda_media")
    mart_potencial["educacao_norm"] = _normalize_metric(mart_potencial.fillna({"educacao_indice": 0.0}), "educacao_indice")
    mart_potencial["internet_norm"] = _normalize_metric(mart_potencial.fillna({"acesso_internet_pct": 0.0}), "acesso_internet_pct")
    mart_potencial["urbanizacao_norm"] = _normalize_metric(mart_potencial.fillna({"urbanizacao_pct": 0.0}), "urbanizacao_pct")
    mart_potencial["potencial_eleitoral_ajustado_social"] = (
        pd.to_numeric(mart_potencial["indice_medio_3ciclos"], errors="coerce").fillna(0.0)
        * (
            0.30 * mart_potencial["pop_norm"]
            + 0.20 * mart_potencial["renda_norm"]
            + 0.20 * mart_potencial["educacao_norm"]
            + 0.15 * mart_potencial["internet_norm"]
            + 0.15 * mart_potencial["urbanizacao_norm"]
            + 1.0
        )
    )
    mart_potencial["janela_anos"] = ",".join(str(y) for y in sorted(years_selected)) if years_selected else ""

    seade_merge = silver_seade.copy()
    if not seade_merge.empty:
        seade_merge = _enrich_seade_metrics(seade_merge)
    mart_territorial = base_metrics.merge(
        seade_merge[
            [
                "municipio_id_ibge7",
                "ipvs",
                "emprego_formal",
                "urbanizacao_pct",
                "acesso_internet_pct",
                "estrutura_urbana_indice",
                "ruralidade_pct",
                "saude",
            ]
        ]
        if not seade_merge.empty
        else pd.DataFrame(
            columns=[
                "municipio_id_ibge7",
                "ipvs",
                "emprego_formal",
                "urbanizacao_pct",
                "acesso_internet_pct",
                "estrutura_urbana_indice",
                "ruralidade_pct",
                "saude",
            ]
        ),
        on="municipio_id_ibge7",
        how="left",
    )
    mart_territorial["emprego"] = mart_territorial.get("emprego_formal", 0.0)
    mart_territorial["ipvs_norm"] = _normalize_metric(mart_territorial.fillna({"ipvs": 0.0}), "ipvs")
    mart_territorial["emprego_norm"] = _normalize_metric(mart_territorial.fillna({"emprego": 0.0}), "emprego")
    mart_territorial["saude_norm"] = _normalize_metric(mart_territorial.fillna({"saude": 0.0}), "saude")
    mart_territorial["estrutura_urbana_norm"] = _normalize_metric(
        mart_territorial.fillna({"estrutura_urbana_indice": 0.0}),
        "estrutura_urbana_indice",
    )
    mart_territorial["ruralidade_norm"] = _normalize_metric(
        mart_territorial.fillna({"ruralidade_pct": 0.0}),
        "ruralidade_pct",
    )
    mart_territorial["score_priorizacao_territorial_sp"] = (
        pd.to_numeric(mart_territorial["indice_medio_3ciclos"], errors="coerce").fillna(0.0)
        * (0.45 * mart_territorial["ipvs_norm"] + 0.30 * mart_territorial["emprego_norm"] + 0.25 * mart_territorial["saude_norm"] + 1.0)
    )
    custo_mobilizacao = (
        0.30 * (1.0 - mart_territorial["estrutura_urbana_norm"])
        + 0.25 * mart_territorial["ruralidade_norm"]
        + 0.20 * (1.0 - _normalize_metric(mart_territorial.fillna({"acesso_internet_pct": 0.0}), "acesso_internet_pct"))
        + 0.15 * (1.0 - _normalize_metric(mart_territorial.fillna({"urbanizacao_pct": 0.0}), "urbanizacao_pct"))
        + 0.10 * (1.0 - mart_territorial["emprego_norm"])
    )
    mart_territorial["janela_anos"] = ",".join(str(y) for y in sorted(years_selected)) if years_selected else ""

    mart_custo_mobilizacao = mart_territorial[
        [
            "municipio_id_ibge7",
            "ranking_medio_3ciclos",
            "indice_medio_3ciclos",
            "anos_observados",
            "emprego_formal",
            "urbanizacao_pct",
            "acesso_internet_pct",
            "estrutura_urbana_indice",
            "ruralidade_pct",
            "estrutura_urbana_norm",
            "ruralidade_norm",
            "emprego_norm",
        ]
    ].copy()
    mart_custo_mobilizacao["custo_mobilizacao_relativo"] = custo_mobilizacao
    mart_custo_mobilizacao["janela_anos"] = mart_territorial["janela_anos"]

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
        "mart_custo_mobilizacao": mart_custo_mobilizacao,
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
    serving_root = paths.gold_serving_root
    serving_root.mkdir(parents=True, exist_ok=True)
    serving_db_path = serving_root / "serving.duckdb"
    stats_path = serving_root / f"serving_stats_{run_id}.json"
    cache_path = serving_root / "query_cache.parquet"

    table_stats: list[dict[str, Any]] = []

    def _safe_identifier(value: str) -> str:
        cleaned = "".join(ch for ch in value if ch.isalnum() or ch == "_")
        if not cleaned:
            raise MedallionPipelineError("identificador SQL invalido para tabela de serving")
        return cleaned

    try:
        import duckdb  # type: ignore

        conn = duckdb.connect(str(serving_db_path))
        try:
            for table_name, df in marts.items():
                safe_table = _safe_identifier(table_name)
                conn.register("_tmp_df", df)
                conn.execute(f"CREATE OR REPLACE TABLE {safe_table} AS SELECT * FROM _tmp_df")  # nosec B608
                if "municipio_id_ibge7" in df.columns:
                    conn.execute(
                        f"CREATE INDEX IF NOT EXISTS idx_{safe_table}_municipio ON {safe_table}(municipio_id_ibge7)"
                    )  # nosec B608
                if "canonical_key" in df.columns:
                    conn.execute(
                        f"CREATE INDEX IF NOT EXISTS idx_{safe_table}_ckey ON {safe_table}(canonical_key)"
                    )  # nosec B608
                conn.execute(f"ANALYZE {safe_table}")  # nosec B608
                row_count = int(conn.execute(f"SELECT COUNT(*) FROM {safe_table}").fetchone()[0])  # nosec B608
                table_stats.append({"table": safe_table, "rows": row_count})

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
    run_dir = paths.ingestion_root / "pipeline_runs" / pipeline_version / run_id
    bronze_dir = run_dir / "bronze"
    silver_dir = run_dir / "silver"
    gold_dir = run_dir / "gold"
    lake_root = paths.lake_root
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
    dim_territorio = _build_dim_territorio(inputs.mapping_csv_path, dim_municipio, dim_alias, secao_df, inputs)
    silver_fato_municipio = _build_silver_fato_municipio(base_df, dim_alias, dim_municipio, inputs)
    review_queue_df = silver_fato_municipio.attrs.get("review_queue_df", pd.DataFrame())
    silver_fato_municipio.attrs = {}
    silver_fato_secao = _build_silver_fato_secao(secao_df, dim_alias, inputs)
    silver_socio = _build_silver_socio(socio_df)
    silver_ibge = _build_silver_context_df(ibge_df, ["codigo_ibge", "cod_ibge", "id_municipio_ibge"])
    silver_seade = _build_silver_context_df(seade_df, ["codigo_ibge", "cod_ibge", "id_municipio_ibge"])
    silver_fiscal = _build_silver_context_df(fiscal_df, ["codigo_ibge", "cod_ibge", "id_municipio_ibge"])
    dim_tempo = _build_dim_tempo(
        silver_municipio=silver_fato_municipio,
        silver_secao=silver_fato_secao,
        silver_fiscal=silver_fiscal,
        inputs=inputs,
    )

    silver_outputs = {
        "dim_municipio": dim_municipio,
        "dim_municipio_alias": dim_alias,
        "dim_territorio": dim_territorio,
        "dim_tempo": dim_tempo,
        "manual_review_queue": review_queue_df,
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
    gold_outputs["dim_territorio"] = dim_territorio.copy()
    gold_outputs["dim_tempo"] = dim_tempo.copy()
    gold_paths: dict[str, str] = {}
    published_paths: dict[str, str] = {}
    paths.gold_root.mkdir(parents=True, exist_ok=True)
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

        publish_path = paths.gold_root / f"{name}_{run_id}.parquet"
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
        "manual_review_rows": int(len(review_queue_df)),
        "update_delay_days": {k: round(v, 3) for k, v in update_delay.items()},
        "drift_score": round(float(drift.get("drift_score", 0.0)), 6),
        "drift_alert": bool(drift.get("drift_alert", 0.0) >= 1.0),
    }
    dataset_manifests = {
        name: build_load_manifest(
            source_name=name,
            collected_at_utc=datetime.now(UTC).isoformat(),
            dataset_path=Path(path),
            df=gold_outputs[name],
            parser_version=pipeline_version,
            quality={
                "status": "ok",
                "rows": int(len(gold_outputs[name])),
                "join_success_pct": quality_metrics["join_success_pct"],
                "null_critical_pct": quality_metrics["null_critical_pct"],
                "drift_score": quality_metrics["drift_score"],
            },
        )
        for name, path in published_paths.items()
    }

    settings = get_settings()
    retention_result = {
        "pipeline_runs": enforce_retention_policy(paths.ingestion_root / "pipeline_runs", retention_days=settings.retention_days),
        "lake": enforce_retention_policy(paths.lake_root, retention_days=settings.retention_days),
        "serving": enforce_retention_policy(paths.gold_serving_root, retention_days=settings.retention_days),
    }

    manifest = {
        "pipeline_version": pipeline_version,
        "run_id": run_id,
        "created_at_utc": datetime.now(UTC).isoformat(),
        "dataset_manifests": dataset_manifests,
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
            "matching": {
                "layers": [
                    "exact_code",
                    "exact_name",
                    "historical_alias",
                    "fuzzy_score",
                    "manual_review",
                ],
                "contract_fields": [
                    "join_status",
                    "join_method",
                    "join_confidence",
                    "needs_review",
                ],
                "manual_review_rows": int(len(review_queue_df)),
            },
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

