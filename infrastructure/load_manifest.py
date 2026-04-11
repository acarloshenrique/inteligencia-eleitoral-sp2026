from __future__ import annotations

from datetime import UTC, datetime
import hashlib
from pathlib import Path
from typing import Any

import pandas as pd


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _detect_schema(df: pd.DataFrame) -> dict[str, str]:
    return {str(col): str(dtype) for col, dtype in df.dtypes.items()}


def _detect_territorial_coverage(df: pd.DataFrame) -> dict[str, Any]:
    coverage: dict[str, Any] = {"granularidade": "indefinida", "rows": int(len(df))}

    if "uf" in df.columns:
        ufs = sorted({str(v).strip().upper() for v in df["uf"].dropna().tolist() if str(v).strip()})
        coverage["ufs"] = ufs
        coverage["granularidade"] = "uf" if ufs else coverage["granularidade"]

    municipio_col = None
    for candidate in ("municipio_id_ibge7", "codigo_ibge", "municipio", "nome_municipio"):
        if candidate in df.columns:
            municipio_col = candidate
            break
    if municipio_col is not None:
        municipios = (
            pd.Series(df[municipio_col])
            .dropna()
            .astype(str)
            .str.strip()
        )
        coverage["municipios_cobertos"] = int(municipios[municipios != ""].nunique())
        coverage["granularidade"] = "municipio"

    return coverage


def _detect_reference_period(df: pd.DataFrame) -> dict[str, Any]:
    period: dict[str, Any] = {"colunas": []}
    for candidate in ("ano", "mes", "turno", "data_referencia", "competencia"):
        if candidate not in df.columns:
            continue
        values = pd.Series(df[candidate]).dropna()
        if values.empty:
            continue
        period["colunas"].append(candidate)
        normalized = sorted({str(v).strip() for v in values.tolist() if str(v).strip()})
        period[candidate] = normalized

    if "ano" in period:
        anos = [int(v) for v in period["ano"] if str(v).isdigit()]
        if anos:
            period["inicio"] = min(anos)
            period["fim"] = max(anos)

    return period


def _default_quality(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        null_pct = 0.0
    else:
        null_pct = float(df.isna().sum().sum() / max(1, df.shape[0] * max(1, df.shape[1])))
    return {
        "status": "ok" if not df.empty else "warning",
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "null_pct": round(null_pct * 100.0, 3),
    }


def build_load_manifest(
    *,
    source_name: str,
    collected_at_utc: str | None,
    dataset_path: Path,
    df: pd.DataFrame,
    parser_version: str,
    quality: dict[str, Any] | None = None,
    reference_period: dict[str, Any] | None = None,
    territorial_coverage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    collected_at = collected_at_utc or datetime.now(UTC).isoformat()
    return {
        "fonte": source_name,
        "data_coleta_utc": collected_at,
        "hash_arquivo": _sha256_file(dataset_path),
        "schema_detectado": _detect_schema(df),
        "cobertura_territorial": territorial_coverage or _detect_territorial_coverage(df),
        "periodo_referencia": reference_period or _detect_reference_period(df),
        "qualidade_carga": quality or _default_quality(df),
        "versao_parser": parser_version,
        "arquivo": str(dataset_path),
    }
