from __future__ import annotations

import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from domain.lgpd import anonymize_columns, minimize_dataframe

PII_CANDIDATE_COLUMNS = {
    "cpf",
    "email",
    "telefone",
    "celular",
    "titulo_eleitor",
    "nome_pessoa",
    "documento",
}


def apply_lgpd_purpose_policy(df: pd.DataFrame, *, purpose: str, salt: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    out = df.copy()
    minimized = False
    if purpose in {"serving", "serving_cache"}:
        out = minimize_dataframe(
            out,
            allowed_columns=[
                "municipio_id_ibge7",
                "canonical_key",
                "ranking_medio_3ciclos",
                "indice_medio_3ciclos",
                "potencial_eleitoral_ajustado_social",
                "score_priorizacao_territorial_sp",
                "sensibilidade_investimento_publico",
                "janela_anos",
                "ano",
                "turno",
                "ranking_final",
            ],
        )
        minimized = True

    pii_present = [c for c in out.columns if c.lower() in PII_CANDIDATE_COLUMNS]
    if pii_present:
        out = anonymize_columns(out, pii_present, salt=salt)
    return out, {"minimized": minimized, "anonymized_columns": pii_present}


def enforce_retention_policy(base_dir: Path, *, retention_days: int) -> dict[str, int]:
    if not base_dir.exists():
        return {"removed": 0}
    cutoff = datetime.now(UTC).timestamp() - max(1, int(retention_days)) * 86400
    removed = 0
    for path in base_dir.rglob("*"):
        if not path.exists():
            continue
        try:
            mtime = path.stat().st_mtime
        except OSError:
            continue
        if mtime >= cutoff:
            continue
        if path.is_file():
            try:
                path.unlink()
                removed += 1
            except OSError:
                continue
        elif path.is_dir():
            try:
                shutil.rmtree(path)
                removed += 1
            except OSError:
                continue
    return {"removed": removed}
