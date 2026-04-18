from __future__ import annotations

import re
import unicodedata
from typing import Any

import pandas as pd

from data_catalog.models import DataSourceSpec

KEY_ALIASES = {
    "ANO_ELEICAO": ["ANO_ELEICAO", "ano_eleicao", "ANO"],
    "SIGLA_UF": ["SG_UF", "SIGLA_UF", "uf", "UF"],
    "COD_MUN_TSE": ["CD_MUNICIPIO", "COD_MUN_TSE", "cod_tse_municipio"],
    "COD_MUN_IBGE": ["CD_MUN", "COD_MUN_IBGE", "cod_municipio_ibge", "municipio_id_ibge7"],
    "MUNICIPIO": ["NM_MUNICIPIO", "MUNICIPIO", "municipio"],
    "ZONA": ["NR_ZONA", "ZONA", "zona_eleitoral"],
    "SECAO": ["NR_SECAO", "SECAO", "secao_eleitoral"],
    "LOCAL_VOTACAO": ["NM_LOCAL_VOTACAO", "LOCAL_VOTACAO", "local_votacao"],
    "ID_CANDIDATO": ["SQ_CANDIDATO", "ID_CANDIDATO", "candidate_id"],
    "CD_SETOR": ["CD_SETOR", "setor_censitario"],
}


def normalize_text_key(value: Any) -> str:
    text = str(value or "").strip().upper()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def only_digits(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


class KeyHarmonizer:
    def harmonize(self, df: pd.DataFrame, source: DataSourceSpec) -> pd.DataFrame:
        del source
        out = df.copy()
        for canonical, aliases in KEY_ALIASES.items():
            if canonical in out.columns:
                continue
            found = next((alias for alias in aliases if alias in out.columns), None)
            if found is not None:
                out[canonical] = out[found]
        for col in ["COD_MUN_TSE", "COD_MUN_IBGE", "ZONA", "SECAO", "ID_CANDIDATO", "CD_SETOR"]:
            if col in out.columns:
                out[col] = out[col].map(only_digits)
        if "SIGLA_UF" in out.columns:
            out["SIGLA_UF"] = out["SIGLA_UF"].astype(str).str.upper().str.strip()
        if "MUNICIPIO" in out.columns:
            out["MUNICIPIO_NORMALIZADO"] = out["MUNICIPIO"].map(normalize_text_key)
        out["harmonization_version"] = "territorial_keys_v1"
        return out
