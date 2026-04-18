from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import pandas as pd

MASTER_COLUMNS = [
    "ANO_ELEICAO",
    "SIGLA_UF",
    "COD_MUN_TSE",
    "COD_MUN_IBGE",
    "MUNICIPIO",
    "ZONA",
    "SECAO",
    "LOCAL_VOTACAO",
    "ID_CANDIDATO",
    "CD_SETOR",
    "territorio_id",
    "MUNICIPIO_NORMALIZADO",
    "join_method",
    "join_confidence",
    "join_ambiguity_flag",
    "join_ambiguity_reason",
    "join_candidates",
]


def normalize_name(value: Any) -> str:
    text = str(value or "").strip().upper()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _digits(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _series(df: pd.DataFrame, column: str, default: Any = "") -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series([default] * len(df), index=df.index)



def _series_first(df: pd.DataFrame, candidates: list[str], default: Any = "") -> pd.Series:
    for column in candidates:
        if column in df.columns:
            return df[column]
    return pd.Series([default] * len(df), index=df.index)


def _first_existing(df: pd.DataFrame, candidates: list[str]) -> str | None:
    normalized = {str(col).lower(): str(col) for col in df.columns}
    for candidate in candidates:
        if candidate.lower() in normalized:
            return normalized[candidate.lower()]
    return None


@dataclass(frozen=True)
class CrosswalkQuality:
    total: int
    exact_matches: int
    normalized_name_matches: int
    fuzzy_matches: int
    ambiguous: int
    unmatched: int
    mean_confidence: float


@dataclass(frozen=True)
class CrosswalkResult:
    dataframe: pd.DataFrame
    exact_matches: int
    fuzzy_matches: int
    unmatched: int
    ambiguous: int = 0
    normalized_name_matches: int = 0
    mean_confidence: float = 0.0

    @property
    def quality(self) -> CrosswalkQuality:
        return CrosswalkQuality(
            total=int(len(self.dataframe)),
            exact_matches=self.exact_matches,
            normalized_name_matches=self.normalized_name_matches,
            fuzzy_matches=self.fuzzy_matches,
            ambiguous=self.ambiguous,
            unmatched=self.unmatched,
            mean_confidence=self.mean_confidence,
        )


class MunicipioCrosswalkBuilder:
    def build(self, tse_df: pd.DataFrame, ibge_df: pd.DataFrame, *, fuzzy_threshold: float = 0.92) -> CrosswalkResult:
        tse = self._prepare_tse(tse_df)
        ibge = self._prepare_ibge(ibge_df)
        ibge_by_code = {row.COD_MUN_IBGE: row for row in ibge.itertuples(index=False) if row.COD_MUN_IBGE}
        ibge_by_name = {
            name: group.reset_index(drop=True)
            for name, group in ibge.groupby("MUNICIPIO_NORMALIZADO", dropna=False)
            if str(name)
        }
        ibge_names = list(ibge_by_name)

        records: list[dict[str, Any]] = []
        for row in tse.itertuples(index=False):
            record = row._asdict()
            declared_ibge = str(record.get("COD_MUN_IBGE", ""))
            name_norm = str(record.get("MUNICIPIO_NORMALIZADO", ""))
            match = self._empty_match()
            if declared_ibge and declared_ibge in ibge_by_code:
                ibge_row = ibge_by_code[declared_ibge]
                match = self._match_from_ibge(ibge_row, method="exact_declared_tse_ibge", confidence=1.0)
            elif name_norm in ibge_by_name:
                candidates = ibge_by_name[name_norm]
                if len(candidates) == 1:
                    match = self._match_from_series(candidates.iloc[0], method="normalized_name_unique", confidence=0.9)
                else:
                    match = self._ambiguous_match(candidates, reason="multiple_ibge_candidates_same_normalized_name")
            else:
                fuzzy = self._best_fuzzy(name_norm, ibge_names)
                if fuzzy is not None and fuzzy[1] >= fuzzy_threshold:
                    candidates = ibge_by_name[fuzzy[0]]
                    if len(candidates) == 1:
                        match = self._match_from_series(candidates.iloc[0], method="fuzzy_name", confidence=round(float(fuzzy[1]) * 0.82, 6))
                    else:
                        match = self._ambiguous_match(candidates, reason="multiple_ibge_candidates_after_fuzzy")
                else:
                    match = self._empty_match(method="unmatched", confidence=0.0)
            records.append({**record, **match})

        out = pd.DataFrame(records)
        out["join_ambiguity_flag"] = out["join_ambiguity_flag"].fillna(False).astype(bool)
        exact = int(out["join_method"].eq("exact_declared_tse_ibge").sum())
        normalized = int(out["join_method"].eq("normalized_name_unique").sum())
        fuzzy_count = int(out["join_method"].eq("fuzzy_name").sum())
        ambiguous = int(out["join_ambiguity_flag"].sum())
        unmatched = int(out["join_method"].eq("unmatched").sum())
        mean_confidence = round(float(pd.to_numeric(out["join_confidence"], errors="coerce").fillna(0.0).mean()), 6)
        return CrosswalkResult(
            dataframe=out,
            exact_matches=exact,
            normalized_name_matches=normalized,
            fuzzy_matches=fuzzy_count,
            ambiguous=ambiguous,
            unmatched=unmatched,
            mean_confidence=mean_confidence,
        )

    def _prepare_tse(self, df: pd.DataFrame) -> pd.DataFrame:
        cod_tse_col = _first_existing(df, ["COD_MUN_TSE", "cod_tse_municipio", "CD_MUNICIPIO"])
        cod_ibge_col = _first_existing(df, ["COD_MUN_IBGE", "cod_municipio_ibge", "municipio_id_ibge7"])
        municipio_col = _first_existing(df, ["MUNICIPIO", "municipio", "NM_MUNICIPIO"])
        uf_col = _first_existing(df, ["SIGLA_UF", "uf", "SG_UF"])
        if municipio_col is None:
            raise ValueError("tse_df precisa conter coluna de municipio")
        out = pd.DataFrame(
            {
                "SIGLA_UF": df[uf_col].astype(str).str.upper().str.strip() if uf_col else "",
                "COD_MUN_TSE": df[cod_tse_col].map(_digits) if cod_tse_col else "",
                "COD_MUN_IBGE": df[cod_ibge_col].map(_digits) if cod_ibge_col else "",
                "MUNICIPIO": df[municipio_col].astype(str).str.strip(),
            }
        )
        out["MUNICIPIO_NORMALIZADO"] = out["MUNICIPIO"].map(normalize_name)
        return out.drop_duplicates(["SIGLA_UF", "COD_MUN_TSE", "MUNICIPIO_NORMALIZADO"]).reset_index(drop=True)

    def _prepare_ibge(self, df: pd.DataFrame) -> pd.DataFrame:
        cod_ibge_col = _first_existing(df, ["COD_MUN_IBGE", "cod_municipio_ibge", "CD_MUN", "municipio_id_ibge7"])
        municipio_col = _first_existing(df, ["MUNICIPIO", "municipio", "NM_MUNICIPIO", "nome"])
        uf_col = _first_existing(df, ["SIGLA_UF", "uf", "SG_UF"])
        if cod_ibge_col is None or municipio_col is None:
            raise ValueError("ibge_df precisa conter COD_MUN_IBGE e MUNICIPIO")
        out = pd.DataFrame(
            {
                "SIGLA_UF_IBGE": df[uf_col].astype(str).str.upper().str.strip() if uf_col else "",
                "COD_MUN_IBGE_MATCH": df[cod_ibge_col].map(_digits),
                "MUNICIPIO_IBGE": df[municipio_col].astype(str).str.strip(),
            }
        )
        out["COD_MUN_IBGE"] = out["COD_MUN_IBGE_MATCH"]
        out["MUNICIPIO_NORMALIZADO"] = out["MUNICIPIO_IBGE"].map(normalize_name)
        return out.drop_duplicates(["COD_MUN_IBGE", "MUNICIPIO_NORMALIZADO"]).reset_index(drop=True)

    def _empty_match(self, *, method: str = "unmatched", confidence: float = 0.0) -> dict[str, Any]:
        return {
            "COD_MUN_IBGE_MATCH": "",
            "MUNICIPIO_IBGE": "",
            "join_method": method,
            "join_confidence": confidence,
            "join_ambiguity_flag": False,
            "join_ambiguity_reason": "",
            "join_candidates": "",
        }

    def _match_from_ibge(self, ibge_row: Any, *, method: str, confidence: float) -> dict[str, Any]:
        return {
            "COD_MUN_IBGE_MATCH": str(ibge_row.COD_MUN_IBGE_MATCH),
            "MUNICIPIO_IBGE": str(ibge_row.MUNICIPIO_IBGE),
            "join_method": method,
            "join_confidence": confidence,
            "join_ambiguity_flag": False,
            "join_ambiguity_reason": "",
            "join_candidates": str(ibge_row.COD_MUN_IBGE_MATCH),
        }

    def _match_from_series(self, row: pd.Series, *, method: str, confidence: float) -> dict[str, Any]:
        return {
            "COD_MUN_IBGE_MATCH": str(row["COD_MUN_IBGE_MATCH"]),
            "MUNICIPIO_IBGE": str(row["MUNICIPIO_IBGE"]),
            "join_method": method,
            "join_confidence": confidence,
            "join_ambiguity_flag": False,
            "join_ambiguity_reason": "",
            "join_candidates": str(row["COD_MUN_IBGE_MATCH"]),
        }

    def _ambiguous_match(self, candidates: pd.DataFrame, *, reason: str) -> dict[str, Any]:
        return {
            "COD_MUN_IBGE_MATCH": "",
            "MUNICIPIO_IBGE": "",
            "join_method": "ambiguous",
            "join_confidence": 0.0,
            "join_ambiguity_flag": True,
            "join_ambiguity_reason": reason,
            "join_candidates": ",".join(candidates["COD_MUN_IBGE_MATCH"].astype(str).tolist()),
        }

    def _best_fuzzy(self, name: str, candidates: list[str]) -> tuple[str, float] | None:
        if not name or not candidates:
            return None
        scored = [(candidate, SequenceMatcher(None, name, candidate).ratio()) for candidate in candidates]
        return max(scored, key=lambda item: item[1])


def build_municipio_crosswalk(tse_df: pd.DataFrame, ibge_df: pd.DataFrame) -> CrosswalkResult:
    return MunicipioCrosswalkBuilder().build(tse_df, ibge_df)


@dataclass(frozen=True)
class MasterIndexResult:
    dataframe: pd.DataFrame
    parquet_path: Path | None
    duckdb_path: Path | None
    manifest_path: Path | None
    quality: dict[str, Any]


class TerritorialMasterIndexBuilder:
    def build_master_index(
        self,
        *,
        zone_fact: pd.DataFrame | None = None,
        section_fact: pd.DataFrame | None = None,
        ibge_municipios: pd.DataFrame | None = None,
        candidate_id: str = "",
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        if zone_fact is not None and not zone_fact.empty:
            frames.append(self.build_from_zone_fact(zone_fact, candidate_id=candidate_id))
        if section_fact is not None and not section_fact.empty:
            frames.append(self.build_from_section_fact(section_fact, candidate_id=candidate_id))
        if not frames:
            return pd.DataFrame(columns=MASTER_COLUMNS)
        master = pd.concat(frames, ignore_index=True).drop_duplicates("territorio_id").reset_index(drop=True)
        if ibge_municipios is not None and not ibge_municipios.empty:
            crosswalk = build_municipio_crosswalk(master, ibge_municipios)
            master = self.apply_crosswalk(master, crosswalk.dataframe)
        return master[MASTER_COLUMNS]

    def build_from_zone_fact(self, zone_fact: pd.DataFrame, *, candidate_id: str = "") -> pd.DataFrame:
        df = zone_fact.copy()
        out = pd.DataFrame(
            {
                "ANO_ELEICAO": pd.to_numeric(_series_first(df, ["ano_eleicao", "ANO_ELEICAO"], 0), errors="coerce")
                .fillna(0)
                .astype(int),
                "SIGLA_UF": _series_first(df, ["uf", "SIGLA_UF"], "").astype(str).str.upper().str.strip(),
                "COD_MUN_TSE": _series_first(df, ["cod_tse_municipio", "COD_MUN_TSE"], "").map(_digits),
                "COD_MUN_IBGE": _series_first(df, ["municipio_id_ibge7", "COD_MUN_IBGE"], "").map(_digits),
                "MUNICIPIO": _series_first(df, ["municipio", "MUNICIPIO"], "").astype(str).str.strip(),
                "ZONA": pd.to_numeric(_series_first(df, ["zona_eleitoral", "ZONA"], 0), errors="coerce")
                .fillna(0)
                .astype(int),
                "SECAO": 0,
                "LOCAL_VOTACAO": "",
                "ID_CANDIDATO": candidate_id,
                "CD_SETOR": "",
                "join_method": _series(df, "match_method", "exact_tse_municipio_zone").astype(str),
                "join_confidence": pd.to_numeric(_series(df, "join_confidence", 0.9), errors="coerce").fillna(0.0),
            }
        )
        out["MUNICIPIO_NORMALIZADO"] = out["MUNICIPIO"].map(normalize_name)
        out["join_ambiguity_flag"] = False
        out["join_ambiguity_reason"] = ""
        out["join_candidates"] = out["COD_MUN_IBGE"]
        out["territorio_id"] = (
            out["ANO_ELEICAO"].astype(str)
            + ":"
            + out["SIGLA_UF"]
            + ":"
            + out["COD_MUN_TSE"].replace("", "SEM_TSE")
            + ":ZE"
            + out["ZONA"].astype(str)
        )
        return out[MASTER_COLUMNS].drop_duplicates("territorio_id").reset_index(drop=True)

    def build_from_section_fact(self, section_fact: pd.DataFrame, *, candidate_id: str = "") -> pd.DataFrame:
        df = section_fact.copy()
        out = pd.DataFrame(
            {
                "ANO_ELEICAO": pd.to_numeric(_series_first(df, ["ano_eleicao", "ANO_ELEICAO"], 0), errors="coerce")
                .fillna(0)
                .astype(int),
                "SIGLA_UF": _series_first(df, ["uf", "SIGLA_UF"], "").astype(str).str.upper().str.strip(),
                "COD_MUN_TSE": _series_first(df, ["cod_tse_municipio", "COD_MUN_TSE"], "").map(_digits),
                "COD_MUN_IBGE": _series_first(df, ["municipio_id_ibge7", "COD_MUN_IBGE"], "").map(_digits),
                "MUNICIPIO": _series_first(df, ["municipio", "MUNICIPIO"], "").astype(str).str.strip(),
                "ZONA": pd.to_numeric(_series_first(df, ["zona_eleitoral", "ZONA"], 0), errors="coerce")
                .fillna(0)
                .astype(int),
                "SECAO": pd.to_numeric(_series_first(df, ["secao_eleitoral", "SECAO"], 0), errors="coerce")
                .fillna(0)
                .astype(int),
                "LOCAL_VOTACAO": _series_first(df, ["local_votacao", "LOCAL_VOTACAO"], "").astype(str),
                "ID_CANDIDATO": candidate_id,
                "CD_SETOR": _series_first(df, ["setor_censitario", "CD_SETOR"], "").map(_digits),
                "join_method": pd.Series(["exact_tse_municipio_zone_section"] * len(df), index=df.index),
                "join_confidence": pd.to_numeric(_series(df, "join_confidence", 0.92), errors="coerce").fillna(0.0),
            }
        )
        out["MUNICIPIO_NORMALIZADO"] = out["MUNICIPIO"].map(normalize_name)
        out["join_ambiguity_flag"] = False
        out["join_ambiguity_reason"] = ""
        out["join_candidates"] = out["COD_MUN_IBGE"]
        out["territorio_id"] = (
            out["ANO_ELEICAO"].astype(str)
            + ":"
            + out["SIGLA_UF"]
            + ":"
            + out["COD_MUN_TSE"].replace("", "SEM_TSE")
            + ":ZE"
            + out["ZONA"].astype(str)
            + ":S"
            + out["SECAO"].astype(str)
        )
        return out[MASTER_COLUMNS].drop_duplicates("territorio_id").reset_index(drop=True)

    def apply_crosswalk(self, master: pd.DataFrame, crosswalk_df: pd.DataFrame) -> pd.DataFrame:
        xwalk_cols = [
            "SIGLA_UF",
            "COD_MUN_TSE",
            "MUNICIPIO_NORMALIZADO",
            "COD_MUN_IBGE_MATCH",
            "join_method",
            "join_confidence",
            "join_ambiguity_flag",
            "join_ambiguity_reason",
            "join_candidates",
        ]
        crosswalk = crosswalk_df[xwalk_cols].drop_duplicates(["SIGLA_UF", "COD_MUN_TSE", "MUNICIPIO_NORMALIZADO"])
        merged = master.merge(
            crosswalk,
            on=["SIGLA_UF", "COD_MUN_TSE", "MUNICIPIO_NORMALIZADO"],
            how="left",
            suffixes=("", "_crosswalk"),
        )
        has_match = merged["COD_MUN_IBGE_MATCH"].astype(str).str.len().gt(0)
        merged.loc[has_match, "COD_MUN_IBGE"] = merged.loc[has_match, "COD_MUN_IBGE_MATCH"]
        for col in ["join_method", "join_confidence", "join_ambiguity_flag", "join_ambiguity_reason", "join_candidates"]:
            cross_col = f"{col}_crosswalk"
            if cross_col in merged.columns:
                merged[col] = merged[cross_col].combine_first(merged[col])
        merged["join_ambiguity_flag"] = merged["join_ambiguity_flag"].fillna(False).astype(bool)
        merged["join_confidence"] = pd.to_numeric(merged["join_confidence"], errors="coerce").fillna(0.0).clip(0, 1)
        return merged[MASTER_COLUMNS].drop_duplicates("territorio_id").reset_index(drop=True)

    def publish_master_index(self, master: pd.DataFrame, output_dir: Path, *, dataset_version: str) -> MasterIndexResult:
        output_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = output_dir / f"territorial_master_index_{dataset_version}.parquet"
        master.to_parquet(parquet_path, index=False)
        duckdb_path = self._write_duckdb(master, output_dir, dataset_version)
        quality = self.quality_report(master)
        manifest_path = output_dir / f"territorial_master_index_{dataset_version}_manifest.json"
        manifest = {
            "dataset": "territorial_master_index",
            "dataset_version": dataset_version,
            "created_at_utc": datetime.now(UTC).isoformat(),
            "parquet_path": str(parquet_path),
            "duckdb_path": str(duckdb_path) if duckdb_path else None,
            "columns": MASTER_COLUMNS,
            "quality": quality,
            "join_policy": {
                "exact_declared_tse_ibge": "codigo IBGE declarado na entrada TSE/territorial",
                "normalized_name_unique": "nome normalizado unico no cadastro IBGE",
                "fuzzy_name": "similaridade textual acima do threshold; requer menor confianca",
                "ambiguous": "multiplos candidatos possiveis; nao preencher COD_MUN_IBGE automaticamente",
                "unmatched": "sem correspondencia suficiente",
            },
        }
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return MasterIndexResult(master, parquet_path, duckdb_path, manifest_path, quality)

    def quality_report(self, master: pd.DataFrame) -> dict[str, Any]:
        rows = int(len(master))
        if rows == 0:
            return {"rows": 0, "coverage_ibge": 0.0, "ambiguous": 0, "unmatched": 0, "mean_join_confidence": 0.0}
        cod_ibge_present = master["COD_MUN_IBGE"].astype(str).str.len().gt(0)
        ambiguous = master["join_ambiguity_flag"].fillna(False).astype(bool)
        unmatched = master["join_method"].astype(str).eq("unmatched") | ~cod_ibge_present
        return {
            "rows": rows,
            "unique_territories": int(master["territorio_id"].nunique()),
            "coverage_ibge": round(float(cod_ibge_present.mean()), 6),
            "ambiguous": int(ambiguous.sum()),
            "unmatched": int(unmatched.sum()),
            "mean_join_confidence": round(float(pd.to_numeric(master["join_confidence"], errors="coerce").fillna(0.0).mean()), 6),
            "join_methods": master["join_method"].astype(str).value_counts(dropna=False).to_dict(),
        }

    def _write_duckdb(self, master: pd.DataFrame, output_dir: Path, dataset_version: str) -> Path | None:
        try:
            import duckdb
        except ImportError:
            return None
        duckdb_path = output_dir / f"territorial_master_index_{dataset_version}.duckdb"
        with duckdb.connect(str(duckdb_path)) as con:
            con.register("_master", master)
            con.execute("CREATE OR REPLACE TABLE territorial_master_index AS SELECT * FROM _master")
        return duckdb_path
