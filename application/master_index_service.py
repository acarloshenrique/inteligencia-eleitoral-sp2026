from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

MASTER_INDEX_COLUMNS: list[str] = [
    "master_record_id",
    "ano_eleicao",
    "uf",
    "cod_municipio_tse",
    "cod_municipio_ibge",
    "municipio_nome",
    "zona",
    "secao",
    "local_votacao",
    "candidate_id",
    "numero_candidato",
    "partido",
    "cd_setor",
    "territorial_cluster_id",
    "join_strategy",
    "join_confidence",
    "source_coverage_score",
]


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def normalize_name(value: Any) -> str:
    text = str(value or "").strip().upper()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def digits_only(value: Any) -> str:
    if pd.isna(value):
        return ""
    return "".join(ch for ch in str(value) if ch.isdigit())


def pad_digits(value: Any, width: int) -> str:
    digits = digits_only(value)
    return digits.zfill(width) if digits else ""


def first_existing(df: pd.DataFrame, aliases: list[str]) -> str | None:
    normalized = {str(column).lower(): str(column) for column in df.columns}
    for alias in aliases:
        if alias.lower() in normalized:
            return normalized[alias.lower()]
    return None


def series_first(df: pd.DataFrame, aliases: list[str], default: Any = "") -> pd.Series:
    column = first_existing(df, aliases)
    if column is None:
        return pd.Series([default] * len(df), index=df.index)
    return df[column]


def _read_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix == ".json":
        return pd.read_json(path)
    return pd.read_csv(path, sep=None, engine="python", dtype=str)


class MasterIndexQualityReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_version: str
    rows: int
    unique_master_records: int
    coverage_ibge: float
    coverage_sector: float
    coverage_candidate: float
    mean_join_confidence: float
    mean_source_coverage_score: float
    join_strategy_counts: dict[str, int] = Field(default_factory=dict)
    limitations: list[str] = Field(default_factory=list)
    generated_at_utc: str = Field(default_factory=utc_now_iso)

    def write(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.model_dump(mode="json"), ensure_ascii=False, indent=2), encoding="utf-8")
        return path


@dataclass(frozen=True)
class MasterIndexBuildResult:
    dataframe: pd.DataFrame
    parquet_path: Path | None
    duckdb_path: Path | None
    manifest_path: Path | None
    quality: MasterIndexQualityReport


class MasterIndexBuilder:
    def build(
        self,
        *,
        resultados_secao: pd.DataFrame,
        eleitorado_secao: pd.DataFrame | None = None,
        locais_votacao: pd.DataFrame | None = None,
        candidatos: pd.DataFrame | None = None,
        prestacao_contas: pd.DataFrame | None = None,
        setores_censitarios: pd.DataFrame | None = None,
        municipio_crosswalk: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        base = self._canonical_resultados(resultados_secao)
        base = self._enrich_section_profile(base, eleitorado_secao)
        base = self._enrich_voting_place(base, locais_votacao)
        base = self._enrich_candidates(base, candidatos)
        base = self._enrich_finance_coverage(base, prestacao_contas)
        base = self._enrich_municipality_crosswalk(base, municipio_crosswalk)
        base = self._enrich_census_sector(base, setores_censitarios)
        base = self._finalize(base)
        return base[MASTER_INDEX_COLUMNS]

    def write_gold(self, master: pd.DataFrame, output_dir: Path, *, dataset_version: str) -> MasterIndexBuildResult:
        output_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = output_dir / f"territorial_electoral_master_index_{dataset_version}.parquet"
        master.to_parquet(parquet_path, index=False)
        duckdb_path = self._write_duckdb(master, output_dir, dataset_version)
        quality = self.quality_report(master, dataset_version=dataset_version)
        manifest_path = output_dir / f"territorial_electoral_master_index_{dataset_version}_manifest.json"
        manifest = {
            "dataset_id": "gold_territorial_electoral_master_index",
            "dataset_version": dataset_version,
            "created_at_utc": utc_now_iso(),
            "parquet_path": str(parquet_path),
            "duckdb_path": str(duckdb_path) if duckdb_path else None,
            "schema": MASTER_INDEX_COLUMNS,
            "quality": quality.model_dump(mode="json"),
            "join_documentation": self.join_documentation(),
        }
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return MasterIndexBuildResult(master, parquet_path, duckdb_path, manifest_path, quality)

    def quality_report(self, master: pd.DataFrame, *, dataset_version: str) -> MasterIndexQualityReport:
        rows = int(len(master))
        if rows == 0:
            return MasterIndexQualityReport(
                dataset_version=dataset_version,
                rows=0,
                unique_master_records=0,
                coverage_ibge=0.0,
                coverage_sector=0.0,
                coverage_candidate=0.0,
                mean_join_confidence=0.0,
                mean_source_coverage_score=0.0,
                limitations=["empty_master_index"],
            )
        coverage_ibge = master["cod_municipio_ibge"].astype(str).str.len().gt(0)
        coverage_sector = master["cd_setor"].astype(str).str.len().gt(0)
        coverage_candidate = master["candidate_id"].astype(str).str.len().gt(0)
        join_confidence = pd.to_numeric(master["join_confidence"], errors="coerce").fillna(0.0)
        source_coverage = pd.to_numeric(master["source_coverage_score"], errors="coerce").fillna(0.0)
        limitations: list[str] = []
        if not coverage_sector.all():
            limitations.append("sector join is incomplete; local voting place to census sector may require geocoding")
        if master["join_strategy"].astype(str).str.contains("approx|fuzzy|name", regex=True).any():
            limitations.append(
                "some rows use inferred or approximate joins and must be interpreted with lower confidence"
            )
        if not coverage_ibge.all():
            limitations.append("some rows have no reliable TSE-IBGE municipal match")
        return MasterIndexQualityReport(
            dataset_version=dataset_version,
            rows=rows,
            unique_master_records=int(master["master_record_id"].nunique()),
            coverage_ibge=round(float(coverage_ibge.mean()), 6),
            coverage_sector=round(float(coverage_sector.mean()), 6),
            coverage_candidate=round(float(coverage_candidate.mean()), 6),
            mean_join_confidence=round(float(join_confidence.mean()), 6),
            mean_source_coverage_score=round(float(source_coverage.mean()), 6),
            join_strategy_counts=master["join_strategy"].astype(str).value_counts().to_dict(),
            limitations=limitations,
        )

    def join_documentation(self) -> dict[str, str]:
        return {
            "exact_section": "ano_eleicao + uf + cod_municipio_tse + zona + secao",
            "exact_voting_place": "same section key plus local_votacao when present",
            "exact_candidate": "ano_eleicao + uf + candidate_id",
            "exact_finance_candidate": "ano_eleicao + uf + candidate_id; territorial attribution is not assumed",
            "exact_tse_ibge_code": "cod_municipio_tse mapped to cod_municipio_ibge through governed crosswalk",
            "exact_ibge_code": "cod_municipio_ibge already declared in source",
            "normalized_name_unique": "uf + normalized municipality name has a single IBGE candidate",
            "fuzzy_name": "municipality name matched by similarity threshold; lower confidence",
            "exact_sector": "cd_setor already present in source",
            "approx_municipality_single_sector": "municipality has one available sector in the provided sector table",
            "no_sector_match": "sector could not be assigned without geocoding or explicit cd_setor",
        }

    def _canonical_resultados(self, df: pd.DataFrame) -> pd.DataFrame:
        out = pd.DataFrame(index=df.index)
        out["ano_eleicao"] = pd.to_numeric(
            series_first(df, ["ano_eleicao", "ANO_ELEICAO", "ano"], 0), errors="coerce"
        ).astype("Int64")
        out["uf"] = series_first(df, ["uf", "SIGLA_UF", "sg_uf"], "").astype(str).str.upper().str.strip()
        out["cod_municipio_tse"] = series_first(df, ["cod_municipio_tse", "COD_MUN_TSE", "CD_MUNICIPIO"], "").map(
            lambda value: pad_digits(value, 5)
        )
        out["cod_municipio_ibge"] = series_first(
            df, ["cod_municipio_ibge", "COD_MUN_IBGE", "municipio_id_ibge7"], ""
        ).map(lambda value: pad_digits(value, 7))
        out["municipio_nome"] = (
            series_first(df, ["municipio_nome", "MUNICIPIO", "NM_MUNICIPIO", "municipio"], "").astype(str).str.strip()
        )
        out["municipio_nome_normalizado"] = out["municipio_nome"].map(normalize_name)
        out["zona"] = series_first(df, ["zona", "ZONA", "NR_ZONA", "zona_eleitoral"], "").map(
            lambda value: pad_digits(value, 4)
        )
        out["secao"] = series_first(df, ["secao", "SECAO", "NR_SECAO", "secao_eleitoral"], "").map(
            lambda value: pad_digits(value, 4)
        )
        out["local_votacao"] = (
            series_first(df, ["local_votacao", "LOCAL_VOTACAO", "NM_LOCAL_VOTACAO"], "").astype(str).str.strip()
        )
        out["candidate_id"] = series_first(df, ["candidate_id", "ID_CANDIDATO", "SQ_CANDIDATO"], "").map(digits_only)
        out["numero_candidato"] = series_first(df, ["numero_candidato", "NR_CANDIDATO"], "").map(digits_only)
        out["partido"] = series_first(df, ["partido", "SG_PARTIDO"], "").astype(str).str.upper().str.strip()
        out["cd_setor"] = series_first(df, ["cd_setor", "CD_SETOR"], "").map(digits_only)
        out["_strategy_parts"] = [["base_resultados_secao"] for _ in range(len(out))]
        out["_confidence_parts"] = [[1.0] for _ in range(len(out))]
        out["_source_flags"] = [{"resultados_secao"} for _ in range(len(out))]
        return out

    def _enrich_section_profile(self, base: pd.DataFrame, df: pd.DataFrame | None) -> pd.DataFrame:
        if df is None or df.empty:
            return base
        profile = self._section_key_frame(df)
        profile["_has_eleitorado_secao"] = True
        merged = base.merge(profile.drop_duplicates(self._section_keys()), on=self._section_keys(), how="left")
        return self._mark_source(
            merged,
            flag_column="_has_eleitorado_secao",
            source_name="eleitorado_secao",
            strategy="exact_section_profile",
            confidence=1.0,
        )

    def _enrich_voting_place(self, base: pd.DataFrame, df: pd.DataFrame | None) -> pd.DataFrame:
        if df is None or df.empty:
            return base
        places = self._section_key_frame(df)
        places["local_votacao_match"] = (
            series_first(df, ["local_votacao", "LOCAL_VOTACAO", "NM_LOCAL_VOTACAO"], "").astype(str).str.strip()
        )
        places["_has_local_votacao"] = True
        merged = base.merge(places.drop_duplicates(self._section_keys()), on=self._section_keys(), how="left")
        missing_local = merged["local_votacao"].astype(str).str.len().eq(0)
        merged.loc[missing_local, "local_votacao"] = merged.loc[missing_local, "local_votacao_match"].fillna("")
        merged = merged.drop(columns=["local_votacao_match"])
        return self._mark_source(
            merged,
            flag_column="_has_local_votacao",
            source_name="locais_votacao",
            strategy="exact_voting_place",
            confidence=1.0,
        )

    def _enrich_candidates(self, base: pd.DataFrame, df: pd.DataFrame | None) -> pd.DataFrame:
        if df is None or df.empty or "candidate_id" not in base.columns:
            return base
        candidates = pd.DataFrame(index=df.index)
        candidates["ano_eleicao"] = pd.to_numeric(
            series_first(df, ["ano_eleicao", "ANO_ELEICAO", "ano"], 0), errors="coerce"
        ).astype("Int64")
        candidates["uf"] = series_first(df, ["uf", "SIGLA_UF", "SG_UF"], "").astype(str).str.upper().str.strip()
        candidates["candidate_id"] = series_first(df, ["candidate_id", "SQ_CANDIDATO", "ID_CANDIDATO"], "").map(
            digits_only
        )
        candidates["numero_candidato_candidate"] = series_first(df, ["numero_candidato", "NR_CANDIDATO"], "").map(
            digits_only
        )
        candidates["partido_candidate"] = (
            series_first(df, ["partido", "SG_PARTIDO"], "").astype(str).str.upper().str.strip()
        )
        candidates["_has_candidate"] = True
        merged = base.merge(
            candidates.drop_duplicates(["ano_eleicao", "uf", "candidate_id"]),
            on=["ano_eleicao", "uf", "candidate_id"],
            how="left",
        )
        missing_number = merged["numero_candidato"].astype(str).str.len().eq(0)
        merged.loc[missing_number, "numero_candidato"] = merged.loc[
            missing_number, "numero_candidato_candidate"
        ].fillna("")
        missing_party = merged["partido"].astype(str).str.len().eq(0)
        merged.loc[missing_party, "partido"] = merged.loc[missing_party, "partido_candidate"].fillna("")
        merged = merged.drop(columns=["numero_candidato_candidate", "partido_candidate"])
        return self._mark_source(
            merged,
            flag_column="_has_candidate",
            source_name="candidatos",
            strategy="exact_candidate",
            confidence=1.0,
        )

    def _enrich_finance_coverage(self, base: pd.DataFrame, df: pd.DataFrame | None) -> pd.DataFrame:
        if df is None or df.empty:
            return base
        finance = pd.DataFrame(index=df.index)
        finance["ano_eleicao"] = pd.to_numeric(
            series_first(df, ["ano_eleicao", "ANO_ELEICAO", "ano"], 0), errors="coerce"
        ).astype("Int64")
        finance["uf"] = series_first(df, ["uf", "SIGLA_UF", "SG_UF"], "").astype(str).str.upper().str.strip()
        finance["candidate_id"] = series_first(df, ["candidate_id", "SQ_CANDIDATO", "ID_CANDIDATO"], "").map(
            digits_only
        )
        finance["_has_finance"] = True
        merged = base.merge(
            finance.drop_duplicates(["ano_eleicao", "uf", "candidate_id"]),
            on=["ano_eleicao", "uf", "candidate_id"],
            how="left",
        )
        return self._mark_source(
            merged,
            flag_column="_has_finance",
            source_name="prestacao_contas",
            strategy="exact_finance_candidate",
            confidence=0.92,
        )

    def _enrich_municipality_crosswalk(self, base: pd.DataFrame, df: pd.DataFrame | None) -> pd.DataFrame:
        if df is None or df.empty:
            return base
        crosswalk = self._prepare_crosswalk(df)
        for index, row in base.iterrows():
            match = self._match_crosswalk(row, crosswalk)
            if match is None:
                self._append_strategy(base, index, "unmatched_tse_ibge", 0.0)
                continue
            if not str(base.at[index, "cod_municipio_ibge"] or ""):
                base.at[index, "cod_municipio_ibge"] = str(match["cod_municipio_ibge"])
            self._append_strategy(base, index, str(match["strategy"]), float(match["confidence"]))
            base.at[index, "_source_flags"].add("municipio_crosswalk")
        return base

    def _enrich_census_sector(self, base: pd.DataFrame, df: pd.DataFrame | None) -> pd.DataFrame:
        if df is None or df.empty:
            return base
        sectors = pd.DataFrame(index=df.index)
        sectors["cod_municipio_ibge"] = series_first(df, ["cod_municipio_ibge", "COD_MUN_IBGE"], "").map(
            lambda value: pad_digits(value, 7)
        )
        sectors["cd_setor"] = series_first(df, ["cd_setor", "CD_SETOR"], "").map(digits_only)
        sectors = sectors[sectors["cd_setor"].astype(str).str.len().gt(0)].drop_duplicates()
        by_mun = {
            municipio: group["cd_setor"].dropna().unique().tolist()
            for municipio, group in sectors.groupby("cod_municipio_ibge")
        }
        for index, row in base.iterrows():
            if str(row.get("cd_setor", "") or ""):
                self._append_strategy(base, index, "exact_sector", 1.0)
                base.at[index, "_source_flags"].add("setores_censitarios")
                continue
            candidates = by_mun.get(str(row.get("cod_municipio_ibge", "")), [])
            if len(candidates) == 1:
                base.at[index, "cd_setor"] = str(candidates[0])
                self._append_strategy(base, index, "approx_municipality_single_sector", 0.72)
                base.at[index, "_source_flags"].add("setores_censitarios")
            else:
                self._append_strategy(base, index, "no_sector_match", 0.0)
        return base

    def _finalize(self, base: pd.DataFrame) -> pd.DataFrame:
        out = base.copy()
        out["territorial_cluster_id"] = (
            out["uf"].astype(str)
            + ":"
            + out["cod_municipio_ibge"].replace("", pd.NA).fillna(out["cod_municipio_tse"]).astype(str)
            + ":Z"
            + out["zona"].astype(str)
        )
        out["join_strategy"] = out["_strategy_parts"].map(lambda values: ";".join(dict.fromkeys(values)))
        out["join_confidence"] = out["_confidence_parts"].map(self._confidence_score)
        out["source_coverage_score"] = out["_source_flags"].map(lambda flags: min(len(flags) / 6, 1.0))
        out["master_record_id"] = out.apply(self._record_id, axis=1)
        out = out.drop_duplicates("master_record_id").reset_index(drop=True)
        return out

    def _record_id(self, row: pd.Series) -> str:
        raw = "|".join(str(row.get(column, "")) for column in MASTER_INDEX_COLUMNS if column != "master_record_id")
        return str(uuid5(NAMESPACE_URL, f"territorial-master-index:{raw}"))

    def _confidence_score(self, values: list[float]) -> float:
        if not values:
            return 0.0
        numeric = [max(0.0, min(1.0, float(value))) for value in values]
        return round(sum(numeric) / len(numeric), 6)

    def _append_strategy(self, df: pd.DataFrame, index: Any, strategy: str, confidence: float) -> None:
        df.at[index, "_strategy_parts"].append(strategy)
        df.at[index, "_confidence_parts"].append(confidence)

    def _mark_source(
        self,
        df: pd.DataFrame,
        *,
        flag_column: str,
        source_name: str,
        strategy: str,
        confidence: float,
    ) -> pd.DataFrame:
        out = df.copy()
        flags = (
            out[flag_column].fillna(False).astype(bool)
            if flag_column in out.columns
            else pd.Series(False, index=out.index)
        )
        for index, matched in flags.items():
            if bool(matched):
                out.at[index, "_source_flags"].add(source_name)
                self._append_strategy(out, index, strategy, confidence)
        return out.drop(columns=[flag_column], errors="ignore")

    def _section_keys(self) -> list[str]:
        return ["ano_eleicao", "uf", "cod_municipio_tse", "zona", "secao"]

    def _section_key_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        out = pd.DataFrame(index=df.index)
        out["ano_eleicao"] = pd.to_numeric(
            series_first(df, ["ano_eleicao", "ANO_ELEICAO", "ano"], 0), errors="coerce"
        ).astype("Int64")
        out["uf"] = series_first(df, ["uf", "SIGLA_UF", "SG_UF"], "").astype(str).str.upper().str.strip()
        out["cod_municipio_tse"] = series_first(df, ["cod_municipio_tse", "COD_MUN_TSE", "CD_MUNICIPIO"], "").map(
            lambda value: pad_digits(value, 5)
        )
        out["zona"] = series_first(df, ["zona", "ZONA", "NR_ZONA", "zona_eleitoral"], "").map(
            lambda value: pad_digits(value, 4)
        )
        out["secao"] = series_first(df, ["secao", "SECAO", "NR_SECAO", "secao_eleitoral"], "").map(
            lambda value: pad_digits(value, 4)
        )
        return out

    def _prepare_crosswalk(self, df: pd.DataFrame) -> pd.DataFrame:
        out = pd.DataFrame(index=df.index)
        out["uf"] = series_first(df, ["uf", "SIGLA_UF", "SG_UF"], "").astype(str).str.upper().str.strip()
        out["cod_municipio_tse"] = series_first(df, ["cod_municipio_tse", "COD_MUN_TSE", "CD_MUNICIPIO"], "").map(
            lambda value: pad_digits(value, 5)
        )
        out["cod_municipio_ibge"] = series_first(df, ["cod_municipio_ibge", "COD_MUN_IBGE"], "").map(
            lambda value: pad_digits(value, 7)
        )
        out["municipio_nome_normalizado"] = series_first(df, ["municipio_nome", "MUNICIPIO", "NM_MUNICIPIO"], "").map(
            normalize_name
        )
        return out.drop_duplicates()

    def _match_crosswalk(self, row: pd.Series, crosswalk: pd.DataFrame) -> dict[str, str | float] | None:
        uf = str(row.get("uf", ""))
        cod_ibge = str(row.get("cod_municipio_ibge", ""))
        cod_tse = str(row.get("cod_municipio_tse", ""))
        name = str(row.get("municipio_nome_normalizado", ""))
        if cod_ibge:
            exact_ibge = crosswalk[(crosswalk["uf"].eq(uf)) & (crosswalk["cod_municipio_ibge"].eq(cod_ibge))]
            if not exact_ibge.empty:
                return {"cod_municipio_ibge": cod_ibge, "strategy": "exact_ibge_code", "confidence": 1.0}
        if cod_tse:
            exact_tse = crosswalk[(crosswalk["uf"].eq(uf)) & (crosswalk["cod_municipio_tse"].eq(cod_tse))]
            if len(exact_tse) == 1:
                return {
                    "cod_municipio_ibge": str(exact_tse.iloc[0]["cod_municipio_ibge"]),
                    "strategy": "exact_tse_ibge_code",
                    "confidence": 0.98,
                }
        name_matches = crosswalk[(crosswalk["uf"].eq(uf)) & (crosswalk["municipio_nome_normalizado"].eq(name))]
        if len(name_matches) == 1:
            return {
                "cod_municipio_ibge": str(name_matches.iloc[0]["cod_municipio_ibge"]),
                "strategy": "normalized_name_unique",
                "confidence": 0.86,
            }
        fuzzy = self._best_fuzzy(
            name, crosswalk[crosswalk["uf"].eq(uf)]["municipio_nome_normalizado"].dropna().tolist()
        )
        if fuzzy is not None and fuzzy[1] >= 0.92:
            fuzzy_matches = crosswalk[(crosswalk["uf"].eq(uf)) & (crosswalk["municipio_nome_normalizado"].eq(fuzzy[0]))]
            if len(fuzzy_matches) == 1:
                return {
                    "cod_municipio_ibge": str(fuzzy_matches.iloc[0]["cod_municipio_ibge"]),
                    "strategy": "fuzzy_name",
                    "confidence": round(float(fuzzy[1]) * 0.78, 6),
                }
        return None

    def _best_fuzzy(self, name: str, candidates: list[str]) -> tuple[str, float] | None:
        if not name or not candidates:
            return None
        return max(
            ((candidate, SequenceMatcher(None, name, candidate).ratio()) for candidate in candidates),
            key=lambda item: item[1],
        )

    def _write_duckdb(self, master: pd.DataFrame, output_dir: Path, dataset_version: str) -> Path | None:
        try:
            import duckdb
        except ImportError:
            return None
        duckdb_path = output_dir / f"territorial_electoral_master_index_{dataset_version}.duckdb"
        with duckdb.connect(str(duckdb_path)) as con:
            con.register("_master_index", master)
            con.execute("CREATE OR REPLACE TABLE territorial_electoral_master_index AS SELECT * FROM _master_index")
        return duckdb_path


class MasterIndexPipeline:
    def __init__(self, builder: MasterIndexBuilder | None = None):
        self.builder = builder or MasterIndexBuilder()

    def run_from_paths(
        self,
        *,
        resultados_secao_path: Path,
        output_dir: Path,
        dataset_version: str,
        eleitorado_secao_path: Path | None = None,
        locais_votacao_path: Path | None = None,
        candidatos_path: Path | None = None,
        prestacao_contas_path: Path | None = None,
        setores_censitarios_path: Path | None = None,
        municipio_crosswalk_path: Path | None = None,
    ) -> MasterIndexBuildResult:
        master = self.builder.build(
            resultados_secao=_read_table(resultados_secao_path),
            eleitorado_secao=_read_table(eleitorado_secao_path) if eleitorado_secao_path else None,
            locais_votacao=_read_table(locais_votacao_path) if locais_votacao_path else None,
            candidatos=_read_table(candidatos_path) if candidatos_path else None,
            prestacao_contas=_read_table(prestacao_contas_path) if prestacao_contas_path else None,
            setores_censitarios=_read_table(setores_censitarios_path) if setores_censitarios_path else None,
            municipio_crosswalk=_read_table(municipio_crosswalk_path) if municipio_crosswalk_path else None,
        )
        return self.builder.write_gold(master, output_dir, dataset_version=dataset_version)
