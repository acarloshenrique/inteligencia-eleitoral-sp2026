from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from ingestion.domain_dictionaries import (
    CATEGORY_NORMALIZATION,
    COLUMN_ALIASES,
    DATE_COLUMNS,
    ELECTORAL_CATEGORY_COLUMNS,
    MASTER_KEY_COLUMNS,
    MONEY_COLUMNS,
)
from ingestion.silver_contracts import SilverSchemaContract, contract_for


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def fix_mojibake(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    if not any(marker in value for marker in ("Ã", "Â", "â", "ð")):
        return value
    try:
        return value.encode("latin1").decode("utf-8")
    except UnicodeError:
        return value


def normalize_column_name(value: Any) -> str:
    text = fix_mojibake(str(value or ""))
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_") or "unknown"


def normalize_territory_name(value: Any) -> str:
    text = fix_mojibake(value)
    text = str(text or "").strip().upper()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def digits_only(value: Any) -> str:
    text = "" if pd.isna(value) else str(value)
    return "".join(char for char in text if char.isdigit())


def pad_digits(value: Any, width: int) -> str:
    digits = digits_only(value)
    return digits.zfill(width) if digits else ""


def normalize_money(value: Any) -> float | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("R$", "").replace(" ", "")
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def normalize_date(value: Any) -> str | None:
    if pd.isna(value) or str(value).strip() == "":
        return None
    parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


class MunicipalCrosswalk(BaseModel):
    model_config = ConfigDict(extra="forbid")

    records: list[dict[str, str]] = Field(default_factory=list)

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> MunicipalCrosswalk:
        normalized = SilverNormalizer().normalize_columns(df)
        required = {"uf", "cod_municipio_tse", "cod_municipio_ibge", "municipio_nome"}
        missing = required - set(normalized.columns)
        if missing:
            raise ValueError(f"crosswalk missing columns: {sorted(missing)}")
        records = []
        for row in normalized.to_dict(orient="records"):
            records.append(
                {
                    "uf": str(row.get("uf", "")).upper().strip(),
                    "cod_municipio_tse": digits_only(row.get("cod_municipio_tse")),
                    "cod_municipio_ibge": digits_only(row.get("cod_municipio_ibge")),
                    "municipio_nome_normalizado": normalize_territory_name(row.get("municipio_nome")),
                }
            )
        return cls(records=records)

    def enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if "join_confidence" not in out.columns:
            out["join_confidence"] = 0.0
        if "cod_municipio_ibge" not in out.columns:
            out["cod_municipio_ibge"] = ""
        for index, row in out.iterrows():
            match = self._match(row)
            if match is None:
                continue
            if not str(row.get("cod_municipio_ibge", "") or "").strip():
                out.at[index, "cod_municipio_ibge"] = match["cod_municipio_ibge"]
            out.at[index, "join_confidence"] = max(float(out.at[index, "join_confidence"]), float(match["confidence"]))
        return out

    def _match(self, row: pd.Series) -> dict[str, str | float] | None:
        uf = str(row.get("uf", "")).upper().strip()
        cod_tse = digits_only(row.get("cod_municipio_tse"))
        cod_ibge = digits_only(row.get("cod_municipio_ibge"))
        municipio = normalize_territory_name(row.get("municipio_nome"))
        if cod_ibge:
            for record in self.records:
                if record["uf"] == uf and record["cod_municipio_ibge"] == cod_ibge:
                    return {**record, "confidence": 1.0}
        if cod_tse:
            for record in self.records:
                if record["uf"] == uf and record["cod_municipio_tse"] == cod_tse:
                    return {**record, "confidence": 0.98}
        if municipio:
            matches = [
                record
                for record in self.records
                if record["uf"] == uf and record["municipio_nome_normalizado"] == municipio
            ]
            if len(matches) == 1:
                return {**matches[0], "confidence": 0.86}
        return None


class SilverQualityReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_id: str
    rows_input: int
    rows_output: int
    duplicates_removed: int
    missing_required_counts: dict[str, int]
    missing_required_columns: list[str] = Field(default_factory=list)
    schema_errors: list[str] = Field(default_factory=list)
    join_confidence_avg: float | None = None
    status: str
    generated_at_utc: str = Field(default_factory=utc_now_iso)

    def write(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.model_dump(mode="json"), ensure_ascii=False, indent=2), encoding="utf-8")
        return path


@dataclass(frozen=True)
class SilverTransformResult:
    dataframe: pd.DataFrame
    quality: SilverQualityReport


class SilverNormalizer:
    def normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out.columns = [normalize_column_name(column) for column in out.columns]
        for column in out.select_dtypes(include=["object", "string"]).columns:
            out[column] = out[column].map(fix_mojibake)
        for canonical, aliases in COLUMN_ALIASES.items():
            if canonical in out.columns:
                continue
            found = next((alias for alias in aliases if normalize_column_name(alias) in out.columns), None)
            if found is not None:
                out[canonical] = out[normalize_column_name(found)]
        return out

    def normalize_master_keys(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if "uf" in out.columns:
            out["uf"] = out["uf"].astype("string").str.upper().str.strip()
        if "municipio_nome" in out.columns:
            out["municipio_nome"] = out["municipio_nome"].map(fix_mojibake).astype("string").str.strip()
            out["municipio_nome_normalizado"] = out["municipio_nome"].map(normalize_territory_name)
        if "cod_municipio_tse" in out.columns:
            out["cod_municipio_tse"] = out["cod_municipio_tse"].map(lambda value: pad_digits(value, 5)).astype("string")
        if "cod_municipio_ibge" in out.columns:
            out["cod_municipio_ibge"] = (
                out["cod_municipio_ibge"].map(lambda value: pad_digits(value, 7)).astype("string")
            )
        if "zona" in out.columns:
            out["zona"] = out["zona"].map(lambda value: pad_digits(value, 4)).astype("string")
        if "secao" in out.columns:
            out["secao"] = out["secao"].map(lambda value: pad_digits(value, 4)).astype("string")
        if "candidate_id" in out.columns:
            out["candidate_id"] = out["candidate_id"].map(digits_only).astype("string")
        if "cpf_candidato" in out.columns:
            out["cpf_candidato"] = out["cpf_candidato"].map(lambda value: pad_digits(value, 11)).astype("string")
        if "numero_candidato" in out.columns:
            out["numero_candidato"] = out["numero_candidato"].map(digits_only).astype("string")
        if "cd_setor" in out.columns:
            out["cd_setor"] = out["cd_setor"].map(digits_only).astype("string")
        return out

    def normalize_types(self, df: pd.DataFrame, contract: SilverSchemaContract) -> pd.DataFrame:
        out = df.copy()
        if "ano_eleicao" in out.columns:
            out["ano_eleicao"] = pd.to_numeric(out["ano_eleicao"], errors="coerce").astype("Int64")
        for column in DATE_COLUMNS:
            if column in out.columns:
                out[column] = out[column].map(normalize_date).astype("string")
        for column in MONEY_COLUMNS:
            if column in out.columns:
                out[column] = out[column].map(normalize_money).astype("Float64")
        for column in ELECTORAL_CATEGORY_COLUMNS:
            if column in out.columns:
                normalized = out[column].map(normalize_territory_name).astype("string")
                mapping = CATEGORY_NORMALIZATION.get(column, {})
                out[column] = normalized.map(lambda value, mapping=mapping: mapping.get(str(value), str(value)))
        for column, dtype in contract.strong_types.items():
            if column in out.columns and dtype not in {"Int64", "Float64"}:
                out[column] = out[column].astype(dtype)
        return out


class SilverSchemaValidator:
    def validate(self, df: pd.DataFrame, contract: SilverSchemaContract) -> tuple[dict[str, int], list[str], list[str]]:
        missing_columns = [column for column in contract.required_columns if column not in df.columns]
        missing_counts = {
            column: int(df[column].isna().sum() + (df[column].astype("string").str.strip() == "").sum())
            for column in contract.required_columns
            if column in df.columns
        }
        errors: list[str] = []
        if missing_columns:
            errors.append(f"missing required columns: {', '.join(missing_columns)}")
        if df.empty:
            errors.append("empty dataset")
        return missing_counts, missing_columns, errors


class BaseSilverTransformer:
    def __init__(
        self,
        *,
        contract: SilverSchemaContract,
        crosswalk: MunicipalCrosswalk | None = None,
        normalizer: SilverNormalizer | None = None,
        validator: SilverSchemaValidator | None = None,
    ):
        self.contract = contract
        self.crosswalk = crosswalk
        self.normalizer = normalizer or SilverNormalizer()
        self.validator = validator or SilverSchemaValidator()

    def transform(
        self,
        df: pd.DataFrame,
        *,
        source_dataset: str,
        source_file: str,
        ingestion_timestamp: str,
    ) -> SilverTransformResult:
        rows_input = int(len(df))
        out = self.normalizer.normalize_columns(df)
        out = self.normalizer.normalize_master_keys(out)
        out = self.normalizer.normalize_types(out, self.contract)
        if self.crosswalk is not None and {"uf", "municipio_nome"}.intersection(out.columns):
            out = self.crosswalk.enrich(out)
        elif "join_confidence" not in out.columns:
            out["join_confidence"] = 1.0 if "cod_municipio_ibge" in out.columns else 0.0
        out = self._add_audit_columns(
            out,
            source_dataset=source_dataset,
            source_file=source_file,
            ingestion_timestamp=ingestion_timestamp,
        )
        out = self._select_stable_order(out)
        before_dedup = len(out)
        out = self._deduplicate(out)
        duplicates_removed = before_dedup - len(out)
        missing_counts, missing_columns, errors = self.validator.validate(out, self.contract)
        quality = SilverQualityReport(
            dataset_id=self.contract.dataset_id,
            rows_input=rows_input,
            rows_output=int(len(out)),
            duplicates_removed=int(duplicates_removed),
            missing_required_counts=missing_counts,
            missing_required_columns=missing_columns,
            schema_errors=errors,
            join_confidence_avg=self._join_confidence(out),
            status="ok" if not errors else "failed",
        )
        return SilverTransformResult(dataframe=out, quality=quality)

    def _add_audit_columns(
        self,
        df: pd.DataFrame,
        *,
        source_dataset: str,
        source_file: str,
        ingestion_timestamp: str,
    ) -> pd.DataFrame:
        out = df.copy()
        out["source_dataset"] = source_dataset
        out["source_file"] = source_file
        out["ingestion_timestamp"] = ingestion_timestamp
        out["transform_timestamp"] = utc_now_iso()
        return out

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        keys = [column for column in self.contract.primary_key if column in df.columns]
        if not keys:
            return df.drop_duplicates().reset_index(drop=True)
        return df.drop_duplicates(keys).reset_index(drop=True)

    def _select_stable_order(self, df: pd.DataFrame) -> pd.DataFrame:
        first = [column for column in MASTER_KEY_COLUMNS if column in df.columns]
        audit = ["source_dataset", "source_file", "ingestion_timestamp", "transform_timestamp", "join_confidence"]
        audit_present = [column for column in audit if column in df.columns]
        remaining = [column for column in df.columns if column not in set(first + audit_present)]
        return df[first + remaining + audit_present]

    def _join_confidence(self, df: pd.DataFrame) -> float | None:
        if "join_confidence" not in df.columns or df.empty:
            return None
        values = pd.to_numeric(df["join_confidence"], errors="coerce")
        if values.dropna().empty:
            return None
        return round(float(values.mean()), 6)


class SilverDatasetTransformer(BaseSilverTransformer):
    @classmethod
    def for_dataset(cls, dataset_id: str, *, crosswalk: MunicipalCrosswalk | None = None) -> SilverDatasetTransformer:
        return cls(contract=contract_for(dataset_id), crosswalk=crosswalk)


class SilverDatasetWriter:
    def write(
        self,
        result: SilverTransformResult,
        *,
        destination_dir: Path,
        dataset_id: str,
        report_name: str = "quality_report.json",
    ) -> tuple[Path, Path]:
        destination_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = destination_dir / f"{dataset_id}.parquet"
        quality_path = destination_dir / report_name
        result.dataframe.to_parquet(parquet_path, index=False)
        result.quality.write(quality_path)
        return parquet_path, quality_path
