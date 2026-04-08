from typing import Literal

import pandas as pd
from pydantic import BaseModel, Field, ValidationError, field_validator

from domain.constants import ALOC_COLS


class DataContractError(ValueError):
    pass


class MunicipioInputRow(BaseModel):
    ranking_final: int = Field(ge=1)
    municipio: str = Field(min_length=1)
    cluster: str = Field(min_length=1)
    indice_final: float = Field(ge=0)
    PD_qt: float | None = Field(default=None, ge=0, le=100)
    pop_censo2022: float | None = Field(default=None, ge=0)

    @field_validator("cluster")
    @classmethod
    def validate_cluster(cls, value: str) -> str:
        allowed = {"Diamante", "Alavanca", "Consolidacao", "Consolidação", "ConsolidaÃ§Ã£o", "Descarte"}
        if value not in allowed:
            raise ValueError("cluster invalido para contrato de entrada")
        return value


class AlocacaoOutputRow(BaseModel):
    municipio: str = Field(min_length=1)
    cluster: str = Field(min_length=1)
    ranking: int = Field(ge=1)
    indice: float = Field(ge=0)
    PD_qt: float = Field(ge=0, le=100)
    pop: int = Field(ge=0)
    budget: float = Field(ge=0)
    digital: float = Field(ge=0)
    offline: float = Field(ge=0)
    meta_fb_ig: float = Field(ge=0)
    youtube: float = Field(ge=0)
    tiktok: float = Field(ge=0)
    whatsapp: float = Field(ge=0)
    google_ads: float = Field(ge=0)
    evento_presencial: float = Field(ge=0)
    radio_local: float = Field(ge=0)
    impresso: float = Field(ge=0)


def _assert_columns(df: pd.DataFrame, required: list[str], context: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise DataContractError(f"{context}: colunas obrigatorias ausentes: {missing}")


def _collect_validation_errors(rows: list[dict], model: type[BaseModel], context: str, max_errors: int = 20) -> None:
    errors: list[str] = []
    for idx, row in enumerate(rows, start=1):
        try:
            model.model_validate(row)
        except ValidationError as exc:
            errors.append(f"linha {idx}: {exc.errors()[0]['msg']}")
            if len(errors) >= max_errors:
                break
    if errors:
        raise DataContractError(f"{context}: " + "; ".join(errors))


def validate_municipios_input(df: pd.DataFrame) -> pd.DataFrame:
    required = ["ranking_final", "municipio", "cluster", "indice_final"]
    _assert_columns(df, required, "contrato de entrada municipios")
    _collect_validation_errors(df.to_dict("records"), MunicipioInputRow, "contrato de entrada municipios")
    return df


def validate_alocacao_output(df: pd.DataFrame) -> pd.DataFrame:
    _assert_columns(df, ALOC_COLS, "contrato de saida alocacao")
    _collect_validation_errors(df[ALOC_COLS].to_dict("records"), AlocacaoOutputRow, "contrato de saida alocacao")
    return df
