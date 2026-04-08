from __future__ import annotations

from typing import Any

import pandas as pd
from pydantic import BaseModel, ValidationError

from domain.source_contracts import (
    BaseEleitoralRow,
    FiscalRow,
    IbgeSocioRow,
    MappingTseIbgeRow,
    SeadeRow,
    SecaoResultadoRow,
)


class SourceContractError(ValueError):
    pass


def _validate_rows(df: pd.DataFrame, model: type[BaseModel], source_name: str, max_errors: int = 20) -> None:
    if df.empty:
        return
    errors: list[str] = []
    for idx, row in enumerate(df.to_dict("records"), start=1):
        try:
            model.model_validate(row)
            for key in ("codigo_tse", "codigo_ibge", "nome_municipio", "municipio"):
                if key in row and str(row.get(key, "")).strip() == "":
                    raise SourceContractError(f"{source_name} linha {idx}: campo '{key}' vazio")
        except ValidationError as exc:
            errors.append(f"{source_name} linha {idx}: {exc.errors()[0]['msg']}")
            if len(errors) >= max_errors:
                break
        except SourceContractError as exc:
            errors.append(str(exc))
            if len(errors) >= max_errors:
                break
    if errors:
        raise SourceContractError("; ".join(errors))


def validate_input_contracts(
    *,
    base_df: pd.DataFrame,
    mapping_df: pd.DataFrame,
    socio_df: pd.DataFrame,
    secao_df: pd.DataFrame,
    ibge_df: pd.DataFrame,
    seade_df: pd.DataFrame,
    fiscal_df: pd.DataFrame,
) -> dict[str, Any]:
    _validate_rows(base_df, BaseEleitoralRow, "base_eleitoral")
    _validate_rows(mapping_df, MappingTseIbgeRow, "mapping_tse_ibge")
    _validate_rows(socio_df, IbgeSocioRow, "contexto_socio")
    _validate_rows(secao_df, SecaoResultadoRow, "resultado_secao")
    _validate_rows(ibge_df, IbgeSocioRow, "ibge_indicadores")
    _validate_rows(seade_df, SeadeRow, "seade_indicadores")
    _validate_rows(fiscal_df, FiscalRow, "transparencia_fiscal")
    return {
        "validated_sources": {
            "base_eleitoral": int(len(base_df)),
            "mapping_tse_ibge": int(len(mapping_df)),
            "contexto_socio": int(len(socio_df)),
            "resultado_secao": int(len(secao_df)),
            "ibge_indicadores": int(len(ibge_df)),
            "seade_indicadores": int(len(seade_df)),
            "transparencia_fiscal": int(len(fiscal_df)),
        }
    }
