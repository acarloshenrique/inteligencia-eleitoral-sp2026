from __future__ import annotations

import logging

import pandas as pd

from application.interfaces import DataStore, ReportStore
from domain.scoring_zona import ZoneAllocationConfig, score_zone_allocation

logger = logging.getLogger(__name__)


def executar_alocacao_zona(
    repo: DataStore,
    report_store: ReportStore | None,
    df_mun: pd.DataFrame,
    budget_total: int | float,
    config: ZoneAllocationConfig | None = None,
) -> pd.DataFrame:
    if not repo.table_exists("fact_zona_eleitoral"):
        return pd.DataFrame()
    zonas = repo.query_df("SELECT * FROM fact_zona_eleitoral")
    resultado = score_zone_allocation(zonas, df_mun, budget_total=budget_total, config=config)
    if report_store is not None and not resultado.empty:
        report_store.save_report(resultado, "ultima_alocacao_zona.parquet")
    try:
        repo.register_table("mart_alocacao_zona_eleitoral", resultado)
    except Exception as exc:
        logger.warning("Falha ao registrar mart_alocacao_zona_eleitoral: %s", exc)
    return resultado
