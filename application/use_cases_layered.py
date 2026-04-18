import logging

import pandas as pd

from application.interfaces import AIService, AnalyticsRepository, ReportStore
from application.rag_service import responde
from config.settings import get_settings
from domain.contracts import validate_alocacao_output, validate_municipios_input
from domain.errors import AppOperationalError, ErrorCode, ErrorDetail
from domain.scoring import CARGOS_EST, PESOS_CLUSTER, TETOS, calcular_alocacao
from infrastructure.allocation_strategy import load_allocation_strategy

logger = logging.getLogger(__name__)


def executar_alocacao(
    repo: AnalyticsRepository,
    report_store: ReportStore,
    df_mun: pd.DataFrame,
    budget: int,
    cargo: str,
    n: int,
    split_d: float,
) -> pd.DataFrame:
    try:
        df_mun = validate_municipios_input(df_mun)
        try:
            strategy = load_allocation_strategy(get_settings().build_paths())
            pesos_cluster = strategy.cluster_weights
            tetos = strategy.office_caps
            cargos_est = strategy.statewide_offices
            channel_weights = strategy.channel_weights
        except Exception as strategy_error:
            logger.warning("Falha ao carregar estrategia de alocacao; usando constantes padrao: %s", strategy_error)
            pesos_cluster = PESOS_CLUSTER
            tetos = TETOS
            cargos_est = CARGOS_EST
            channel_weights = None
        df_r = calcular_alocacao(
            df_mun=df_mun,
            budget=budget,
            cargo=cargo,
            n=n,
            split_d=split_d,
            pesos_cluster=pesos_cluster,
            tetos=tetos,
            cargos_est=cargos_est,
            channel_weights=channel_weights,
        )
        df_r = validate_alocacao_output(df_r)
    except Exception as e:
        raise AppOperationalError(
            ErrorDetail(
                code=ErrorCode.ALLOCATION_CONTRACT_VIOLATION,
                message="Falha de contrato ou regra ao calcular alocacao.",
                operation="executar_alocacao",
            )
        ) from e
    try:
        report_store.save_report(df_r, "ultima_alocacao.parquet")
    except Exception as e:
        logger.warning("Falha ao persistir relatorio de alocacao: %s", e)
    try:
        repo.register_table("alocacao", df_r)
    except Exception as e:
        raise AppOperationalError(
            ErrorDetail(
                code=ErrorCode.ALLOCATION_EXECUTION_FAILED,
                message="Nao foi possivel registrar alocacao no repositorio.",
                operation="executar_alocacao",
            )
        ) from e
    return df_r


def responder_pergunta(
    repo: AnalyticsRepository,
    ai_service: AIService,
    pergunta: str,
    historico: list[dict],
) -> tuple[str, str, int]:
    return responde(db=repo, col=ai_service, pergunta=pergunta, historico=historico)
