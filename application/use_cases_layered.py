import logging

from application.input_safety import sanitize_user_prompt
from application.intent_router import resolve_intent_query
from application.interfaces import AIService, AnalyticsRepository, ReportStore
from domain.allocation import calcular_alocacao
from domain.constants import CARGOS_EST, PESOS_CLUSTER, SYSTEM_PROMPT, TETOS
from domain.contracts import validate_alocacao_output, validate_municipios_input
from domain.errors import AppOperationalError, ErrorCode, ErrorDetail

logger = logging.getLogger(__name__)


def executar_alocacao(
    repo: AnalyticsRepository,
    report_store: ReportStore,
    df_mun,
    budget: int,
    cargo: str,
    n: int,
    split_d: float,
):
    try:
        df_mun = validate_municipios_input(df_mun)
        df_r = calcular_alocacao(
            df_mun=df_mun,
            budget=budget,
            cargo=cargo,
            n=n,
            split_d=split_d,
            pesos_cluster=PESOS_CLUSTER,
            tetos=TETOS,
            cargos_est=CARGOS_EST,
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


def _sql_contextual(repo: AnalyticsRepository, pergunta: str) -> str:
    return resolve_intent_query(repo, pergunta).sql


def responder_pergunta(
    repo: AnalyticsRepository,
    ai_service: AIService,
    pergunta: str,
    historico: list[dict],
):
    sanitized = sanitize_user_prompt(pergunta)
    pergunta_segura = sanitized.text
    if not pergunta_segura:
        pergunta_segura = "Mostre o ranking territorial principal."
    if sanitized.injection_flag:
        logger.warning("Possivel prompt injection detectado no chat; original_length=%s", sanitized.original_length)

    sem_txt = ""
    try:
        sem_txt = ai_service.search_relevant(pergunta_segura, n_results=5)
    except Exception as e:
        logger.warning("Busca semantica indisponivel nesta consulta: %s", e)

    est = ""
    sql = _sql_contextual(repo, pergunta_segura)
    try:
        est_df = repo.query_df(sql)
        if not est_df.empty:
            est = est_df.to_string(index=False)
    except Exception as e:
        logger.warning("Falha ao executar SQL contextual do chat: %s", e)
        raise AppOperationalError(
            ErrorDetail(
                code=ErrorCode.CHAT_QUERY_FAILED,
                message="Consulta contextual do chat falhou no repositorio.",
                operation="responder_pergunta.query_df",
            )
        ) from e

    safety_note = ""
    if sanitized.truncated:
        safety_note += "\nObservacao: pergunta truncada para limite operacional."
    if sanitized.injection_flag:
        safety_note += "\nObservacao: entrada marcada para auditoria por padrao adversarial."
    ctx = f"Municipios relevantes: {sem_txt}\n\nDados:\n{est}{safety_note}"
    try:
        resposta, total_tokens = ai_service.complete(SYSTEM_PROMPT, historico, pergunta_segura, ctx)
        return resposta, sem_txt, total_tokens
    except Exception as e:
        logger.error("Falha na geracao da resposta do chat: %s", e)
        raise AppOperationalError(
            ErrorDetail(
                code=ErrorCode.CHAT_LLM_FAILED,
                message="Nao foi possivel gerar resposta do assistente no momento.",
                operation="responder_pergunta.complete",
            )
        ) from e
