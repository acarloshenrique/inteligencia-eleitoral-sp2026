from __future__ import annotations

import logging

from application.input_safety import sanitize_user_prompt
from application.intent_router import resolve_intent_query
from application.interfaces import AIService, AnalyticsRepository
from domain.errors import AppOperationalError, ErrorCode, ErrorDetail
from infrastructure.privacy import redact_text

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """Voce e analista senior de inteligencia eleitoral SP 2026.
644 municipios paulistas ranqueados por: Territorial 35% + VS 25% + ISE 20% + PD 20%.
Clusters: Diamante (territorial>70 e VS>70) -> maximo investimento | Alavanca -> potencial latente | Consolidacao -> manutencao | Descarte -> minimo.
Responda em portugues, seja preciso, cite dados do contexto. Nao invente valores."""


def _sql_contextual(db: AnalyticsRepository, pergunta: str) -> str:
    return resolve_intent_query(db, pergunta).sql


def responde(
    db: AnalyticsRepository,
    col: AIService,
    pergunta: str,
    historico: list[dict],
    *,
    system_prompt: str = SYSTEM_PROMPT,
) -> tuple[str, str, int]:
    sanitized = sanitize_user_prompt(pergunta)
    pergunta_segura = sanitized.text
    if not pergunta_segura:
        pergunta_segura = "Mostre o ranking territorial principal."
    if sanitized.injection_flag:
        logger.warning("Possivel prompt injection detectado no chat; original_length=%s", sanitized.original_length)

    sem_txt = ""
    try:
        sem_txt = col.search_relevant(pergunta_segura, n_results=5)
    except Exception as e:
        logger.warning("Busca semantica indisponivel nesta consulta: %s", redact_text(e))

    est = ""
    sql = _sql_contextual(db, pergunta_segura)
    try:
        est_df = db.query_df(sql)
        if not est_df.empty:
            est = est_df.to_string(index=False)
    except Exception as e:
        logger.warning("Falha ao executar SQL contextual do chat: %s", redact_text(e))
        raise AppOperationalError(
            ErrorDetail(
                code=ErrorCode.CHAT_QUERY_FAILED,
                message="Consulta contextual do chat falhou no repositorio.",
                operation="responde.query_df",
            )
        ) from e

    safety_note = ""
    if sanitized.truncated:
        safety_note += "\nObservacao: pergunta truncada para limite operacional."
    if sanitized.injection_flag:
        safety_note += "\nObservacao: entrada marcada para auditoria por padrao adversarial."
    ctx = f"Municipios relevantes: {sem_txt}\n\nDados:\n{est}{safety_note}"
    try:
        resposta, total_tokens = col.complete(system_prompt, historico, pergunta_segura, ctx)
        return resposta, sem_txt, total_tokens
    except Exception as e:
        logger.error("Falha na geracao da resposta do chat: %s", redact_text(e))
        raise AppOperationalError(
            ErrorDetail(
                code=ErrorCode.CHAT_LLM_FAILED,
                message="Nao foi possivel gerar resposta do assistente no momento.",
                operation="responde.complete",
            )
        ) from e
