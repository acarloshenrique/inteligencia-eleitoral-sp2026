from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from application.input_safety import normalize_for_matching
from infrastructure.sql_safety import get_allowed_chat_query_template, is_allowed_chat_query_template


class TableAwareRepository(Protocol):
    def table_exists(self, table: str) -> bool: ...


class ChatIntent(StrEnum):
    ALLOCATION = "allocation"
    SECTIONS = "sections"
    FIELD_MAP = "field_map"
    CLUSTERS = "clusters"
    STATS = "stats"
    PRODUCT_RECOMMENDATION = "product_recommendation"
    RANKING = "ranking"


@dataclass(frozen=True)
class IntentQuery:
    intent: ChatIntent
    template_id: str
    sql: str
    required_table: str


_INTENT_KEYWORDS: tuple[tuple[ChatIntent, tuple[str, ...]], ...] = (
    (ChatIntent.PRODUCT_RECOMMENDATION, ("recomendacao", "canal ideal", "mensagem ideal", "verba sugerida", "roi politico")),
    (ChatIntent.ALLOCATION, ("budget", "alocar", "alocacao", "investir", "verba")),
    (ChatIntent.SECTIONS, ("secao", "secoes", "campo", "zona eleitoral")),
    (ChatIntent.FIELD_MAP, ("mapa", "custo", "mobilizacao")),
    (ChatIntent.CLUSTERS, ("cluster", "diamante", "alavanca", "consolidacao", "descarte")),
    (ChatIntent.STATS, ("media", "total", "estatistica", "sumario")),
)

_INTENT_SPECS: dict[ChatIntent, tuple[str, str]] = {
    ChatIntent.PRODUCT_RECOMMENDATION: ("chat.product_recommendation", "mart_recomendacao_alocacao"),
    ChatIntent.ALLOCATION: ("chat.allocation", "alocacao"),
    ChatIntent.SECTIONS: ("chat.sections", "secoes"),
    ChatIntent.FIELD_MAP: ("chat.field_map", "mapa_tatico"),
    ChatIntent.CLUSTERS: ("chat.clusters", "municipios"),
    ChatIntent.STATS: ("chat.stats", "municipios"),
    ChatIntent.RANKING: ("chat.ranking", "municipios"),
}


def _build_intent_query(intent: ChatIntent) -> IntentQuery:
    template_id, required_table = _INTENT_SPECS[intent]
    sql = get_allowed_chat_query_template(template_id)
    if not is_allowed_chat_query_template(template_id, sql):
        raise ValueError(f"template de query nao permitido para intent {intent}")
    return IntentQuery(intent=intent, template_id=template_id, sql=sql, required_table=required_table)


def classify_intent(question: str) -> ChatIntent:
    q = normalize_for_matching(question)
    for intent, keywords in _INTENT_KEYWORDS:
        if any(keyword in q for keyword in keywords):
            return intent
    return ChatIntent.RANKING


def resolve_intent_query(repo: TableAwareRepository, question: str) -> IntentQuery:
    preferred = classify_intent(question)
    candidate = _build_intent_query(preferred)
    if repo.table_exists(candidate.required_table):
        return candidate
    return _build_intent_query(ChatIntent.RANKING)
