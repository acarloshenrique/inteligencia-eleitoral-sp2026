from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol


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
    sql: str
    required_table: str


_INTENT_KEYWORDS: tuple[tuple[ChatIntent, tuple[str, ...]], ...] = (
    (ChatIntent.PRODUCT_RECOMMENDATION, ("recomendacao", "recomenda??o", "canal ideal", "mensagem ideal", "verba sugerida", "roi politico", "roi pol?tico")),
    (ChatIntent.ALLOCATION, ("budget", "alocar", "alocacao", "aloca??o", "investir", "verba")),
    (ChatIntent.SECTIONS, ("secao", "se??o", "secoes", "se??es", "campo", "zona eleitoral")),
    (ChatIntent.FIELD_MAP, ("mapa", "custo", "mobilizacao", "mobiliza??o")),
    (ChatIntent.CLUSTERS, ("cluster", "diamante", "alavanca", "consolidacao", "consolida??o", "descarte")),
    (ChatIntent.STATS, ("media", "m?dia", "total", "estatistica", "estat?stica", "sumario", "sum?rio")),
)

_QUERY_TEMPLATES: dict[ChatIntent, IntentQuery] = {
    ChatIntent.PRODUCT_RECOMMENDATION: IntentQuery(
        intent=ChatIntent.PRODUCT_RECOMMENDATION,
        required_table="mart_recomendacao_alocacao",
        sql=(
            "SELECT ranking, municipio_id_ibge7, ROUND(verba_sugerida,0) AS verba_sugerida, "
            "canal_ideal, mensagem_ideal, justificativa "
            "FROM mart_recomendacao_alocacao ORDER BY ranking LIMIT 15"
        ),
    ),
    ChatIntent.ALLOCATION: IntentQuery(
        intent=ChatIntent.ALLOCATION,
        required_table="alocacao",
        sql=(
            "SELECT municipio, cluster, ROUND(budget,0) AS budget, ROUND(digital,0) AS digital, "
            "ROUND(offline,0) AS offline FROM alocacao ORDER BY ranking LIMIT 15"
        ),
    ),
    ChatIntent.SECTIONS: IntentQuery(
        intent=ChatIntent.SECTIONS,
        required_table="secoes",
        sql=(
            "SELECT NM_MUNICIPIO, NR_ZONA, NR_SECAO, eleitores_aptos, votos_nominais, "
            "score_secao, prioridade_secao FROM secoes ORDER BY score_secao DESC LIMIT 15"
        ),
    ),
    ChatIntent.FIELD_MAP: IntentQuery(
        intent=ChatIntent.FIELD_MAP,
        required_table="mapa_tatico",
        sql=(
            "SELECT NM_MUNICIPIO, cluster, total_secoes, secoes_alta, "
            "ROUND(budget_total_mun,0) AS budget, ROUND(custo_por_secao_alta,0) AS custo_secao "
            "FROM mapa_tatico ORDER BY ranking_final LIMIT 15"
        ),
    ),
    ChatIntent.CLUSTERS: IntentQuery(
        intent=ChatIntent.CLUSTERS,
        required_table="municipios",
        sql=(
            "SELECT cluster, COUNT(*) AS n, ROUND(AVG(indice_final),1) AS indice_medio "
            "FROM municipios GROUP BY cluster ORDER BY indice_medio DESC"
        ),
    ),
    ChatIntent.STATS: IntentQuery(
        intent=ChatIntent.STATS,
        required_table="municipios",
        sql=(
            "SELECT ROUND(AVG(indice_final),1) AS media, ROUND(MAX(indice_final),1) AS maximo, "
            "COUNT(*) AS total, SUM(CASE WHEN cluster='Diamante' THEN 1 ELSE 0 END) AS diamante "
            "FROM municipios"
        ),
    ),
    ChatIntent.RANKING: IntentQuery(
        intent=ChatIntent.RANKING,
        required_table="municipios",
        sql=(
            "SELECT municipio, cluster, ROUND(indice_final,1) AS indice, ranking_final "
            "FROM municipios ORDER BY ranking_final LIMIT 15"
        ),
    ),
}


def classify_intent(question: str) -> ChatIntent:
    q = str(question or "").casefold()
    for intent, keywords in _INTENT_KEYWORDS:
        if any(keyword.casefold() in q for keyword in keywords):
            return intent
    return ChatIntent.RANKING


def resolve_intent_query(repo: TableAwareRepository, question: str) -> IntentQuery:
    preferred = classify_intent(question)
    candidate = _QUERY_TEMPLATES[preferred]
    if repo.table_exists(candidate.required_table):
        return candidate
    return _QUERY_TEMPLATES[ChatIntent.RANKING]
