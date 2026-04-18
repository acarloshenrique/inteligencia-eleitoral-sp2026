from __future__ import annotations

import re
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
class IntentPattern:
    intent: ChatIntent
    expression: re.Pattern[str]
    weight: float
    label: str


@dataclass(frozen=True)
class IntentClassification:
    intent: ChatIntent
    confidence: float
    matched_patterns: tuple[str, ...]
    normalized_question: str


@dataclass(frozen=True)
class IntentQuery:
    intent: ChatIntent
    template_id: str
    sql: str
    required_table: str
    confidence: float = 0.0
    matched_patterns: tuple[str, ...] = ()


_INTENT_PRIORITY: dict[ChatIntent, int] = {
    ChatIntent.PRODUCT_RECOMMENDATION: 0,
    ChatIntent.ALLOCATION: 1,
    ChatIntent.SECTIONS: 2,
    ChatIntent.FIELD_MAP: 3,
    ChatIntent.CLUSTERS: 4,
    ChatIntent.STATS: 5,
    ChatIntent.RANKING: 6,
}


def _pattern(intent: ChatIntent, regex: str, *, weight: float, label: str) -> IntentPattern:
    return IntentPattern(intent=intent, expression=re.compile(regex, re.IGNORECASE), weight=weight, label=label)


_INTENT_PATTERNS: tuple[IntentPattern, ...] = (
    _pattern(
        ChatIntent.PRODUCT_RECOMMENDATION,
        r"\b(recomend(?:acao|ar|e)?|canal\s+ideal|mensagem\s+ideal|verba\s+sugerida|roi\s+politico)\b",
        weight=2.0,
        label="product_recommendation.terms",
    ),
    _pattern(
        ChatIntent.PRODUCT_RECOMMENDATION,
        r"\b(qual|onde|maior|melhor)\b.{0,40}\b(roi|canal|mensagem)\b",
        weight=2.0,
        label="product_recommendation.question",
    ),
    _pattern(
        ChatIntent.ALLOCATION,
        r"\b(budget|orcamento|aloc(?:ar|acao)|invest(?:ir|imento)|verba|desperdic(?:io|ando))\b",
        weight=1.5,
        label="allocation.terms",
    ),
    _pattern(
        ChatIntent.ALLOCATION,
        r"\b(r\$|rs\s*)?\d+[\d\.,]*\s*(k|mil|mi|milhoes)?\b.{0,40}\b(aloc|invest|verba|orcamento|budget)\b",
        weight=2.0,
        label="allocation.amount",
    ),
    _pattern(
        ChatIntent.SECTIONS,
        r"\b(sec(?:ao|oes)|zona\s+eleitoral|local\s+de\s+votacao|urna|campo)\b",
        weight=1.5,
        label="sections.terms",
    ),
    _pattern(
        ChatIntent.FIELD_MAP,
        r"\b(mapa(?:\s+tatico)?|mobilizacao|custo\s+por\s+secao|territorial)\b",
        weight=1.25,
        label="field_map.terms",
    ),
    _pattern(
        ChatIntent.CLUSTERS,
        r"\b(cluster(?:es|s)?|diamante|alavanca|consolidacao|descarte)\b",
        weight=1.5,
        label="clusters.terms",
    ),
    _pattern(
        ChatIntent.STATS,
        r"\b(media|total|estatistic(?:a|as)|sumario|resumo|quantos|distribuicao)\b",
        weight=1.25,
        label="stats.terms",
    ),
    _pattern(
        ChatIntent.RANKING,
        r"\b(ranking|rank|top\s*\d*|liste|listar|principais|prioridade|prioritarios|municipios?)\b",
        weight=1.0,
        label="ranking.terms",
    ),
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


def _build_intent_query(classification: IntentClassification) -> IntentQuery:
    template_id, required_table = _INTENT_SPECS[classification.intent]
    sql = get_allowed_chat_query_template(template_id)
    if not is_allowed_chat_query_template(template_id, sql):
        raise ValueError(f"template de query nao permitido para intent {classification.intent}")
    return IntentQuery(
        intent=classification.intent,
        template_id=template_id,
        sql=sql,
        required_table=required_table,
        confidence=classification.confidence,
        matched_patterns=classification.matched_patterns,
    )


def classify_question_intent(question: str) -> IntentClassification:
    normalized = normalize_for_matching(question)
    scores: dict[ChatIntent, float] = {}
    matches: dict[ChatIntent, list[str]] = {}

    for pattern in _INTENT_PATTERNS:
        found = pattern.expression.findall(normalized)
        if not found:
            continue
        scores[pattern.intent] = scores.get(pattern.intent, 0.0) + (pattern.weight * len(found))
        matches.setdefault(pattern.intent, []).append(pattern.label)

    if not scores:
        return IntentClassification(
            intent=ChatIntent.RANKING,
            confidence=0.0,
            matched_patterns=(),
            normalized_question=normalized,
        )

    intent, score = sorted(scores.items(), key=lambda item: (-item[1], _INTENT_PRIORITY[item[0]]))[0]
    total_score = sum(scores.values())
    confidence = float(score / total_score) if total_score else 0.0
    return IntentClassification(
        intent=intent,
        confidence=round(confidence, 4),
        matched_patterns=tuple(matches.get(intent, ())),
        normalized_question=normalized,
    )


def classify_intent(question: str) -> ChatIntent:
    return classify_question_intent(question).intent


def resolve_intent_query(repo: TableAwareRepository, question: str) -> IntentQuery:
    preferred = classify_question_intent(question)
    candidate = _build_intent_query(preferred)
    if repo.table_exists(candidate.required_table):
        return candidate
    fallback = IntentClassification(
        intent=ChatIntent.RANKING,
        confidence=0.0,
        matched_patterns=("fallback.missing_table",),
        normalized_question=preferred.normalized_question,
    )
    return _build_intent_query(fallback)
