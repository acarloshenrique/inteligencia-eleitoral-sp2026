import re

from infrastructure.privacy import sql_references_sensitive_columns

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
ALLOWED_TABLES = frozenset(
    {
        "municipios",
        "alocacao",
        "secoes",
        "mapa_tatico",
        "mart_custo_mobilizacao",
        "mart_priorizacao_territorial_sp",
        "dim_tempo",
        "mart_municipio_eleitoral",
        "mart_score_alocacao_modular",
        "mart_simulacao_orcamento",
        "mart_recomendacao_alocacao",
        "mart_midia_paga_municipio",
        "mart_social_mensagem_territorial",
        "mart_social_canal_regiao",
        "mart_recomendacao_zona_eleitoral",
        "mart_alocacao_zona_eleitoral",
        "features_zona_eleitoral",
        "fact_zona_eleitoral",
        "fact_secao_eleitoral",
        "dim_territorio_eleitoral",
    }
)

CHAT_QUERY_TEMPLATES = {
    "chat.product_recommendation": (
        "SELECT ranking, municipio_id_ibge7, ROUND(verba_sugerida,0) AS verba_sugerida, "
        "canal_ideal, mensagem_ideal, justificativa "
        "FROM mart_recomendacao_alocacao ORDER BY ranking LIMIT 15"
    ),
    "chat.allocation": (
        "SELECT municipio, cluster, ROUND(budget,0) AS budget, ROUND(digital,0) AS digital, "
        "ROUND(offline,0) AS offline FROM alocacao ORDER BY ranking LIMIT 15"
    ),
    "chat.sections": (
        "SELECT NM_MUNICIPIO, NR_ZONA, NR_SECAO, eleitores_aptos, votos_nominais, "
        "score_secao, prioridade_secao FROM secoes ORDER BY score_secao DESC LIMIT 15"
    ),
    "chat.field_map": (
        "SELECT NM_MUNICIPIO, cluster, total_secoes, secoes_alta, "
        "ROUND(budget_total_mun,0) AS budget, ROUND(custo_por_secao_alta,0) AS custo_secao "
        "FROM mapa_tatico ORDER BY ranking_final LIMIT 15"
    ),
    "chat.clusters": (
        "SELECT cluster, COUNT(*) AS n, ROUND(AVG(indice_final),1) AS indice_medio "
        "FROM municipios GROUP BY cluster ORDER BY indice_medio DESC"
    ),
    "chat.stats": (
        "SELECT ROUND(AVG(indice_final),1) AS media, ROUND(MAX(indice_final),1) AS maximo, "
        "COUNT(*) AS total, SUM(CASE WHEN cluster='Diamante' THEN 1 ELSE 0 END) AS diamante "
        "FROM municipios"
    ),
    "chat.ranking": (
        "SELECT municipio, cluster, ROUND(indice_final,1) AS indice, ranking_final "
        "FROM municipios ORDER BY ranking_final LIMIT 15"
    ),
}


def is_safe_identifier(identifier: str) -> bool:
    return bool(_IDENTIFIER_RE.fullmatch(identifier))


def is_allowed_table_name(table: str) -> bool:
    if not is_safe_identifier(table):
        return False
    return table in ALLOWED_TABLES


def get_allowed_chat_query_template(template_id: str) -> str:
    try:
        return CHAT_QUERY_TEMPLATES[template_id]
    except KeyError as exc:
        raise ValueError(f"chat query template nao permitido: {template_id}") from exc


def is_allowed_chat_query_template(template_id: str, sql: str | None = None) -> bool:
    expected = CHAT_QUERY_TEMPLATES.get(template_id)
    if expected is None:
        return False
    if sql is None:
        return True
    return sql == expected


def validate_chat_query_templates_lgpd() -> dict[str, bool]:
    return {template_id: not sql_references_sensitive_columns(sql) for template_id, sql in CHAT_QUERY_TEMPLATES.items()}
