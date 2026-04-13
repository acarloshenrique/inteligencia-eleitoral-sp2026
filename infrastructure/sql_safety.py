import re


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
    }
)


def is_safe_identifier(identifier: str) -> bool:
    return bool(_IDENTIFIER_RE.fullmatch(identifier))


def is_allowed_table_name(table: str) -> bool:
    if not is_safe_identifier(table):
        return False
    return table in ALLOWED_TABLES
