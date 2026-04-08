import re


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
ALLOWED_TABLES = frozenset({"municipios", "alocacao", "secoes", "mapa_tatico"})


def is_safe_identifier(identifier: str) -> bool:
    return bool(_IDENTIFIER_RE.fullmatch(identifier))


def is_allowed_table_name(table: str) -> bool:
    if not is_safe_identifier(table):
        return False
    return table in ALLOWED_TABLES
