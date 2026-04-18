from __future__ import annotations

import json
import re
from typing import Any

CPF_RE = re.compile(r"\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b")
TITULO_ELEITOR_RE = re.compile(r"\b\d{12}\b")
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PHONE_RE = re.compile(r"(?<!\d)(?:\+?55\s*)?(?:\(?\d{2}\)?\s*)?9?\d{4}[-\s]?\d{4}(?!\d)")

SENSITIVE_FIELD_NAMES: frozenset[str] = frozenset(
    {
        "cpf",
        "cpf_eleitor",
        "documento",
        "documento_eleitor",
        "rg",
        "titulo",
        "titulo_eleitor",
        "nr_titulo",
        "numero_titulo",
        "nome",
        "nome_eleitor",
        "nm_eleitor",
        "data_nascimento",
        "dt_nascimento",
        "endereco",
        "logradouro",
        "email",
        "telefone",
        "celular",
        "whatsapp",
    }
)

SENSITIVE_SQL_COLUMNS: frozenset[str] = SENSITIVE_FIELD_NAMES | frozenset(
    {
        "nome_mae",
        "nome_pai",
        "zona_residencial",
        "secao_residencial",
    }
)

_REDACTIONS: tuple[tuple[re.Pattern[str], str], ...] = (
    (CPF_RE, "[REDACTED_CPF]"),
    (TITULO_ELEITOR_RE, "[REDACTED_TITULO_ELEITOR]"),
    (EMAIL_RE, "[REDACTED_EMAIL]"),
    (PHONE_RE, "[REDACTED_PHONE]"),
)


def redact_text(value: Any) -> str:
    text = str(value or "")
    for pattern, replacement in _REDACTIONS:
        text = pattern.sub(replacement, text)
    return text


def _is_sensitive_key(key: Any) -> bool:
    normalized = str(key or "").strip().lower()
    return normalized in SENSITIVE_FIELD_NAMES or any(part in SENSITIVE_FIELD_NAMES for part in normalized.split("_"))


def redact_for_log(value: Any) -> Any:
    if value is None or isinstance(value, bool | int | float):
        return value
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, dict):
        return {
            str(key): "[REDACTED]" if _is_sensitive_key(key) else redact_for_log(item) for key, item in value.items()
        }
    if isinstance(value, tuple):
        return tuple(redact_for_log(item) for item in value)
    if isinstance(value, list):
        return [redact_for_log(item) for item in value]
    if isinstance(value, set):
        return sorted(redact_for_log(item) for item in value)
    return redact_text(value)


def redact_json_for_log(value: dict[str, Any] | list[Any] | None) -> str:
    payload: dict[str, Any] | list[Any] = {} if value is None else value
    return json.dumps(redact_for_log(payload), ensure_ascii=False)


def contains_personal_data(value: Any) -> bool:
    redacted = redact_for_log(value)
    return redacted != value


def sql_references_sensitive_columns(sql: str) -> bool:
    normalized = str(sql or "").lower()
    return any(re.search(rf"\b{re.escape(column)}\b", normalized) for column in SENSITIVE_SQL_COLUMNS)
