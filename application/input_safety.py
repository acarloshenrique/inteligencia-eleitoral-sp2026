from __future__ import annotations

import re
from dataclasses import dataclass

_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_TAG_RE = re.compile(r"<[^>]{1,200}>")
_WS_RE = re.compile(r"\s+")
_INJECTION_PATTERNS = (
    "ignore previous instructions",
    "ignore all previous instructions",
    "desconsidere as instrucoes anteriores",
    "desconsidere as instru??es anteriores",
    "revele o system prompt",
    "show system prompt",
    "developer message",
    "system message",
)


@dataclass(frozen=True)
class SanitizedPrompt:
    original_length: int
    text: str
    truncated: bool
    injection_flag: bool


def sanitize_user_prompt(prompt: str, *, max_chars: int = 500) -> SanitizedPrompt:
    raw = str(prompt or "")
    cleaned = _CONTROL_RE.sub(" ", raw)
    cleaned = _TAG_RE.sub(" ", cleaned)
    cleaned = _WS_RE.sub(" ", cleaned).strip()
    truncated = len(cleaned) > max_chars
    if truncated:
        cleaned = cleaned[:max_chars].rstrip()
    normalized = cleaned.casefold()
    injection_flag = any(pattern in normalized for pattern in _INJECTION_PATTERNS)
    return SanitizedPrompt(original_length=len(raw), text=cleaned, truncated=truncated, injection_flag=injection_flag)
