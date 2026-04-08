from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ErrorCode(StrEnum):
    ALLOCATION_CONTRACT_VIOLATION = "E-ALOC-001"
    ALLOCATION_EXECUTION_FAILED = "E-ALOC-002"
    CHAT_QUERY_FAILED = "E-CHAT-001"
    CHAT_LLM_FAILED = "E-CHAT-002"
    INFRA_UNAVAILABLE = "E-INFRA-001"


@dataclass
class ErrorDetail:
    code: ErrorCode
    message: str
    operation: str


class AppError(Exception):
    def __init__(self, detail: ErrorDetail):
        super().__init__(detail.message)
        self.detail = detail

    @property
    def code(self) -> str:
        return str(self.detail.code)

    @property
    def operation(self) -> str:
        return self.detail.operation

    def to_operational_message(self) -> str:
        return f"[{self.code}] {self.detail.message} (operacao={self.operation})"


class AppOperationalError(AppError):
    pass
