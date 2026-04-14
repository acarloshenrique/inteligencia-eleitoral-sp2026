from __future__ import annotations

from typing import Any, Protocol, Sequence, runtime_checkable

import pandas as pd


@runtime_checkable
class DataStore(Protocol):
    """Tabular analytical store used by application use cases."""

    def table_exists(self, table: str) -> bool: ...

    def query_df(self, sql: str, params: Sequence[Any] | None = None) -> pd.DataFrame: ...

    def register_table(self, name: str, df: pd.DataFrame) -> None: ...

    def count_municipios(self) -> int: ...


@runtime_checkable
class AnalyticsRepository(DataStore, Protocol):
    """Backward-compatible name for the canonical DataStore port."""


@runtime_checkable
class ReportStore(Protocol):
    def save_report(self, df: pd.DataFrame, nome_arquivo: str) -> None: ...

    def load_report(self, nome_arquivo: str) -> pd.DataFrame | None: ...


@runtime_checkable
class VectorStore(Protocol):
    """Semantic retrieval port used by RAG workflows."""

    def search_relevant(self, pergunta: str, n_results: int = 5) -> str: ...


@runtime_checkable
class LLMClient(Protocol):
    """Text generation port used after deterministic/context retrieval."""

    def complete(self, system_prompt: str, historico: list[dict], pergunta: str, contexto: str) -> tuple[str, int]: ...


@runtime_checkable
class AIService(VectorStore, LLMClient, Protocol):
    """Backward-compatible composition of VectorStore and LLMClient ports."""
