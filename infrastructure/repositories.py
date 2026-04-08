from pathlib import Path
import time
from typing import Any, Sequence

import pandas as pd

from config.settings import AppPaths, get_settings
from infrastructure.ai import carrega_stack_ia
from infrastructure.env import persistir_relatorio, resolve_relatorio_path
from infrastructure.rag_cache import TimedLruCache
from infrastructure.rag_metrics import RagMetricsTracker
from infrastructure.sql_safety import is_allowed_table_name


class DuckDBAnalyticsRepository:
    def __init__(self, db):
        self._db = db

    def table_exists(self, table: str) -> bool:
        if not is_allowed_table_name(table):
            return False
        try:
            tabelas = self._db.execute("SHOW TABLES").df()
            if "name" in tabelas.columns:
                nomes = {str(n).lower() for n in tabelas["name"].tolist()}
            else:
                nomes = {str(n).lower() for n in tabelas.iloc[:, 0].tolist()}
            return table.lower() in nomes
        except Exception:
            return False

    def query_df(self, sql: str, params: Sequence[Any] | None = None) -> pd.DataFrame:
        if params is None:
            return self._db.execute(sql).df()
        return self._db.execute(sql, params).df()

    def register_table(self, name: str, df: pd.DataFrame) -> None:
        self._db.register(name, df)

    def count_municipios(self) -> int:
        df = self.query_df("SELECT COUNT(*) AS n FROM municipios")
        if df.empty:
            return 0
        return int(df.iloc[0]["n"])


class ParquetReportStore:
    def __init__(self, paths: AppPaths):
        self._paths = paths

    def save_report(self, df: pd.DataFrame, nome_arquivo: str) -> None:
        persistir_relatorio(self._paths, df, nome_arquivo)

    def load_report(self, nome_arquivo: str) -> pd.DataFrame | None:
        caminho = resolve_relatorio_path(self._paths, nome_arquivo)
        if not caminho.exists():
            return None
        return pd.read_parquet(str(caminho))


class ChromaGroqAIService:
    def __init__(self, chromadb_path: Path, *, app_paths: AppPaths | None = None):
        self._chromadb_path = chromadb_path
        settings = get_settings()
        paths = app_paths or settings.build_paths()
        self._metrics = RagMetricsTracker(paths=paths)
        self._cost_per_1k = float(settings.rag_cost_per_1k_tokens_usd)
        self._search_cache: TimedLruCache[str, dict[str, Any]] = TimedLruCache(maxsize=256, ttl_seconds=900)
        self._llm_cache: TimedLruCache[str, tuple[str, int]] = TimedLruCache(maxsize=256, ttl_seconds=300)
        self._recent_retrievals: TimedLruCache[str, list[str]] = TimedLruCache(maxsize=512, ttl_seconds=900)

    def _build_llm_fallback(self, pergunta: str, contexto: str) -> str:
        dados = contexto.split("Dados:\n", 1)[-1].strip()
        if not dados:
            return (
                "O LLM est\u00e1 indispon\u00edvel no momento. "
                "Posso seguir com an\u00e1lise determin\u00edstica via SQL se voc\u00ea refinar a pergunta."
            )
        resumo = dados.splitlines()[:8]
        return (
            "Modo degradado ativo (LLM indispon\u00edvel). "
            "Resumo determin\u00edstico dos dados consultados:\n" + "\n".join(resumo)
        )

    def _parse_retrieved(self, sem_txt: str) -> list[str]:
        return [m.strip() for m in sem_txt.split(",") if m.strip()]

    def _cache_key_complete(self, pergunta: str, contexto: str) -> str:
        return f"{pergunta.strip().lower()}||{contexto.strip().lower()}"

    def search_relevant(self, pergunta: str, n_results: int = 5) -> str:
        key = f"{pergunta.strip().lower()}::{n_results}"
        cached = self._search_cache.get(key)
        if cached is not None:
            sem_txt = str(cached["sem_txt"])
            self._recent_retrievals.set(pergunta.strip().lower(), self._parse_retrieved(sem_txt))
            self._search_cache.set(
                key,
                {
                    "sem_txt": sem_txt,
                    "latency_vector_ms": float(cached.get("latency_vector_ms", 0.0)),
                    "fallback_vector": bool(cached.get("fallback_vector", False)),
                    "cached_hit": True,
                },
            )
            return sem_txt

        start = time.perf_counter()
        sem_txt = ""
        fallback_vector = False
        try:
            embedder, col, _, _ = carrega_stack_ia(self._chromadb_path)
            if col is None or embedder is None:
                fallback_vector = True
            else:
                encoded = embedder.encode([pergunta])[0]
                vec = encoded.tolist() if hasattr(encoded, "tolist") else list(encoded)
                res = col.query(query_embeddings=[vec], n_results=n_results)
                metadatas = res.get("metadatas") or []
                municipios = []
                if metadatas and metadatas[0]:
                    for meta in metadatas[0]:
                        municipio = str(meta.get("municipio", "")).strip()
                        if municipio:
                            municipios.append(municipio)
                sem_txt = ", ".join(municipios)
        except Exception:
            fallback_vector = True
            sem_txt = ""
        latency_ms = (time.perf_counter() - start) * 1000.0
        self._search_cache.set(
            key,
            {
                "sem_txt": sem_txt,
                "latency_vector_ms": latency_ms,
                "fallback_vector": fallback_vector,
                "cached_hit": False,
            },
        )
        self._recent_retrievals.set(pergunta.strip().lower(), self._parse_retrieved(sem_txt))
        return sem_txt

    def complete(self, system_prompt: str, historico: list[dict], pergunta: str, contexto: str) -> tuple[str, int]:
        overall_start = time.perf_counter()
        search_key = f"{pergunta.strip().lower()}::5"
        search_meta = self._search_cache.get(search_key) or {}
        latency_vector_ms = float(search_meta.get("latency_vector_ms", 0.0))
        fallback_vector = bool(search_meta.get("fallback_vector", False))

        cached_key = self._cache_key_complete(pergunta, contexto)
        cached_value = self._llm_cache.get(cached_key)
        cached_llm = cached_value is not None
        fallback_llm = False
        llm_latency_ms = 0.0

        if cached_value is not None:
            texto, total_tokens = cached_value
        else:
            _, _, llm, _ = carrega_stack_ia(self._chromadb_path)
            mensagens = [{"role": "system", "content": system_prompt}]
            mensagens.extend(historico[-6:])
            mensagens.append({"role": "user", "content": f"CONTEXTO:\n{contexto}\n\nPERGUNTA: {pergunta}"})
            llm_start = time.perf_counter()
            try:
                resposta = llm.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=mensagens,
                    max_tokens=1024,
                    temperature=0.3,
                )
                texto = resposta.choices[0].message.content
                usage = getattr(resposta, "usage", None)
                total_tokens = int(getattr(usage, "total_tokens", 0) or 0)
            except Exception:
                fallback_llm = True
                texto = self._build_llm_fallback(pergunta, contexto)
                total_tokens = 0
            llm_latency_ms = (time.perf_counter() - llm_start) * 1000.0
            self._llm_cache.set(cached_key, (texto, total_tokens))

        total_latency_ms = (time.perf_counter() - overall_start) * 1000.0
        retrieved = self._recent_retrievals.get(pergunta.strip().lower()) or []
        cost_estimated = float(total_tokens / 1000.0 * self._cost_per_1k)
        self._metrics.record_query(
            question=pergunta,
            retrieved_municipios=retrieved,
            latency_total_ms=total_latency_ms,
            latency_vector_ms=latency_vector_ms,
            latency_llm_ms=llm_latency_ms,
            fallback_vector=fallback_vector,
            fallback_llm=fallback_llm,
            tokens_total=total_tokens,
            cost_estimated_usd=cost_estimated,
            cached_vector=bool(search_meta.get("cached_hit", False)),
            cached_llm=cached_llm,
        )
        return texto, total_tokens
