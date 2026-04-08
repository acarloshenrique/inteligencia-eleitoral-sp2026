import logging

import chromadb
import streamlit as st
from sentence_transformers import SentenceTransformer

from config.settings import get_settings

logger = logging.getLogger(__name__)


class MockCompletions:
    def create(self, model, messages, max_tokens=512, temperature=0.3):
        from unittest.mock import MagicMock

        p = messages[-1]["content"].lower()
        if "budget" in p or "alocar" in p:
            t = "Recomendo concentrar o investimento nos municípios Diamante, com foco em eventos presenciais e Meta FB+IG para maximizar o retorno eleitoral."
        elif "diamante" in p or "cluster" in p:
            t = "Municípios Diamante combinam score territorial >70 e VS >70 — histórico favorável e alta receptividade social. São a prioridade máxima de investimento."
        elif "seção" in p or "campo" in p:
            t = "As seções de Alta prioridade têm maior volume de votos e alto engajamento nominal. Concentre eventos e distribuição de material nessas seções."
        elif "ranking" in p or "priorit" in p:
            t = "Cássia dos Coqueiros lidera (índice 94.5), seguida por Dirce Reis (88.8) e Cândido Rodrigues (85.8). Todos no cluster Diamante."
        else:
            t = "Posso analisar ranking de municípios, clusters táticos, alocação de budget, estratégia de mídia e priorização de seções eleitorais para SP 2026."
        r = MagicMock()
        r.choices[0].message.content = t
        r.usage.total_tokens = len(t.split()) * 2
        return r


class MockLLMClient:
    def __init__(self):
        self.chat = type("Chat", (), {"completions": MockCompletions()})()


@st.cache_resource(show_spinner="Carregando embedder...")
def carrega_embedder():
    return SentenceTransformer("all-MiniLM-L6-v2")


@st.cache_resource(show_spinner="Conectando ChromaDB...")
def carrega_chroma(chromadb_path):
    if not chromadb_path.exists():
        return None
    c = chromadb.PersistentClient(path=str(chromadb_path))
    for nome in ("municipios_v2", "municipios"):
        try:
            col = c.get_collection(nome)
            if col.count() >= 600:
                return col
        except Exception as e:
            logger.warning("Falha ao carregar coleção ChromaDB '%s': %s", nome, e)
    return None


@st.cache_resource(show_spinner="Conectando LLM...")
def carrega_llm():
    key = get_settings().groq_api_key
    if key:
        try:
            from groq import Groq

            c = Groq(api_key=key)
            c.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": "OK"}],
                max_tokens=3,
            )
            return c, True
        except Exception as e:
            logger.warning("Falha ao conectar no provider LLM real; usando fallback simulado. Motivo: %s", e)
    return MockLLMClient(), False


@st.cache_resource(show_spinner=False)
def carrega_stack_ia(chromadb_path):
    col = carrega_chroma(chromadb_path)
    embedder = carrega_embedder() if col else None
    llm, groq_real = carrega_llm()
    return embedder, col, llm, groq_real
