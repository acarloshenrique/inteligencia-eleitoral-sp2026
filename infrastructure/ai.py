import logging
from typing import Any

import streamlit as st

from config.settings import get_settings
from infrastructure.secret_factory import build_secret_provider

logger = logging.getLogger(__name__)


@st.cache_resource(show_spinner="Carregando embedder...")
def carrega_embedder():
    from infrastructure.embeddings import build_default_embedder

    return build_default_embedder()


@st.cache_resource(show_spinner="Conectando ChromaDB...")
def carrega_chroma(chromadb_path):
    if not chromadb_path.exists():
        return None
    import chromadb

    c = chromadb.PersistentClient(path=str(chromadb_path))
    for nome in ("municipios_v2", "municipios"):
        try:
            col = c.get_collection(nome)
            if col.count() >= 600:
                return col
        except Exception as e:
            logger.warning("Falha ao carregar colecao ChromaDB '%s': %s", nome, e)
    return None


@st.cache_resource(show_spinner="Conectando LLM...")
def carrega_llm() -> tuple[Any | None, bool]:
    settings = get_settings()
    key = build_secret_provider(settings).get_secret("GROQ_API_KEY") or settings.groq_api_key
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
            logger.warning("Falha ao conectar no provider LLM real; modo degradado ativo. Motivo: %s", e)
    return None, False


@st.cache_resource(show_spinner=False)
def carrega_stack_ia(chromadb_path):
    col = carrega_chroma(chromadb_path)
    embedder = carrega_embedder() if col else None
    llm, groq_real = carrega_llm()
    return embedder, col, llm, groq_real
