from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import numpy as np

DEFAULT_EMBEDDING_MODEL_ID = "Xenova/all-MiniLM-L6-v2"
DEFAULT_ONNX_MODEL_FILE = "onnx/model.onnx"
DEFAULT_MAX_LENGTH = 256


class OnnxEmbeddingError(RuntimeError):
    pass


class OnnxMiniLMEmbedder:
    """ONNXRuntime embedder with an encode() interface for RAG retrieval."""

    def __init__(
        self,
        *,
        model_id: str = DEFAULT_EMBEDDING_MODEL_ID,
        model_file: str = DEFAULT_ONNX_MODEL_FILE,
        cache_dir: str | Path | None = None,
        max_length: int = DEFAULT_MAX_LENGTH,
        tokenizer: Any | None = None,
        session: Any | None = None,
    ) -> None:
        self.model_id = model_id
        self.model_file = model_file
        self.max_length = int(max_length)
        self._tokenizer = tokenizer
        self._session = session
        if self._tokenizer is None or self._session is None:
            model_dir = self._resolve_model_dir(model_id=model_id, model_file=model_file, cache_dir=cache_dir)
            if self._tokenizer is None:
                self._tokenizer = self._load_tokenizer(model_dir)
            if self._session is None:
                self._session = self._load_session(model_dir / model_file)

    def encode(self, texts: str | Iterable[str]) -> np.ndarray:
        single_input = isinstance(texts, str)
        batch = [texts] if single_input else [str(text) for text in texts]
        if not batch:
            return np.empty((0, 0), dtype=np.float32)

        encoded = self._tokenizer.encode_batch(batch)
        input_ids, attention_mask, token_type_ids = self._batch_to_arrays(encoded)
        session_inputs = self._build_session_inputs(input_ids, attention_mask, token_type_ids)
        outputs = self._session.run(None, session_inputs)
        if not outputs:
            raise OnnxEmbeddingError("ONNX embedding model returned no outputs")
        pooled = self._mean_pool(np.asarray(outputs[0]), attention_mask)
        normalized = self._l2_normalize(pooled).astype(np.float32)
        return normalized[0] if single_input else normalized

    def _batch_to_arrays(self, encoded: list[Any]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        lengths = [min(len(item.ids), self.max_length) for item in encoded]
        batch_max = max(lengths) if lengths else 0
        input_ids = np.zeros((len(encoded), batch_max), dtype=np.int64)
        attention_mask = np.zeros((len(encoded), batch_max), dtype=np.int64)
        token_type_ids = np.zeros((len(encoded), batch_max), dtype=np.int64)

        for row, item in enumerate(encoded):
            length = lengths[row]
            if length == 0:
                continue
            input_ids[row, :length] = np.asarray(item.ids[:length], dtype=np.int64)
            mask = getattr(item, "attention_mask", None) or [1] * len(item.ids)
            attention_mask[row, :length] = np.asarray(mask[:length], dtype=np.int64)
            types = getattr(item, "type_ids", None) or [0] * len(item.ids)
            token_type_ids[row, :length] = np.asarray(types[:length], dtype=np.int64)
        return input_ids, attention_mask, token_type_ids

    def _build_session_inputs(
        self,
        input_ids: np.ndarray,
        attention_mask: np.ndarray,
        token_type_ids: np.ndarray,
    ) -> dict[str, np.ndarray]:
        available = {item.name for item in self._session.get_inputs()}
        payload: dict[str, np.ndarray] = {}
        if "input_ids" in available:
            payload["input_ids"] = input_ids
        if "attention_mask" in available:
            payload["attention_mask"] = attention_mask
        if "token_type_ids" in available:
            payload["token_type_ids"] = token_type_ids
        if not payload:
            raise OnnxEmbeddingError("ONNX embedding model has no supported text inputs")
        return payload

    @staticmethod
    def _mean_pool(token_embeddings: np.ndarray, attention_mask: np.ndarray) -> np.ndarray:
        if token_embeddings.ndim != 3:
            raise OnnxEmbeddingError("ONNX embedding output must be [batch, tokens, dimensions]")
        mask = attention_mask.astype(np.float32)[..., None]
        summed = np.sum(token_embeddings.astype(np.float32) * mask, axis=1)
        counts = np.clip(np.sum(mask, axis=1), a_min=1e-9, a_max=None)
        return summed / counts

    @staticmethod
    def _l2_normalize(embeddings: np.ndarray) -> np.ndarray:
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        return embeddings / np.clip(norms, a_min=1e-12, a_max=None)

    @staticmethod
    def _resolve_model_dir(*, model_id: str, model_file: str, cache_dir: str | Path | None) -> Path:
        local_path = Path(model_id)
        if local_path.exists():
            if not (local_path / model_file).exists():
                raise OnnxEmbeddingError(f"Modelo ONNX nao encontrado em {local_path / model_file}")
            return local_path
        try:
            from huggingface_hub import snapshot_download
        except ImportError as exc:
            raise OnnxEmbeddingError("huggingface-hub e necessario para baixar/cachear o modelo ONNX") from exc
        try:
            return Path(
                snapshot_download(
                    repo_id=model_id,
                    cache_dir=str(cache_dir) if cache_dir else None,
                    allow_patterns=[model_file, "tokenizer.json", "tokenizer_config.json", "special_tokens_map.json"],
                )
            )
        except Exception as exc:
            raise OnnxEmbeddingError(f"Falha ao resolver modelo de embeddings ONNX '{model_id}'") from exc

    @staticmethod
    def _load_tokenizer(model_dir: Path) -> Any:
        tokenizer_path = model_dir / "tokenizer.json"
        if not tokenizer_path.exists():
            raise OnnxEmbeddingError(f"tokenizer.json nao encontrado em {model_dir}")
        try:
            from tokenizers import Tokenizer
        except ImportError as exc:
            raise OnnxEmbeddingError("tokenizers e necessario para embeddings ONNX") from exc
        return Tokenizer.from_file(str(tokenizer_path))

    @staticmethod
    def _load_session(model_path: Path) -> Any:
        if not model_path.exists():
            raise OnnxEmbeddingError(f"Modelo ONNX nao encontrado em {model_path}")
        try:
            import onnxruntime as ort
        except ImportError as exc:
            raise OnnxEmbeddingError("onnxruntime e necessario para inferencia de embeddings") from exc
        return ort.InferenceSession(str(model_path), providers=["CPUExecutionProvider"])


def build_default_embedder() -> OnnxMiniLMEmbedder:
    from config.settings import get_settings

    settings = get_settings()
    return OnnxMiniLMEmbedder(
        model_id=settings.embedding_model_id,
        model_file=settings.embedding_onnx_model_file,
        cache_dir=settings.embedding_cache_dir or None,
        max_length=settings.embedding_max_length,
    )
