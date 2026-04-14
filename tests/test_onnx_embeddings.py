import numpy as np

from infrastructure.embeddings import OnnxMiniLMEmbedder


class _Encoding:
    def __init__(self, ids, attention_mask=None, type_ids=None):
        self.ids = ids
        self.attention_mask = attention_mask or [1] * len(ids)
        self.type_ids = type_ids or [0] * len(ids)


class _Tokenizer:
    def encode_batch(self, texts):
        return [_Encoding([101, len(text), 102]) for text in texts]


class _Input:
    def __init__(self, name):
        self.name = name


class _Session:
    def get_inputs(self):
        return [_Input("input_ids"), _Input("attention_mask"), _Input("token_type_ids")]

    def run(self, output_names, inputs):
        input_ids = inputs["input_ids"].astype(np.float32)
        batch, tokens = input_ids.shape
        hidden = np.zeros((batch, tokens, 2), dtype=np.float32)
        hidden[:, :, 0] = input_ids
        hidden[:, :, 1] = 1.0
        return [hidden]


def test_onnx_embedder_encodes_and_normalizes_embeddings():
    embedder = OnnxMiniLMEmbedder(tokenizer=_Tokenizer(), session=_Session(), max_length=3)

    embeddings = embedder.encode(["abc", "abcdef"])

    assert embeddings.shape == (2, 2)
    assert embeddings.dtype == np.float32
    assert np.allclose(np.linalg.norm(embeddings, axis=1), [1.0, 1.0])


def test_onnx_embedder_keeps_sentence_transformer_style_single_input():
    embedder = OnnxMiniLMEmbedder(tokenizer=_Tokenizer(), session=_Session(), max_length=3)

    embedding = embedder.encode("abc")

    assert embedding.shape == (2,)
