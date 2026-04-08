import json
from pathlib import Path
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from infrastructure.source_prioritization import load_source_catalog, prioritize_sources


def test_prioritization_accepts_high_signal_a_and_rejects_low_quality():
    payload = [
        {
            "key": "a_top",
            "nome": "Fonte A Top",
            "prioridade": "A",
            "area": "eleitoral",
            "cobertura_municipal": 0.99,
            "atualizacao_dias": 20,
            "licenca_aberta": True,
            "schema_quality": 0.9,
            "endpoint": "https://example.com/a_top",
        },
        {
            "key": "a_bad",
            "nome": "Fonte A Ruim",
            "prioridade": "A",
            "area": "eleitoral",
            "cobertura_municipal": 0.9,
            "atualizacao_dias": 90,
            "licenca_aberta": True,
            "schema_quality": 0.8,
            "endpoint": "https://example.com/a_bad",
        },
        {
            "key": "b_ok",
            "nome": "Fonte B OK",
            "prioridade": "B",
            "area": "fiscal",
            "cobertura_municipal": 0.85,
            "atualizacao_dias": 60,
            "licenca_aberta": True,
            "schema_quality": 0.8,
            "endpoint": "https://example.com/b_ok",
        },
    ]
    with tempfile.TemporaryDirectory() as tmp:
        catalog = Path(tmp) / "sources.json"
        catalog.write_text(json.dumps(payload), encoding="utf-8")
        sources = load_source_catalog(catalog)
        grouped = prioritize_sources(sources)

    assert len(grouped["accepted_a"]) == 1
    assert grouped["accepted_a"][0].source.key == "a_top"
    assert len(grouped["accepted_b"]) == 1
    assert grouped["accepted_b"][0].source.key == "b_ok"
    assert len(grouped["rejected"]) == 1
    assert grouped["rejected"][0].source.key == "a_bad"
