from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from semantic_layer import SemanticQueryError, SemanticQueryService, SemanticRegistryWriter, build_semantic_registry


def _tables() -> dict[str, pd.DataFrame]:
    return {
        "gold_priority_score": pd.DataFrame(
            [
                {
                    "candidate_id": "123",
                    "territorio_id": "t1",
                    "uf": "SP",
                    "municipio_nome": "Sao Paulo",
                    "zona": "0001",
                    "score_prioridade_final": 0.9,
                    "potencial_expansao_score": 0.4,
                },
                {
                    "candidate_id": "123",
                    "territorio_id": "t2",
                    "uf": "SP",
                    "municipio_nome": "Campinas",
                    "zona": "0002",
                    "score_prioridade_final": 0.6,
                    "potencial_expansao_score": 0.8,
                },
            ]
        ),
        "gold_allocation_recommendations": pd.DataFrame(
            [
                {
                    "scenario_id": "baseline",
                    "candidate_id": "123",
                    "territorio_id": "t1",
                    "tipo_acao_sugerida": "retencao_base",
                    "score_prioridade_final": 0.9,
                    "percentual_orcamento_sugerido": 0.7,
                    "recurso_sugerido": 7000.0,
                }
            ]
        ),
        "gold_thematic_affinity": pd.DataFrame(
            [
                {"territorio_id": "t1", "tema": "saude", "thematic_affinity_score": 0.8},
                {"territorio_id": "t1", "tema": "educacao", "thematic_affinity_score": 0.6},
            ]
        ),
    }


def test_semantic_registry_has_required_entities_metrics_and_dimensions() -> None:
    registry = build_semantic_registry()

    assert {entity.entity_id for entity in registry.entities} == {
        "candidato",
        "territorio",
        "secao_eleitoral",
        "local_votacao",
        "municipio",
        "cluster_territorial",
        "tema",
        "recomendacao",
        "cenario",
        "gasto",
        "base_eleitoral",
        "concorrencia",
    }
    assert {metric.metric_id for metric in registry.metrics} == {
        "forca_base",
        "potencial_expansao",
        "intensidade_competitiva",
        "aderencia_tematica",
        "eficiencia_gasto",
        "prioridade_territorial",
        "confianca_recomendacao",
        "cobertura_territorial",
        "custo_por_voto_estimado",
        "share_potencial",
    }
    for metric in registry.metrics:
        assert metric.formula
        assert metric.grain
        assert metric.source_table
        assert metric.source_columns


def test_semantic_query_service_returns_metric_frame_and_ranking() -> None:
    service = SemanticQueryService(_tables())

    metric = service.metric_frame("prioridade_territorial", filters={"candidate_id": "123"})
    ranking = service.territory_ranking(candidate_id="123", limit=1)

    assert list(metric["metric_id"].unique()) == ["prioridade_territorial"]
    assert ranking.loc[0, "territorio_id"] == "t1"
    assert ranking.loc[0, "metric_value"] == 0.9


def test_semantic_query_service_handles_derived_metrics() -> None:
    service = SemanticQueryService(_tables())

    thematic = service.metric_frame("aderencia_tematica")
    confidence = service.metric_frame("confianca_recomendacao")

    assert thematic.loc[0, "metric_value"] == 0.7
    assert confidence.loc[0, "metric_value"] > 0


def test_semantic_service_supports_api_ui_catalog_consumption() -> None:
    service = SemanticQueryService(_tables())

    metrics = service.metric_catalog()
    entities = service.entity_catalog()
    recommendations = service.allocation_recommendations(candidate_id="123", scenario_id="baseline")

    assert "prioridade_territorial" in set(metrics["metric_id"])
    assert "territorio" in set(entities["entity_id"])
    assert recommendations.loc[0, "recurso_sugerido"] == 7000.0
    assert service.entity_dimensions("territorio")


def test_semantic_registry_writer_exports_json_and_markdown(tmp_path: Path) -> None:
    registry = build_semantic_registry()
    writer = SemanticRegistryWriter()

    json_path = writer.write_json(tmp_path / "semantic_registry.json", registry)
    md_path = writer.write_markdown(tmp_path / "semantic_registry.md", registry)

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    markdown = md_path.read_text(encoding="utf-8")
    assert payload["version"] == "semantic_registry_v1"
    assert "## Metrics" in markdown
    assert "prioridade_territorial" in markdown


def test_semantic_query_raises_for_unknown_metric() -> None:
    service = SemanticQueryService(_tables())

    try:
        service.metric_frame("nao_existe")
    except SemanticQueryError as exc:
        assert "Unknown semantic metric" in str(exc)
    else:
        raise AssertionError("Expected SemanticQueryError")
