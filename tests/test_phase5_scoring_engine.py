from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scoring import ScoreWeights, ScoringEngine, load_score_weights
from scoring.base_strength import compute_base_strength
from scoring.competition import compute_competition
from scoring.cost_efficiency import compute_cost_efficiency
from scoring.expansion import compute_expansion
from scoring.thematic_affinity import compute_thematic_affinity


def _territories() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "territorio_id": "A",
                "municipio": "SAO PAULO",
                "base_context_score": 0.8,
                "eleitores_aptos": 1000,
                "abstencao_pct": 0.2,
                "competitividade": 0.7,
                "custo_operacional_estimado": 10.0,
                "indicadores_tematicos": {"saude": 0.9, "educacao": 0.4},
            },
            {
                "territorio_id": "B",
                "municipio": "OSASCO",
                "base_context_score": 0.3,
                "eleitores_aptos": 5000,
                "abstencao_pct": 0.5,
                "competitividade": 0.4,
                "custo_operacional_estimado": 30.0,
                "indicadores_tematicos": {"saude": 0.2, "educacao": 0.8},
            },
        ]
    )


def test_each_score_component_is_computed_by_own_module():
    df = _territories()

    assert compute_base_strength(df).between(0, 1).all()
    assert compute_thematic_affinity(df, {"saude": 1.0}).between(0, 1).all()
    assert compute_expansion(df).between(0, 1).all()
    assert compute_cost_efficiency(df).between(0, 1).all()
    assert compute_competition(df).between(0, 1).all()


def test_priority_score_uses_configurable_formula_and_component_explanations():
    weights = ScoreWeights(
        base_eleitoral_score=0.30,
        afinidade_tematica_score=0.20,
        potencial_expansao_score=0.15,
        custo_eficiencia_score=0.15,
        concorrencia_score=-0.10,
        capacidade_operacional_score=0.10,
    )
    scored = ScoringEngine(weights=weights).score(_territories(), thematic_vector={"saude": 1.0}, capacidade_operacional=0.8)
    row = scored.iloc[0]
    expected = (
        0.30 * row["base_eleitoral_score"]
        + 0.20 * row["afinidade_tematica_score"]
        + 0.15 * row["potencial_expansao_score"]
        + 0.15 * row["custo_eficiencia_score"]
        - 0.10 * row["concorrencia_score"]
        + 0.10 * row["capacidade_operacional_score"]
    )

    assert row["score_prioridade_final"] == max(0.0, min(1.0, expected))
    assert "score_prioridade_final=" in row["score_explanation"]
    assert row["score_component_details"]["concorrencia_score"]["weight"] == -0.10
    assert row["score_component_details"]["base_eleitoral_score"]["contribution"] > 0


def test_score_weights_load_from_json_and_yaml(tmp_path: Path):
    json_path = tmp_path / "weights.json"
    json_path.write_text(json.dumps({"base_eleitoral_score": 0.4}), encoding="utf-8")
    yaml_path = tmp_path / "weights.yaml"
    yaml_path.write_text("base_eleitoral_score: 0.25\nconcorrencia_score: -0.05\n", encoding="utf-8")

    json_weights = load_score_weights(json_path)
    yaml_weights = load_score_weights(yaml_path)

    assert json_weights.base_eleitoral_score == 0.4
    assert json_weights.afinidade_tematica_score == 0.20
    assert yaml_weights.base_eleitoral_score == 0.25
    assert yaml_weights.concorrencia_score == -0.05


def test_scoring_engine_persists_gold_parquet_duckdb_manifest(tmp_path: Path):
    engine = ScoringEngine()
    scored = engine.score(_territories(), thematic_vector={"saude": 1.0}, capacidade_operacional=0.8)

    result = engine.persist_gold(scored, gold_root=tmp_path / "gold", dataset_version="unit")

    assert result.parquet_path.exists()
    assert result.manifest_path.exists()
    assert result.rows == 2
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["dataset"] == "territorial_priority_scores"
    assert manifest["quality"]["rows"] == 2
    assert manifest["weights"]["base_eleitoral_score"] == 0.30
    persisted = pd.read_parquet(result.parquet_path)
    assert "score_prioridade_final" in persisted.columns
    assert "score_component_details" in persisted.columns


def test_breakdowns_include_component_details():
    engine = ScoringEngine()
    scored = engine.score(_territories(), thematic_vector={"saude": 1.0}, capacidade_operacional=0.8)

    breakdown = engine.breakdowns(scored)[0]

    assert breakdown.territorio_id == "A"
    assert "summary" in breakdown.explicacoes
    assert "components" in breakdown.explicacoes
    assert "afinidade_tematica_score" in breakdown.explicacoes["components"]
