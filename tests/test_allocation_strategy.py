from pathlib import Path

import pandas as pd

from config.settings import Settings
from domain.allocation import calcular_alocacao
from infrastructure.allocation_engine import build_modular_allocation_scores
from infrastructure.allocation_strategy import load_allocation_strategy


def _df_mun():
    return pd.DataFrame(
        {
            "ranking_final": [1, 2],
            "municipio": ["Cidade A", "Cidade B"],
            "cluster": ["Diamante", "Alavanca"],
            "indice_final": [90.0, 80.0],
            "PD_qt": [70.0, 30.0],
            "pop_censo2022": [100_000, 10_000],
        }
    )


def test_load_default_allocation_strategy_from_repo_config(tmp_path):
    paths = Settings(DATA_ROOT=str(tmp_path / "data")).build_paths()
    strategy = load_allocation_strategy(paths)

    assert strategy.default_budget == 50000.0
    assert strategy.cluster_weights["Diamante"] == 1.0
    assert strategy.office_caps["prefeito_pequeno"] == 420000.0
    assert round(sum(strategy.score_modular_weights.normalized().values()), 6) == 1.0


def test_tenant_override_changes_budget_and_weights_without_code_change(tmp_path):
    default_paths = Settings(DATA_ROOT=str(tmp_path / "data"), TENANT_ID="cliente-a").build_paths()
    tenant_paths = Settings(DATA_ROOT=str(tmp_path / "data"), TENANT_ID="cliente-b").build_paths()
    override = tenant_paths.tenant_root / "config" / "allocation_weights.yaml"
    override.parent.mkdir(parents=True, exist_ok=True)
    override.write_text(
        """
version: cliente-b-v1
default_budget: 123456
cluster_weights:
  Alavanca: 0.95
score_modular_weights:
  potencial_eleitoral: 0.10
  oportunidade: 0.10
  eficiencia_midia: 0.60
  custo: 0.10
  risco_invertido: 0.10
""".strip(),
        encoding="utf-8",
    )

    default_strategy = load_allocation_strategy(default_paths)
    tenant_strategy = load_allocation_strategy(tenant_paths)

    assert default_strategy.default_budget == 50000.0
    assert default_strategy.cluster_weights["Alavanca"] == 0.70
    assert tenant_strategy.default_budget == 123456.0
    assert tenant_strategy.cluster_weights["Alavanca"] == 0.95
    assert tenant_strategy.version == "cliente-b-v1"
    assert tenant_strategy.tenant_override_path == override


def test_calcular_alocacao_uses_external_channel_weights():
    paths = Settings().build_paths()
    strategy = load_allocation_strategy(paths)
    baseline = calcular_alocacao(
        _df_mun(),
        budget=100000,
        cargo="deputado_federal",
        n=2,
        split_d=0.5,
        pesos_cluster=strategy.cluster_weights,
        tetos=strategy.office_caps,
        cargos_est=strategy.statewide_offices,
        channel_weights=strategy.channel_weights,
    )
    modified = strategy.model_copy(deep=True)
    payload = modified.channel_weights.model_dump()
    payload["digital_base"]["youtube"] = 0.70
    payload["digital_base"]["meta_fb_ig"] = 0.05
    modified_channel = modified.channel_weights.__class__.model_validate(payload)
    changed = calcular_alocacao(
        _df_mun(),
        budget=100000,
        cargo="deputado_federal",
        n=2,
        split_d=0.5,
        pesos_cluster=strategy.cluster_weights,
        tetos=strategy.office_caps,
        cargos_est=strategy.statewide_offices,
        channel_weights=modified_channel,
    )

    assert changed["youtube"].sum() > baseline["youtube"].sum()


def test_modular_score_uses_external_score_weights():
    mart_municipio = pd.DataFrame(
        [
            {"municipio_id_ibge7": "1", "indice_medio_3ciclos": 90.0, "data_quality_score": 0.90},
            {"municipio_id_ibge7": "2", "indice_medio_3ciclos": 40.0, "data_quality_score": 0.90},
        ]
    )
    mart_potencial = pd.DataFrame(
        [
            {"municipio_id_ibge7": "1", "potencial_eleitoral_ajustado_social": 100.0},
            {"municipio_id_ibge7": "2", "potencial_eleitoral_ajustado_social": 10.0},
        ]
    )
    mart_territorial = pd.DataFrame(
        [
            {"municipio_id_ibge7": "1", "score_priorizacao_territorial_sp": 10.0},
            {"municipio_id_ibge7": "2", "score_priorizacao_territorial_sp": 100.0},
        ]
    )
    empty = pd.DataFrame()

    potential_first = build_modular_allocation_scores(
        mart_municipio=mart_municipio,
        mart_potencial=mart_potencial,
        mart_territorial=mart_territorial,
        mart_custo=empty,
        mart_sensibilidade=empty,
        mart_midia=empty,
        score_weights={"potencial_eleitoral": 1.0, "oportunidade": 0.0, "eficiencia_midia": 0.0, "custo": 0.0, "risco_invertido": 0.0},
    )
    opportunity_first = build_modular_allocation_scores(
        mart_municipio=mart_municipio,
        mart_potencial=mart_potencial,
        mart_territorial=mart_territorial,
        mart_custo=empty,
        mart_sensibilidade=empty,
        mart_midia=empty,
        score_weights={"potencial_eleitoral": 0.0, "oportunidade": 1.0, "eficiencia_midia": 0.0, "custo": 0.0, "risco_invertido": 0.0},
    )

    assert potential_first.iloc[0]["municipio_id_ibge7"] == "1"
    assert opportunity_first.iloc[0]["municipio_id_ibge7"] == "2"
