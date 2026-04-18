from __future__ import annotations

import pandas as pd

from application.candidate_context_service import CandidateContextService
from application.electoral_base_mapper import ElectoralBaseMapper, normalize_territory_name
from application.thematic_profile_builder import ThematicProfileBuilder, normalize_theme
from domain.decision_models import CandidateProfile, CandidateTheme


def test_thematic_profile_builder_classifies_declared_public_and_explicit_themes():
    candidate = CandidateProfile(
        candidate_id="cand",
        nome_politico="Nome",
        cargo="Prefeito",
        partido="P",
        temas_prioritarios=("Saúde",),
        temas_secundarios=("Educação",),
    )
    themes = [CandidateTheme(candidate_id="cand", tema="Segurança", prioridade="alta", legitimidade_percebida=0.8)]
    materials = [
        {"texto": "Plano publico para ampliar UBS, hospital e atendimento do SUS", "fonte": "programa"},
        {"temas": ["mobilidade"], "descricao": "Transporte publico e onibus"},
    ]

    profile = ThematicProfileBuilder().build_profile(candidate, themes=themes, public_materials=materials)

    assert normalize_theme("Saúde Pública") == "saude_publica"
    assert profile.thematic_vector["saude"] > profile.thematic_vector["educacao"]
    assert profile.thematic_vector["seguranca"] >= 0.6
    assert profile.theme_legitimacy["seguranca"] >= 0.6
    assert any(signal.tema == "mobilidade" and signal.evidence_count >= 1 for signal in profile.classified_themes)


def test_electoral_base_mapper_consolidates_manual_and_historical_base():
    candidate = CandidateProfile(
        candidate_id="cand",
        nome_politico="Nome",
        cargo="Prefeito",
        partido="P",
        municipios_base=("São Paulo",),
        zonas_base=("2",),
    )
    territories = pd.DataFrame(
        [
            {"territorio_id": "A", "municipio": "Sao Paulo", "zona_eleitoral": 1, "eleitores_aptos": 1000, "abstencao_pct": 0.2, "competitividade": 0.6},
            {"territorio_id": "B", "municipio": "Osasco", "zona_eleitoral": 2, "eleitores_aptos": 5000, "abstencao_pct": 0.4, "competitividade": 0.8},
            {"territorio_id": "C", "municipio": "Santos", "zona_eleitoral": 3, "eleitores_aptos": 7000, "abstencao_pct": 0.5, "competitividade": 0.7},
        ]
    )
    history = pd.DataFrame(
        [
            {"candidate_id": "cand", "municipio": "Santos", "zona": 3, "percentual_votos": 0.42},
            {"candidate_id": "other", "municipio": "Osasco", "zona": 2, "percentual_votos": 0.99},
        ]
    )

    maps = ElectoralBaseMapper().build_signal_maps(candidate, territories, historical_results=history)

    assert normalize_territory_name("São Paulo") == "SAO PAULO"
    base = maps.consolidated_base_map.set_index("territorio_id")
    assert bool(base.loc["A", "candidate_base_flag"]) is True
    assert bool(base.loc["B", "candidate_zone_base_flag"]) is True
    assert base.loc["C", "historical_base_score"] == 0.42
    assert base.loc["A", "base_context_score"] >= 0.55
    assert maps.plausible_expansion_map.iloc[0]["expansion_plausibility_score"] >= maps.plausible_expansion_map.iloc[-1]["expansion_plausibility_score"]
    assert maps.territorial_coherence_score > 0.5


def test_candidate_context_service_outputs_identity_theme_base_and_expansion_maps():
    candidate = CandidateProfile(
        candidate_id="cand",
        nome_politico="Nome",
        cargo="Prefeito",
        partido="P",
        idade=45,
        incumbente=True,
        biografia_resumida="Gestor publico com atuacao em saude e educacao." * 5,
        temas_prioritarios=("saude",),
        temas_secundarios=("educacao",),
        historico_eleitoral=({"ano": 2020, "votos": 1000},),
        municipios_base=("SAO PAULO",),
    )
    territories = pd.DataFrame(
        [
            {"territorio_id": "A", "municipio": "SAO PAULO", "zona_eleitoral": 1, "eleitores_aptos": 10000, "abstencao_pct": 0.2, "competitividade": 0.6},
            {"territorio_id": "B", "municipio": "OSASCO", "zona_eleitoral": 2, "eleitores_aptos": 12000, "abstencao_pct": 0.4, "competitividade": 0.8},
        ]
    )
    themes = [CandidateTheme(candidate_id="cand", tema="saude", prioridade="alta", legitimidade_percebida=0.9)]
    public_materials = [{"texto": "Programa publico com foco em hospital, UBS e educacao integral."}]

    context = CandidateContextService().build_context(
        candidate,
        territories=territories,
        themes=themes,
        public_materials=public_materials,
    )

    assert context.identity_vector["incumbente"] == 1.0
    assert context.identity_vector["historico_eleitoral_informado"] > 0
    assert context.thematic_vector["saude"] >= 0.75
    assert context.theme_legitimacy["saude"] >= 0.7
    assert not context.consolidated_base_map.empty
    assert not context.plausible_expansion_map.empty
    assert context.territorial_coherence_score > 0
    assert context.coherence_score > 0.45
