from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from application.electoral_base_mapper import ElectoralBaseMapper, TerritorialSignalMaps
from application.thematic_profile_builder import ThematicProfile, ThematicProfileBuilder
from domain.decision_models import CandidateProfile, CandidateTheme


@dataclass(frozen=True)
class CandidateContext:
    candidate: CandidateProfile
    thematic_vector: dict[str, float]
    identity_vector: dict[str, float]
    municipios_base: set[str]
    zonas_base: set[str]
    coherence_score: float
    theme_legitimacy: dict[str, float] = field(default_factory=dict)
    classified_themes: list[Any] = field(default_factory=list)
    consolidated_base_map: pd.DataFrame = field(default_factory=pd.DataFrame)
    plausible_expansion_map: pd.DataFrame = field(default_factory=pd.DataFrame)
    territorial_coherence_score: float = 0.0


class CandidateContextService:
    def __init__(self, thematic_builder: ThematicProfileBuilder | None = None, base_mapper: ElectoralBaseMapper | None = None):
        self.thematic_builder = thematic_builder or ThematicProfileBuilder()
        self.base_mapper = base_mapper or ElectoralBaseMapper()

    def build_context(
        self,
        candidate: CandidateProfile,
        territories: pd.DataFrame | None = None,
        themes: list[CandidateTheme] | None = None,
        historical_results: pd.DataFrame | None = None,
        public_materials: list[dict[str, Any]] | None = None,
    ) -> CandidateContext:
        thematic_profile = self.thematic_builder.build_profile(
            candidate,
            themes=themes,
            public_materials=public_materials,
        )
        territory_maps = self._build_territorial_maps(candidate, territories, historical_results)
        identity_vector = self._identity_vector(candidate, thematic_profile, territory_maps)
        coherence = self._overall_coherence(identity_vector, thematic_profile, territory_maps)
        return CandidateContext(
            candidate=candidate,
            thematic_vector=thematic_profile.thematic_vector,
            identity_vector=identity_vector,
            municipios_base=territory_maps.municipios_base,
            zonas_base=territory_maps.zonas_base,
            coherence_score=coherence,
            theme_legitimacy=thematic_profile.theme_legitimacy,
            classified_themes=thematic_profile.classified_themes,
            consolidated_base_map=territory_maps.consolidated_base_map,
            plausible_expansion_map=territory_maps.plausible_expansion_map,
            territorial_coherence_score=territory_maps.territorial_coherence_score,
        )

    def _build_territorial_maps(
        self,
        candidate: CandidateProfile,
        territories: pd.DataFrame | None,
        historical_results: pd.DataFrame | None,
    ) -> TerritorialSignalMaps:
        if territories is None or territories.empty:
            territories = pd.DataFrame(
                [
                    {
                        "municipio": municipio,
                        "zona_eleitoral": "",
                        "territorio_id": municipio,
                        "eleitores_aptos": 0,
                        "abstencao_pct": 0.0,
                        "competitividade": 0.5,
                    }
                    for municipio in candidate.municipios_base
                ]
            )
        return self.base_mapper.build_signal_maps(candidate, territories, historical_results=historical_results)

    def _identity_vector(
        self,
        candidate: CandidateProfile,
        thematic_profile: ThematicProfile,
        territory_maps: TerritorialSignalMaps,
    ) -> dict[str, float]:
        biography_score = min(1.0, len(candidate.biografia_resumida.strip()) / 500.0)
        history_score = min(1.0, len(candidate.historico_eleitoral) / 3.0)
        theme_score = min(1.0, len(thematic_profile.thematic_vector) / 5.0)
        territorial_score = territory_maps.territorial_coherence_score
        return {
            "incumbente": 1.0 if candidate.incumbente else 0.0,
            "idade_informada": 1.0 if candidate.idade is not None else 0.0,
            "biografia_informada": biography_score,
            "historico_eleitoral_informado": history_score,
            "temas_informados": theme_score,
            "territorio_informado": territorial_score,
        }

    def _overall_coherence(
        self,
        identity_vector: dict[str, float],
        thematic_profile: ThematicProfile,
        territory_maps: TerritorialSignalMaps,
    ) -> float:
        theme_legitimacy = (
            sum(thematic_profile.theme_legitimacy.values()) / len(thematic_profile.theme_legitimacy)
            if thematic_profile.theme_legitimacy
            else 0.0
        )
        identity_completeness = sum(identity_vector.values()) / max(len(identity_vector), 1)
        return max(
            0.0,
            min(
                1.0,
                0.35 * identity_completeness
                + 0.30 * territory_maps.territorial_coherence_score
                + 0.25 * theme_legitimacy
                + 0.10 * min(1.0, len(thematic_profile.thematic_vector) / 4.0),
            ),
        )
