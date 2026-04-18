from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any

from domain.decision_models import CandidateProfile, CandidateTheme

THEME_SYNONYMS: dict[str, tuple[str, ...]] = {
    "saude": ("saude", "saúde", "sus", "hospital", "ubs", "medico", "médico"),
    "educacao": ("educacao", "educação", "escola", "creche", "professor", "ensino"),
    "seguranca": ("seguranca", "segurança", "policia", "polícia", "crime", "violencia", "violência"),
    "emprego": ("emprego", "renda", "trabalho", "desenvolvimento", "empreendedorismo"),
    "mobilidade": ("mobilidade", "transporte", "onibus", "ônibus", "transito", "trânsito"),
    "habitacao": ("habitacao", "habitação", "moradia", "casa", "regularizacao", "regularização"),
    "meio_ambiente": ("meio ambiente", "clima", "saneamento", "residuo", "resíduo", "verde"),
}


@dataclass(frozen=True)
class ThemeSignal:
    tema: str
    score: float
    prioridade: str
    evidence_count: int = 0
    legitimacy_score: float = 0.5
    notes: str = ""


@dataclass(frozen=True)
class ThematicProfile:
    thematic_vector: dict[str, float]
    theme_legitimacy: dict[str, float]
    classified_themes: list[ThemeSignal] = field(default_factory=list)


def normalize_theme(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def normalize_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


class ThematicProfileBuilder:
    """Build deterministic thematic signals from declared themes and public structured materials."""

    def build(
        self,
        candidate: CandidateProfile,
        themes: list[CandidateTheme] | None = None,
        public_materials: list[dict[str, Any]] | None = None,
    ) -> dict[str, float]:
        return self.build_profile(candidate, themes=themes, public_materials=public_materials).thematic_vector

    def build_profile(
        self,
        candidate: CandidateProfile,
        *,
        themes: list[CandidateTheme] | None = None,
        public_materials: list[dict[str, Any]] | None = None,
    ) -> ThematicProfile:
        declared = self._declared_theme_weights(candidate)
        explicit = self._explicit_theme_weights(themes or [])
        evidence_counts = self._material_evidence_counts(public_materials or [])
        all_themes = sorted(set(declared) | set(explicit) | set(evidence_counts))

        thematic_vector: dict[str, float] = {}
        legitimacy: dict[str, float] = {}
        classified: list[ThemeSignal] = []
        for tema in all_themes:
            declared_score = declared.get(tema, 0.0)
            explicit_score = explicit.get(tema, 0.0)
            evidence_count = evidence_counts.get(tema, 0)
            evidence_score = min(1.0, evidence_count / 3.0)
            score = _clamp01(max(declared_score, explicit_score) * 0.75 + evidence_score * 0.25)
            legitimacy_score = self._legitimacy_for_theme(tema, themes or [], evidence_count=evidence_count)
            priority = "alta" if score >= 0.75 else "media" if score >= 0.45 else "baixa"
            thematic_vector[tema] = score
            legitimacy[tema] = legitimacy_score
            classified.append(
                ThemeSignal(
                    tema=tema,
                    score=score,
                    prioridade=priority,
                    evidence_count=evidence_count,
                    legitimacy_score=legitimacy_score,
                )
            )
        return ThematicProfile(thematic_vector=thematic_vector, theme_legitimacy=legitimacy, classified_themes=classified)

    def _declared_theme_weights(self, candidate: CandidateProfile) -> dict[str, float]:
        weights: dict[str, float] = {}
        for tema in candidate.temas_prioritarios:
            normalized = normalize_theme(tema)
            if normalized:
                weights[normalized] = max(weights.get(normalized, 0.0), 1.0)
        for tema in candidate.temas_secundarios:
            normalized = normalize_theme(tema)
            if normalized:
                weights[normalized] = max(weights.get(normalized, 0.0), 0.55)
        return weights

    def _explicit_theme_weights(self, themes: list[CandidateTheme]) -> dict[str, float]:
        weights: dict[str, float] = {}
        for theme in themes:
            normalized = normalize_theme(theme.tema)
            if not normalized:
                continue
            base = {"alta": 1.0, "media": 0.65, "baixa": 0.35}[theme.prioridade]
            weights[normalized] = max(weights.get(normalized, 0.0), _clamp01(base * theme.legitimidade_percebida))
        return weights

    def _material_evidence_counts(self, public_materials: list[dict[str, Any]]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for material in public_materials:
            themes = material.get("temas") or material.get("themes") or []
            if isinstance(themes, str):
                themes = [themes]
            for theme in themes:
                normalized = normalize_theme(theme)
                if normalized:
                    counts[normalized] = counts.get(normalized, 0) + 1
            text = normalize_text(material.get("texto") or material.get("text") or material.get("descricao") or "")
            for canonical, variants in THEME_SYNONYMS.items():
                if any(normalize_text(variant) in text for variant in variants):
                    counts[canonical] = counts.get(canonical, 0) + 1
        return counts

    def _legitimacy_for_theme(self, tema: str, themes: list[CandidateTheme], *, evidence_count: int) -> float:
        explicit = [theme for theme in themes if normalize_theme(theme.tema) == tema]
        explicit_score = max((theme.legitimidade_percebida for theme in explicit), default=0.5)
        evidence_bonus = min(0.25, evidence_count * 0.08)
        return _clamp01(0.75 * explicit_score + evidence_bonus)
