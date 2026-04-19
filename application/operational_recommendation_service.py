from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from allocation.budget_allocator import BudgetAllocator
from allocation.explanation_builder import build_justification, build_scenario_summary, choose_action
from allocation.scenario_builder import SCENARIO_PROFILES, ScenarioBuilder, ScenarioName
from infrastructure.product_reports import build_executive_pdf_bytes, build_operational_workbook_bytes


@dataclass(frozen=True)
class OperationalRecommendationResult:
    recommendations_path: Path
    summary_path: Path
    executive_pdf_path: Path
    workbook_path: Path
    rows: int
    scenarios: list[str]
    generated_at_utc: str


class OperationalRecommendationService:
    def __init__(self, paths) -> None:
        self.paths = paths

    def run(
        self,
        *,
        scores_path: Path | None = None,
        dataset_version: str = "sprint3_operational_recommendations",
        budget_total: float = 200000.0,
        top_n: int = 30,
        capacidade_operacional: float = 0.7,
        janela_temporal_dias: int = 45,
        score_granularity: str = "zona",
        scenarios: tuple[str, ...] = ("conservador", "hibrido", "agressivo"),
    ) -> OperationalRecommendationResult:
        source_path = scores_path or self._latest_calibrated_scores()
        if source_path is None:
            raise FileNotFoundError("Not found in repo: gold_calibrated_priority_scores.")
        scores = pd.read_parquet(source_path)
        base = self._prepare_scores(scores, score_granularity=score_granularity)
        frames: list[pd.DataFrame] = []
        summaries: list[dict[str, Any]] = []
        for scenario_name in scenarios:
            scenario = ScenarioBuilder().build(
                budget_total=budget_total,
                scenario=scenario_name,
                top_n=top_n,
                capacidade_operacional=capacidade_operacional,
                janela_temporal_dias=janela_temporal_dias,
            )
            current_scenario = scenario
            scenario_frame = base.copy()
            scenario_frame["tipo_recomendacao"] = scenario_frame.apply(
                lambda row, active_scenario=current_scenario: choose_action(row, scenario=active_scenario),
                axis=1,
            )
            allocated = BudgetAllocator().allocate(scenario_frame, scenario=scenario)
            allocated["scenario_id"] = scenario.name
            allocated["action_budget_target"] = allocated["tipo_recomendacao"].map(
                lambda action, active_scenario=current_scenario: active_scenario.action_budget(str(action))
            )
            allocated["canal_ideal"] = allocated.apply(self._channel_for_action, axis=1)
            allocated["mensagem_ideal"] = allocated.apply(self._message_for_action, axis=1)
            allocated["justificativa"] = allocated.apply(
                lambda row, active_scenario=current_scenario: build_justification(row, scenario=active_scenario),
                axis=1,
            )
            allocated["recomendacao_operacional"] = allocated.apply(self._operational_sentence, axis=1)
            allocated["generated_at_utc"] = datetime.now(UTC).isoformat()
            frames.append(allocated)
            summaries.append(self._scenario_summary(allocated, scenario_name=scenario.name))
        recommendations = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        root = self.paths.lakehouse_root / "serving" / "operational" / dataset_version
        root.mkdir(parents=True, exist_ok=True)
        recommendations_path = root / "operational_recommendations.parquet"
        summary_path = root / "scenario_budget_summary.json"
        pdf_path = root / "executive_report.pdf"
        workbook_path = root / "operational_workbook.xlsx"
        recommendations.to_parquet(recommendations_path, index=False)
        generated_at_utc = datetime.now(UTC).isoformat()
        summary_payload: dict[str, Any] = {
            "dataset_version": dataset_version,
            "source_scores_path": str(source_path),
            "generated_at_utc": generated_at_utc,
            "budget_total_per_scenario": float(budget_total),
            "score_granularity": score_granularity,
            "scenarios": summaries,
        }
        summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        report_scores = self._scores_for_report(base)
        report_recs = self._recommendations_for_report(recommendations)
        pdf_path.write_bytes(build_executive_pdf_bytes(scores=report_scores, recommendations=report_recs))
        workbook_path.write_bytes(
            build_operational_workbook_bytes(
                {
                    "recommendations": recommendations,
                    "scenario_summary": pd.DataFrame(summaries),
                    "top_scores": base.head(top_n),
                }
            )
        )
        return OperationalRecommendationResult(
            recommendations_path=recommendations_path,
            summary_path=summary_path,
            executive_pdf_path=pdf_path,
            workbook_path=workbook_path,
            rows=int(len(recommendations)),
            scenarios=list(scenarios),
            generated_at_utc=generated_at_utc,
        )

    def _latest_calibrated_scores(self) -> Path | None:
        roots = [self.paths.lakehouse_root, self.paths.lake_root]
        candidates: list[Path] = []
        for root in roots:
            if root.exists():
                candidates.extend(root.rglob("gold_calibrated_priority_scores.parquet"))
        return sorted(candidates, key=lambda path: path.stat().st_mtime, reverse=True)[0] if candidates else None

    def _prepare_scores(self, scores: pd.DataFrame, *, score_granularity: str) -> pd.DataFrame:
        out = scores[scores["score_granularity"].astype(str).eq(score_granularity)].copy()
        if out.empty:
            out = scores.copy()
        if "territorio_id" not in out.columns:
            out["territorio_id"] = out.get("score_record_id", pd.Series(range(len(out)), index=out.index)).astype(str)
        if "municipio" not in out.columns:
            out["municipio"] = out.get("municipio_nome", "")
        if "zona_eleitoral" not in out.columns:
            out["zona_eleitoral"] = out.get("zona", "")
        for column, default in {
            "base_eleitoral_score": 0.0,
            "afinidade_tematica_score": 0.5,
            "potencial_expansao_score": 0.0,
            "custo_eficiencia_score": 0.5,
            "concorrencia_score": 0.5,
            "score_prioridade_final": 0.0,
            "data_quality_score": 0.7,
            "join_confidence": 0.7,
        }.items():
            if column not in out.columns:
                out[column] = default
            out[column] = pd.to_numeric(out[column], errors="coerce").fillna(default)
        return out.sort_values("score_prioridade_final", ascending=False).reset_index(drop=True)

    def _scenario_summary(self, allocated: pd.DataFrame, *, scenario_name: str) -> dict[str, Any]:
        summary = build_scenario_summary(
            allocated,
            ScenarioBuilder().build(
                budget_total=float(allocated["recurso_sugerido"].sum()) if not allocated.empty else 0.0,
                scenario=scenario_name,
                top_n=max(len(allocated), 1),
                capacidade_operacional=float(allocated.get("capacidade_operacional_informada", pd.Series([0.0])).iloc[0])
                if not allocated.empty
                else 0.0,
                janela_temporal_dias=int(allocated.get("janela_temporal_dias", pd.Series([45])).iloc[0])
                if not allocated.empty
                else 45,
            ),
        )
        profile_name: ScenarioName = scenario_name if scenario_name in SCENARIO_PROFILES else "hibrido"  # type: ignore[assignment]
        summary["action_budget_profile"] = SCENARIO_PROFILES[profile_name]
        summary["avg_confidence"] = round(
            float(pd.to_numeric(allocated.get("join_confidence", 0), errors="coerce").fillna(0).mean())
            if not allocated.empty
            else 0.0,
            6,
        )
        return summary

    def _channel_for_action(self, row: pd.Series) -> str:
        action = str(row.get("tipo_recomendacao", "presenca_fisica"))
        if action in {"retencao_base", "ativacao_lideranca_local", "presenca_fisica"}:
            return "campo_liderancas_whatsapp"
        if action == "comunicacao_programatica":
            return "meta_ads_google_ads"
        if action == "reforco_area_competitiva":
            return "meta_ads_radio_local_campo"
        if action == "expansao_territorial":
            return "campo_meta_ads_testes_mensagem"
        return "campo_e_digital"

    def _message_for_action(self, row: pd.Series) -> str:
        action = str(row.get("tipo_recomendacao", "presenca_fisica"))
        if action == "retencao_base":
            return "consolidar base e reduzir abstencao"
        if action == "expansao_territorial":
            return "apresentacao do candidato com tema prioritario local"
        if action == "reforco_area_competitiva":
            return "comparativo de propostas e mobilizacao de indecisos agregados"
        if action == "comunicacao_programatica":
            return "mensagem tematica segmentada por territorio agregado"
        if action == "ativacao_lideranca_local":
            return "liderancas locais e presenca territorial"
        return "presenca fisica e lembranca de voto"

    def _operational_sentence(self, row: pd.Series) -> str:
        return (
            f"Aplicar R$ {float(row.get('recurso_sugerido', 0.0)):,.0f} em "
            f"{row.get('municipio', row.get('municipio_nome', 'territorio'))} zona {row.get('zona_eleitoral', row.get('zona', ''))}, "
            f"priorizando {row.get('canal_ideal')} com mensagem: {row.get('mensagem_ideal')}."
        )

    def _scores_for_report(self, scores: pd.DataFrame) -> pd.DataFrame:
        out = pd.DataFrame(
            {
                "ranking": range(1, len(scores) + 1),
                "municipio_id_ibge7": scores.get("cod_municipio_ibge", ""),
                "municipio": scores.get("municipio", scores.get("municipio_nome", "")),
                "score_alocacao": pd.to_numeric(scores["score_prioridade_final"], errors="coerce").fillna(0.0) * 100,
                "score_potencial_eleitoral": scores["base_eleitoral_score"],
                "score_oportunidade": scores["potencial_expansao_score"],
                "score_eficiencia_midia": scores["afinidade_tematica_score"],
                "score_custo": scores["custo_eficiencia_score"],
                "score_risco": scores["concorrencia_score"],
                "data_quality_score": scores.get("data_quality_score", 0.0),
                "join_confidence": scores.get("join_confidence", 0.0),
            }
        )
        return out

    def _recommendations_for_report(self, recommendations: pd.DataFrame) -> pd.DataFrame:
        if recommendations.empty:
            return pd.DataFrame()
        return pd.DataFrame(
            {
                "ranking": range(1, len(recommendations) + 1),
                "municipio_id_ibge7": recommendations.get("cod_municipio_ibge", ""),
                "verba_sugerida": recommendations["recurso_sugerido"],
                "canal_ideal": recommendations["canal_ideal"],
                "mensagem_ideal": recommendations["mensagem_ideal"],
                "justificativa": recommendations["justificativa"],
            }
        )
