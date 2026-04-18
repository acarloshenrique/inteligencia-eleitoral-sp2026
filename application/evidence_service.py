from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pandas as pd

from domain.decision_models import EvidenceRecord


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


class EvidenceService:
    def build_metric_evidence(
        self,
        *,
        territorio_id: str,
        dataset: str,
        descricao: str,
        fonte: str = "data_lake_gold",
        entidade_origem: str = "territorio",
        chave_registro: str | None = None,
    ) -> EvidenceRecord:
        return EvidenceRecord(
            evidence_id=str(uuid4()),
            entidade_origem=entidade_origem,
            fonte=fonte,
            dataset=dataset,
            chave_registro=chave_registro or territorio_id,
            descricao=descricao,
            timestamp_ingestao=datetime.now(UTC),
        )

    def build_recommendation_evidence(self, row: pd.Series, *, territorio_id: str) -> list[EvidenceRecord]:
        evidences = [
            self.build_metric_evidence(
                territorio_id=territorio_id,
                dataset="territorial_priority_scores",
                descricao=str(row.get("score_explanation", "Score de prioridade territorial calculado.")),
            ),
            self.build_metric_evidence(
                territorio_id=territorio_id,
                dataset="allocation_scenario",
                descricao=(
                    f"Cenario {row.get('cenario', 'hibrido')} sugeriu R$ {float(row.get('recurso_sugerido', 0.0) or 0.0):,.0f} "
                    f"para acao {row.get('tipo_recomendacao', 'presenca_fisica')}."
                ),
            ),
        ]
        if any(str(row.get(col, "")).strip() for col in ["source_name", "ingestion_run_id", "lake_layer"]):
            evidences.append(
                self.build_metric_evidence(
                    territorio_id=territorio_id,
                    dataset=str(row.get("source_name") or "territorial_data_lake"),
                    descricao=(
                        f"Proveniencia lake_layer={row.get('lake_layer', 'gold')}; "
                        f"run_id={row.get('ingestion_run_id', 'nao_informado')}."
                    ),
                    fonte="data_lake_lineage",
                )
            )
        if "data_quality_score" in row or "join_confidence" in row:
            evidences.append(
                self.build_metric_evidence(
                    territorio_id=territorio_id,
                    dataset="data_quality_join_confidence",
                    descricao=(
                        f"Qualidade={float(row.get('data_quality_score', 0.8) or 0.8):.2f}; "
                        f"join_confidence={float(row.get('join_confidence', 0.8) or 0.8):.2f}."
                    ),
                    fonte="data_governance",
                )
            )
        return evidences

    def confidence_from_evidence(
        self,
        evidences: list[EvidenceRecord],
        *,
        data_quality_score: float = 0.8,
        join_confidence: float = 0.8,
        score_completeness: float = 1.0,
    ) -> float:
        evidence_factor = min(1.0, len(evidences) / 4.0)
        return _clamp01(
            0.40 * _clamp01(data_quality_score)
            + 0.25 * _clamp01(join_confidence)
            + 0.20 * evidence_factor
            + 0.15 * _clamp01(score_completeness)
        )
