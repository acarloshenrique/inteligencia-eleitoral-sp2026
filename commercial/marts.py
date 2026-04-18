from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pandas as pd


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def first_existing(df: pd.DataFrame, candidates: list[str]) -> str | None:
    normalized = {str(column).lower(): str(column) for column in df.columns}
    for candidate in candidates:
        if candidate.lower() in normalized:
            return normalized[candidate.lower()]
    return None


def series_first(df: pd.DataFrame, candidates: list[str], default: Any = "") -> pd.Series:
    column = first_existing(df, candidates)
    if column is None:
        return pd.Series([default] * len(df), index=df.index)
    return df[column]


def numeric(series: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(default)


@dataclass(frozen=True)
class CommercialMartResult:
    marts: dict[str, pd.DataFrame]
    generated_at_utc: str


class CommercialMartBuilder:
    def build(
        self,
        *,
        tenant_id: str,
        campaign_id: str,
        snapshot_id: str,
        gold_tables: dict[str, pd.DataFrame],
    ) -> CommercialMartResult:
        generated_at = utc_now_iso()
        demo = self.commercial_demo_summary(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            gold_tables=gold_tables,
        )
        premium = self.premium_report_tables(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            gold_tables=gold_tables,
        )
        pitch = self.pitch_metrics(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            gold_tables=gold_tables,
        )
        return CommercialMartResult(
            marts={
                "commercial_demo_summary": demo,
                "premium_report_tables": premium,
                "commercial_pitch_metrics": pitch,
            },
            generated_at_utc=generated_at,
        )

    def commercial_demo_summary(
        self,
        *,
        tenant_id: str,
        campaign_id: str,
        snapshot_id: str,
        gold_tables: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        priority = gold_tables.get("gold_priority_score", pd.DataFrame())
        recommendations = gold_tables.get("gold_allocation_recommendations", pd.DataFrame())
        health = gold_tables.get("lake_health_report", pd.DataFrame())
        rows: list[dict[str, Any]] = []
        if not priority.empty:
            top = priority.sort_values("score_prioridade_final", ascending=False).head(10)
            for rank, (_, row) in enumerate(top.iterrows(), start=1):
                rows.append(
                    {
                        "tenant_id": tenant_id,
                        "campaign_id": campaign_id,
                        "snapshot_id": snapshot_id,
                        "rank": rank,
                        "candidate_id": row.get("candidate_id", ""),
                        "territorio_id": row.get("territorio_id", ""),
                        "municipio_nome": row.get("municipio_nome", ""),
                        "zona": row.get("zona", ""),
                        "score_prioridade_final": float(row.get("score_prioridade_final", 0.0)),
                        "demo_message": f"Territorio priorizado com score {float(row.get('score_prioridade_final', 0.0)):.2f}.",
                    }
                )
        if recommendations.empty and not rows:
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "campaign_id": campaign_id,
                    "snapshot_id": snapshot_id,
                    "rank": 0,
                    "candidate_id": "",
                    "territorio_id": "",
                    "municipio_nome": "",
                    "zona": "",
                    "score_prioridade_final": 0.0,
                    "demo_message": "Not found in repo: gold_priority_score/gold_allocation_recommendations.",
                }
            )
        demo = pd.DataFrame(rows)
        demo["has_recommendations"] = not recommendations.empty
        demo["lake_quality_available"] = not health.empty
        return demo

    def premium_report_tables(
        self,
        *,
        tenant_id: str,
        campaign_id: str,
        snapshot_id: str,
        gold_tables: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        recommendations = gold_tables.get("gold_allocation_recommendations", pd.DataFrame())
        priority = gold_tables.get("gold_priority_score", pd.DataFrame())
        if recommendations.empty:
            recommendations = priority.copy()
            recommendations["recurso_sugerido"] = 0.0
            recommendations["tipo_acao_sugerida"] = recommendations.get("tipo_acao_sugerida", "presenca_fisica")
            recommendations["justificativa"] = recommendations.get("score_explanation", "")
        if recommendations.empty:
            return pd.DataFrame(
                [
                    {
                        "tenant_id": tenant_id,
                        "campaign_id": campaign_id,
                        "snapshot_id": snapshot_id,
                        "status": "Not found in repo",
                    }
                ]
            )
        out = recommendations.copy()
        out["tenant_id"] = tenant_id
        out["campaign_id"] = campaign_id
        out["snapshot_id"] = snapshot_id
        keep = [
            "tenant_id",
            "campaign_id",
            "snapshot_id",
            "scenario_id",
            "candidate_id",
            "territorio_id",
            "tipo_acao_sugerida",
            "score_prioridade_final",
            "recurso_sugerido",
            "percentual_orcamento_sugerido",
            "justificativa",
        ]
        return out[[column for column in keep if column in out.columns]].copy()

    def pitch_metrics(
        self,
        *,
        tenant_id: str,
        campaign_id: str,
        snapshot_id: str,
        gold_tables: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        priority = gold_tables.get("gold_priority_score", pd.DataFrame())
        master = gold_tables.get("gold_territorial_electoral_master_index", pd.DataFrame())
        quality = gold_tables.get("lake_health_report", pd.DataFrame())
        recommendations = gold_tables.get("gold_allocation_recommendations", pd.DataFrame())
        metrics = {
            "tenant_id": tenant_id,
            "campaign_id": campaign_id,
            "snapshot_id": snapshot_id,
            "candidates_supported": int(priority["candidate_id"].nunique()) if "candidate_id" in priority else 0,
            "territories_ranked": int(priority["territorio_id"].nunique()) if "territorio_id" in priority else 0,
            "master_records": int(len(master)) if not master.empty else 0,
            "recommendations_generated": int(len(recommendations)) if not recommendations.empty else 0,
            "avg_priority_score": float(numeric(series_first(priority, ["score_prioridade_final"], 0)).mean())
            if not priority.empty
            else 0.0,
            "avg_join_confidence": float(numeric(series_first(master, ["join_confidence"], 0)).mean())
            if not master.empty
            else 0.0,
            "lake_quality_score": float(numeric(series_first(quality, ["aggregate_quality_score"], 0)).mean())
            if not quality.empty
            else 0.0,
        }
        return pd.DataFrame([metrics])
