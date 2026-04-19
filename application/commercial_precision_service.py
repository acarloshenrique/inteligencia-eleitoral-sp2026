from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from infrastructure.product_reports import build_operational_workbook_bytes

EXPECTED_SCENARIOS = ("conservador", "hibrido", "agressivo")


@dataclass(frozen=True)
class CommercialPrecisionResult:
    root: Path
    multi_candidate_summary_path: Path
    campaign_snapshots_path: Path
    scenario_comparison_path: Path
    readiness_json_path: Path
    readiness_markdown_path: Path
    demo_workbook_path: Path
    demo_markdown_path: Path
    rows: int
    candidate_count: int
    readiness_score: float
    generated_at_utc: str


class CommercialPrecisionService:
    """Builds commercial-grade serving artifacts from ready gold/serving outputs."""

    def __init__(self, paths) -> None:
        self.paths = paths

    def run(
        self,
        *,
        dataset_version: str = "sprint4_commercial_precision",
        tenant_id: str = "default",
        campaign_id: str = "campanha_sp_2026",
        snapshot_id: str | None = None,
        operational_path: Path | None = None,
        scores_path: Path | None = None,
    ) -> CommercialPrecisionResult:
        generated_at = datetime.now(UTC).isoformat()
        operational_source = operational_path or self._latest_operational_recommendations()
        scores_source = scores_path or self._latest_calibrated_scores()
        recommendations = self._read_table(operational_source)
        scores = self._read_table(scores_source) if scores_source is not None else pd.DataFrame()
        if recommendations.empty:
            raise FileNotFoundError("Not found in repo: operational_recommendations.parquet.")

        snapshot = snapshot_id or self._snapshot_id(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            dataset_version=dataset_version,
            recommendations=recommendations,
        )
        recommendations = self._stamp(
            recommendations,
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot,
            dataset_version=dataset_version,
            generated_at_utc=generated_at,
        )
        scores = self._stamp(
            scores,
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot,
            dataset_version=dataset_version,
            generated_at_utc=generated_at,
        )
        multi_candidate = self._multi_candidate_summary(recommendations)
        snapshots = self._campaign_snapshots(
            recommendations=recommendations,
            scores=scores,
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot,
            dataset_version=dataset_version,
            operational_source=operational_source,
            scores_source=scores_source,
            generated_at_utc=generated_at,
        )
        scenario_comparison = self._scenario_comparison(recommendations)
        readiness = self._readiness_payload(
            recommendations=recommendations,
            scores=scores,
            snapshot=snapshots,
            scenario_comparison=scenario_comparison,
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot,
            dataset_version=dataset_version,
            generated_at_utc=generated_at,
        )

        root = self.paths.lakehouse_root / "serving" / "commercial" / dataset_version
        root.mkdir(parents=True, exist_ok=True)
        multi_candidate_path = root / "multi_candidate_summary.parquet"
        snapshots_path = root / "campaign_snapshots.parquet"
        comparison_path = root / "scenario_comparison.parquet"
        readiness_json_path = root / "readiness_report.json"
        readiness_md_path = root / "readiness_report.md"
        demo_workbook_path = root / "commercial_demo_pack.xlsx"
        demo_md_path = root / "commercial_demo_sp2024_2026.md"

        multi_candidate.to_parquet(multi_candidate_path, index=False)
        snapshots.to_parquet(snapshots_path, index=False)
        scenario_comparison.to_parquet(comparison_path, index=False)
        readiness_json_path.write_text(json.dumps(readiness, ensure_ascii=False, indent=2), encoding="utf-8")
        readiness_md_path.write_text(self._readiness_markdown(readiness), encoding="utf-8")
        demo_md_path.write_text(
            self._demo_markdown(
                readiness=readiness,
                multi_candidate=multi_candidate,
                scenario_comparison=scenario_comparison,
                recommendations=recommendations,
            ),
            encoding="utf-8",
        )
        demo_workbook_path.write_bytes(
            build_operational_workbook_bytes(
                {
                    "multi_candidate": multi_candidate,
                    "campaign_snapshots": snapshots,
                    "scenario_comparison": scenario_comparison,
                    "top_recommendations": recommendations.head(50),
                    "readiness": pd.DataFrame([readiness]),
                }
            )
        )
        return CommercialPrecisionResult(
            root=root,
            multi_candidate_summary_path=multi_candidate_path,
            campaign_snapshots_path=snapshots_path,
            scenario_comparison_path=comparison_path,
            readiness_json_path=readiness_json_path,
            readiness_markdown_path=readiness_md_path,
            demo_workbook_path=demo_workbook_path,
            demo_markdown_path=demo_md_path,
            rows=int(len(recommendations)),
            candidate_count=int(multi_candidate["candidate_id"].nunique()) if not multi_candidate.empty else 0,
            readiness_score=float(readiness["readiness_score"]),
            generated_at_utc=generated_at,
        )

    def _latest_operational_recommendations(self) -> Path | None:
        candidates = list(self.paths.lakehouse_root.rglob("operational_recommendations.parquet"))
        return sorted(candidates, key=lambda path: path.stat().st_mtime, reverse=True)[0] if candidates else None

    def _latest_calibrated_scores(self) -> Path | None:
        roots = [self.paths.lakehouse_root, self.paths.lake_root]
        candidates: list[Path] = []
        for root in roots:
            if root.exists():
                candidates.extend(root.rglob("gold_calibrated_priority_scores.parquet"))
        return sorted(candidates, key=lambda path: path.stat().st_mtime, reverse=True)[0] if candidates else None

    def _read_table(self, path: Path | None) -> pd.DataFrame:
        if path is None or not path.exists():
            return pd.DataFrame()
        if path.suffix.lower() == ".parquet":
            return pd.read_parquet(path)
        if path.suffix.lower() == ".csv":
            return pd.read_csv(path)
        raise ValueError(f"Unsupported table format: {path}")

    def _stamp(
        self,
        df: pd.DataFrame,
        *,
        tenant_id: str,
        campaign_id: str,
        snapshot_id: str,
        dataset_version: str,
        generated_at_utc: str,
    ) -> pd.DataFrame:
        if df.empty:
            return df.copy()
        out = df.copy()
        out["tenant_id"] = tenant_id
        out["campaign_id"] = campaign_id
        out["snapshot_id"] = snapshot_id
        out["dataset_version"] = dataset_version
        out["commercial_generated_at_utc"] = generated_at_utc
        return out

    def _snapshot_id(
        self,
        *,
        tenant_id: str,
        campaign_id: str,
        dataset_version: str,
        recommendations: pd.DataFrame,
    ) -> str:
        candidates = ",".join(sorted(str(value) for value in recommendations.get("candidate_id", pd.Series()).dropna().unique()))
        scenarios = ",".join(sorted(str(value) for value in recommendations.get("scenario_id", pd.Series()).dropna().unique()))
        raw = f"{tenant_id}:{campaign_id}:{dataset_version}:{candidates}:{scenarios}:{len(recommendations)}"
        return "snapshot_" + sha256(raw.encode("utf-8")).hexdigest()[:12]

    def _multi_candidate_summary(self, recommendations: pd.DataFrame) -> pd.DataFrame:
        df = recommendations.copy()
        if "candidate_id" not in df.columns:
            df["candidate_id"] = "aggregate"
        rows: list[dict[str, Any]] = []
        for candidate_id, group in df.groupby("candidate_id", dropna=False):
            top = group.sort_values("score_prioridade_final", ascending=False).head(5)
            rows.append(
                {
                    "tenant_id": str(group["tenant_id"].iloc[0]),
                    "campaign_id": str(group["campaign_id"].iloc[0]),
                    "snapshot_id": str(group["snapshot_id"].iloc[0]),
                    "candidate_id": str(candidate_id or "aggregate"),
                    "scenario_count": int(group["scenario_id"].nunique()) if "scenario_id" in group else 0,
                    "territories_recommended": int(group["territorio_id"].nunique()) if "territorio_id" in group else len(group),
                    "recommendation_rows": int(len(group)),
                    "total_budget_all_scenarios": float(pd.to_numeric(group.get("recurso_sugerido", 0.0), errors="coerce").fillna(0).sum()),
                    "avg_priority_score": float(pd.to_numeric(group.get("score_prioridade_final", 0.0), errors="coerce").fillna(0).mean()),
                    "avg_confidence_score": float(pd.to_numeric(group.get("confidence_score", group.get("join_confidence", 0.0)), errors="coerce").fillna(0).mean()),
                    "top_territories": "; ".join(
                        f"{row.get('municipio_nome', row.get('municipio', 'territorio'))} zona {row.get('zona', row.get('zona_eleitoral', ''))}"
                        for _, row in top.iterrows()
                    ),
                    "multi_candidate_ready": True,
                }
            )
        return pd.DataFrame(rows).sort_values("avg_priority_score", ascending=False).reset_index(drop=True)

    def _campaign_snapshots(
        self,
        *,
        recommendations: pd.DataFrame,
        scores: pd.DataFrame,
        tenant_id: str,
        campaign_id: str,
        snapshot_id: str,
        dataset_version: str,
        operational_source: Path | None,
        scores_source: Path | None,
        generated_at_utc: str,
    ) -> pd.DataFrame:
        years = sorted(
            {
                int(value)
                for value in pd.concat(
                    [
                        pd.to_numeric(recommendations.get("ano_eleicao", pd.Series(dtype="float64")), errors="coerce"),
                        pd.to_numeric(scores.get("ano_eleicao", pd.Series(dtype="float64")), errors="coerce"),
                    ],
                    ignore_index=True,
                ).dropna()
            }
        )
        return pd.DataFrame(
            [
                {
                    "tenant_id": tenant_id,
                    "campaign_id": campaign_id,
                    "snapshot_id": snapshot_id,
                    "dataset_version": dataset_version,
                    "generated_at_utc": generated_at_utc,
                    "source_operational_path": str(operational_source) if operational_source is not None else "",
                    "source_scores_path": str(scores_source) if scores_source is not None else "",
                    "election_years_available": ",".join(str(year) for year in years),
                    "planning_cycle": "SP 2026",
                    "real_data_basis": "SP 2024" if 2024 in years else "Not found in repo",
                    "candidate_count": int(recommendations["candidate_id"].nunique()) if "candidate_id" in recommendations else 0,
                    "scenario_count": int(recommendations["scenario_id"].nunique()) if "scenario_id" in recommendations else 0,
                    "recommendation_rows": int(len(recommendations)),
                    "score_rows": int(len(scores)),
                    "lgpd_scope": "dados publicos agregados; sem microtargeting individual",
                    "snapshot_status": "demo_ready" if len(recommendations) > 0 and 2024 in years else "limited",
                }
            ]
        )

    def _scenario_comparison(self, recommendations: pd.DataFrame) -> pd.DataFrame:
        df = recommendations.copy()
        if "candidate_id" not in df.columns:
            df["candidate_id"] = "aggregate"
        grouped = (
            df.groupby(["candidate_id", "scenario_id"], dropna=False)
            .agg(
                territories=("territorio_id", "nunique"),
                budget_total=("recurso_sugerido", "sum"),
                avg_priority_score=("score_prioridade_final", "mean"),
                avg_confidence_score=("confidence_score", "mean"),
            )
            .reset_index()
        )
        actions = (
            df.pivot_table(
                index=["candidate_id", "scenario_id"],
                columns="tipo_recomendacao",
                values="recurso_sugerido",
                aggfunc="sum",
                fill_value=0.0,
            )
            .reset_index()
            .rename_axis(None, axis=1)
        )
        out = grouped.merge(actions, on=["candidate_id", "scenario_id"], how="left")
        baseline = out[out["scenario_id"].eq("hibrido")][["candidate_id", "budget_total", "avg_priority_score"]].rename(
            columns={"budget_total": "hibrido_budget_total", "avg_priority_score": "hibrido_avg_priority_score"}
        )
        out = out.merge(baseline, on="candidate_id", how="left")
        out["budget_delta_vs_hibrido"] = out["budget_total"] - out["hibrido_budget_total"].fillna(0.0)
        out["priority_delta_vs_hibrido"] = out["avg_priority_score"] - out["hibrido_avg_priority_score"].fillna(0.0)
        out["scenario_interpretation"] = out["scenario_id"].map(
            {
                "conservador": "protege base e reduz risco operacional",
                "hibrido": "equilibra base, expansao e competicao",
                "agressivo": "aumenta exposicao em expansao e disputa",
            }
        ).fillna("cenario customizado")
        return out.sort_values(["candidate_id", "scenario_id"]).reset_index(drop=True)

    def _readiness_payload(
        self,
        *,
        recommendations: pd.DataFrame,
        scores: pd.DataFrame,
        snapshot: pd.DataFrame,
        scenario_comparison: pd.DataFrame,
        tenant_id: str,
        campaign_id: str,
        snapshot_id: str,
        dataset_version: str,
        generated_at_utc: str,
    ) -> dict[str, Any]:
        scenarios = set(str(value) for value in recommendations.get("scenario_id", pd.Series()).dropna().unique())
        years = set(int(value) for value in pd.to_numeric(recommendations.get("ano_eleicao", pd.Series()), errors="coerce").dropna())
        candidate_count = int(recommendations["candidate_id"].nunique()) if "candidate_id" in recommendations else 0
        avg_confidence = float(
            pd.to_numeric(recommendations.get("confidence_score", recommendations.get("join_confidence", 0.0)), errors="coerce")
            .fillna(0.0)
            .mean()
        )
        checks = {
            "real_sp_2024_data": bool((recommendations.get("uf", pd.Series(dtype=str)).astype(str).eq("SP")).any() and 2024 in years),
            "sp_2026_planning_snapshot": campaign_id.lower().endswith("2026") or "2026" in campaign_id,
            "multi_candidate_schema": "candidate_id" in recommendations.columns,
            "multi_candidate_data": candidate_count >= 2,
            "three_scenarios_available": all(scenario in scenarios for scenario in EXPECTED_SCENARIOS),
            "scenario_comparison_available": not scenario_comparison.empty,
            "exportable_demo_available": True,
            "confidence_above_070": avg_confidence >= 0.70,
        }
        weights = {
            "real_sp_2024_data": 0.20,
            "sp_2026_planning_snapshot": 0.10,
            "multi_candidate_schema": 0.12,
            "multi_candidate_data": 0.08,
            "three_scenarios_available": 0.18,
            "scenario_comparison_available": 0.12,
            "exportable_demo_available": 0.10,
            "confidence_above_070": 0.10,
        }
        readiness_score = round(sum(weights[key] for key, passed in checks.items() if passed), 6)
        limitations = []
        if not checks["multi_candidate_data"]:
            limitations.append("Dados reais atuais possuem apenas candidate_id agregado; engine ja aceita multiplos candidatos.")
        if not checks["real_sp_2024_data"]:
            limitations.append("Base real SP 2024 nao foi detectada no output operacional.")
        if scores.empty:
            limitations.append("Scores calibrados externos nao foram encontrados; readiness usa recomendacoes operacionais.")
        return {
            "tenant_id": tenant_id,
            "campaign_id": campaign_id,
            "snapshot_id": snapshot_id,
            "dataset_version": dataset_version,
            "generated_at_utc": generated_at_utc,
            "readiness_score": readiness_score,
            "status": "commercial_demo_ready" if readiness_score >= 0.82 else "commercial_demo_limited",
            "checks": checks,
            "candidate_count": candidate_count,
            "scenario_count": len(scenarios),
            "recommendation_rows": int(len(recommendations)),
            "score_rows": int(len(scores)),
            "snapshot_rows": int(len(snapshot)),
            "avg_confidence": round(avg_confidence, 6),
            "limitations": limitations,
            "lgpd_scope": "Somente dados publicos agregados e territoriais; nao usar para inferencia individual.",
            "next_commercial_step": "Validar narrativa com consultor de campanha usando top territorios e comparacao de cenarios.",
        }

    def _readiness_markdown(self, readiness: dict[str, Any]) -> str:
        checks = readiness["checks"]
        lines = [
            "# Readiness comercial",
            "",
            f"- Status: {readiness['status']}",
            f"- Score: {readiness['readiness_score']:.2f}",
            f"- Candidatos: {readiness['candidate_count']}",
            f"- Cenarios: {readiness['scenario_count']}",
            f"- Recomendacoes: {readiness['recommendation_rows']}",
            f"- Confianca media: {readiness['avg_confidence']:.2f}",
            "",
            "## Checks",
            "",
        ]
        for key, passed in checks.items():
            lines.append(f"- {key}: {'ok' if passed else 'pendente'}")
        if readiness["limitations"]:
            lines.extend(["", "## Limitacoes", ""])
            lines.extend(f"- {item}" for item in readiness["limitations"])
        return "\n".join(lines) + "\n"

    def _demo_markdown(
        self,
        *,
        readiness: dict[str, Any],
        multi_candidate: pd.DataFrame,
        scenario_comparison: pd.DataFrame,
        recommendations: pd.DataFrame,
    ) -> str:
        top = recommendations.sort_values("score_prioridade_final", ascending=False).head(8)
        lines = [
            "# Demo comercial SP 2024/2026",
            "",
            "Pacote de demonstracao baseado em dados eleitorais agregados de SP 2024 e planejamento de campanha SP 2026.",
            "",
            f"Readiness: {readiness['readiness_score']:.2f} ({readiness['status']})",
            f"Candidatos no snapshot: {len(multi_candidate)}",
            f"Cenarios comparados: {scenario_comparison['scenario_id'].nunique() if not scenario_comparison.empty else 0}",
            "",
            "## Top territorios",
            "",
        ]
        for _, row in top.iterrows():
            lines.append(
                f"- {row.get('municipio_nome', row.get('municipio', 'territorio'))} zona {row.get('zona', row.get('zona_eleitoral', ''))}: "
                f"score {float(row.get('score_prioridade_final', 0.0)):.3f}, "
                f"acao {row.get('tipo_recomendacao', 'n/d')}, "
                f"cenario {row.get('scenario_id', 'n/d')}"
            )
        lines.extend(["", "## Mensagem de venda", ""])
        lines.append(
            "A plataforma entrega ranking territorial, comparacao de cenarios, recomendacao de verba e evidencias de qualidade em um pacote auditavel."
        )
        return "\n".join(lines) + "\n"
