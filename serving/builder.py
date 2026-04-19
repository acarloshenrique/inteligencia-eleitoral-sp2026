from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from serving.models import ServingOutputSpec


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def numeric(series: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(default)


def first_existing(df: pd.DataFrame, aliases: list[str]) -> str | None:
    columns = {str(column).lower(): str(column) for column in df.columns}
    for alias in aliases:
        if alias.lower() in columns:
            return columns[alias.lower()]
    return None


def series_first(df: pd.DataFrame, aliases: list[str], default: Any = "") -> pd.Series:
    column = first_existing(df, aliases)
    if column is None:
        return pd.Series([default] * len(df), index=df.index)
    return df[column]


def first_non_empty(values: pd.Series) -> str:
    for value in values:
        text = str(value or "").strip()
        if text and text.lower() != "nan":
            return text
    return ""


SERVING_OUTPUT_SPECS: dict[str, ServingOutputSpec] = {
    "serving_territory_ranking": ServingOutputSpec(
        output_id="serving_territory_ranking",
        description="Ranking territorial pronto para API/UI, com score, territorio, acao e confianca.",
        grain="tenant_id + campaign_id + snapshot_id + candidate_id + territorio_id",
        primary_key=["tenant_id", "campaign_id", "snapshot_id", "candidate_id", "territorio_id"],
        source_tables=["gold_priority_score", "gold_territory_profile", "gold_allocation_recommendations"],
        audiences=["api", "ui", "commercial_demo"],
        quality_rules=["score_prioridade_final entre 0 e 1", "candidate_id e territorio_id obrigatorios"],
        join_limitations=["Granularidade de setor censitario depende de cd_setor/geocoding no master index."],
    ),
    "serving_allocation_recommendations": ServingOutputSpec(
        output_id="serving_allocation_recommendations",
        description="Recomendacoes finais de alocacao para API, UI, relatorio e motor de recomendacao.",
        grain="tenant_id + campaign_id + snapshot_id + scenario_id + candidate_id + territorio_id",
        primary_key=["tenant_id", "campaign_id", "snapshot_id", "scenario_id", "candidate_id", "territorio_id"],
        source_tables=["gold_allocation_recommendations", "gold_priority_score"],
        audiences=["api", "ui", "report", "recommendation_engine"],
        quality_rules=["recurso_sugerido nao negativo", "justificativa nao vazia", "sem dados pessoais de eleitores"],
        lgpd_classification="campaign_operational_confidential",
        join_limitations=["ROI politico e estimativo; nao representa inferencia individual de preferencia politica."],
    ),
    "serving_data_readiness": ServingOutputSpec(
        output_id="serving_data_readiness",
        description="Resumo operacional de cobertura, qualidade e prontidao comercial do lake.",
        grain="tenant_id + campaign_id + snapshot_id",
        primary_key=["tenant_id", "campaign_id", "snapshot_id"],
        source_tables=[
            "lake_health_report",
            "gold_territorial_electoral_master_index",
            "gold_priority_score",
            "gold_allocation_recommendations",
        ],
        audiences=["api", "ui", "report", "commercial_demo"],
        formats=["parquet", "json", "csv"],
        quality_rules=["readiness_score entre 0 e 1", "limitacoes de join explicitadas"],
        join_limitations=["Campos ausentes sao reportados como lacunas, nao inferidos silenciosamente."],
    ),
    "serving_zone_ranking": ServingOutputSpec(
        output_id="serving_zone_ranking",
        description="Ranking de zonas eleitorais pronto para API/UI, com prioridade, disputa e confianca.",
        grain="tenant_id + campaign_id + snapshot_id + candidate_id + zona_id",
        primary_key=["tenant_id", "campaign_id", "snapshot_id", "candidate_id", "zona_id"],
        source_tables=["gold_zone_priority_score", "gold_section_master_index_quality"],
        audiences=["api", "ui", "commercial_demo"],
        quality_rules=["score_prioridade_final entre 0 e 1", "zona_id obrigatorio"],
        join_limitations=["Zonas sem secao/local herdam limitacoes documentadas no master index."],
    ),
    "serving_municipality_zone_detail": ServingOutputSpec(
        output_id="serving_municipality_zone_detail",
        description="Detalhe Municipio -> Zonas para analise operacional e explicabilidade territorial.",
        grain="tenant_id + campaign_id + snapshot_id + candidate_id + municipio_nome + zona",
        primary_key=["tenant_id", "campaign_id", "snapshot_id", "candidate_id", "municipio_nome", "zona"],
        source_tables=["gold_zone_priority_score", "gold_section_master_index_quality"],
        audiences=["api", "ui", "report"],
        quality_rules=["municipio_nome e zona obrigatorios", "limites de join expostos"],
        join_limitations=["Nao representa microtargeting individual; leitura agregada por zona/secao."],
    ),
}


@dataclass(frozen=True)
class ServingBuildResult:
    outputs: dict[str, pd.DataFrame]
    specs: dict[str, ServingOutputSpec]
    generated_at_utc: str
    warnings: list[str]


class ServingLayerBuilder:
    def build(
        self,
        *,
        tenant_id: str,
        campaign_id: str,
        snapshot_id: str,
        dataset_version: str,
        tables: dict[str, pd.DataFrame],
    ) -> ServingBuildResult:
        generated_at = utc_now_iso()
        warnings: list[str] = []
        ranking = self.territory_ranking(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            dataset_version=dataset_version,
            tables=tables,
            generated_at_utc=generated_at,
            warnings=warnings,
        )
        recommendations = self.allocation_recommendations(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            dataset_version=dataset_version,
            tables=tables,
            generated_at_utc=generated_at,
            warnings=warnings,
        )
        readiness = self.data_readiness(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            dataset_version=dataset_version,
            tables=tables,
            generated_at_utc=generated_at,
            warnings=warnings,
        )
        zone_ranking = self.zone_ranking(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            dataset_version=dataset_version,
            tables=tables,
            generated_at_utc=generated_at,
            warnings=warnings,
        )
        municipality_zone_detail = self.municipality_zone_detail(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            dataset_version=dataset_version,
            tables=tables,
            generated_at_utc=generated_at,
            warnings=warnings,
        )
        return ServingBuildResult(
            outputs={
                "serving_territory_ranking": ranking,
                "serving_allocation_recommendations": recommendations,
                "serving_data_readiness": readiness,
                "serving_zone_ranking": zone_ranking,
                "serving_municipality_zone_detail": municipality_zone_detail,
            },
            specs=SERVING_OUTPUT_SPECS,
            generated_at_utc=generated_at,
            warnings=warnings,
        )

    def territory_ranking(
        self,
        *,
        tenant_id: str,
        campaign_id: str,
        snapshot_id: str,
        dataset_version: str,
        tables: dict[str, pd.DataFrame],
        generated_at_utc: str,
        warnings: list[str],
    ) -> pd.DataFrame:
        priority = tables.get("gold_priority_score", pd.DataFrame())
        if priority.empty:
            warnings.append("Not found in repo: gold_priority_score; serving_territory_ranking vazio.")
            return self._empty_status(tenant_id, campaign_id, snapshot_id, dataset_version, generated_at_utc)
        out = priority.copy()
        territory = tables.get("gold_territory_profile", pd.DataFrame())
        if not territory.empty and "territorio_id" in out.columns and "territorio_id" in territory.columns:
            enrich_columns = [
                column
                for column in [
                    "territorio_id",
                    "municipio_nome",
                    "uf",
                    "zona",
                    "secao",
                    "local_votacao",
                    "data_quality_score",
                ]
                if column in territory.columns and (column == "territorio_id" or column not in out.columns)
            ]
            if len(enrich_columns) > 1:
                out = out.merge(
                    territory[enrich_columns].drop_duplicates("territorio_id"), on="territorio_id", how="left"
                )
        out = self._stamp(out, tenant_id, campaign_id, snapshot_id, dataset_version, generated_at_utc)
        out["rank"] = (
            out.groupby("candidate_id", dropna=False)["score_prioridade_final"]
            .rank(method="first", ascending=False)
            .astype(int)
            if "candidate_id" in out.columns and "score_prioridade_final" in out.columns
            else range(1, len(out) + 1)
        )
        out["confidence_score"] = self._confidence(out)
        keep = [
            "tenant_id",
            "campaign_id",
            "snapshot_id",
            "dataset_version",
            "generated_at_utc",
            "rank",
            "candidate_id",
            "territorio_id",
            "uf",
            "municipio_nome",
            "zona",
            "secao",
            "local_votacao",
            "territorial_cluster_id",
            "score_prioridade_final",
            "confidence_score",
            "score_explanation",
            "join_confidence",
            "data_quality_score",
        ]
        return out[[column for column in keep if column in out.columns]].sort_values(
            ["candidate_id", "rank"], na_position="last"
        )

    def allocation_recommendations(
        self,
        *,
        tenant_id: str,
        campaign_id: str,
        snapshot_id: str,
        dataset_version: str,
        tables: dict[str, pd.DataFrame],
        generated_at_utc: str,
        warnings: list[str],
    ) -> pd.DataFrame:
        recs = tables.get("gold_allocation_recommendations", pd.DataFrame())
        if recs.empty:
            warnings.append(
                "Not found in repo: gold_allocation_recommendations; usando gold_priority_score como fallback."
            )
            recs = tables.get("gold_priority_score", pd.DataFrame()).copy()
            if recs.empty:
                return self._empty_status(tenant_id, campaign_id, snapshot_id, dataset_version, generated_at_utc)
            recs["scenario_id"] = "baseline"
            recs["tipo_acao_sugerida"] = "presenca_fisica"
            recs["recurso_sugerido"] = 0.0
            recs["percentual_orcamento_sugerido"] = 0.0
            recs["justificativa"] = series_first(recs, ["score_explanation"], "Sem recomendacao gold disponivel.")
        out = self._stamp(recs.copy(), tenant_id, campaign_id, snapshot_id, dataset_version, generated_at_utc)
        territory = tables.get("gold_territory_profile", pd.DataFrame())
        if not territory.empty and "territorio_id" in out.columns and "territorio_id" in territory.columns:
            enrich_columns = [
                column
                for column in ["territorio_id", "uf", "municipio_nome", "zona", "secao", "local_votacao"]
                if column in territory.columns
            ]
            out = out.merge(territory[enrich_columns].drop_duplicates("territorio_id"), on="territorio_id", how="left")
        out["confidence_score"] = self._confidence(out)
        out["evidence_ids"] = self._evidence_ids(out)
        keep = [
            "tenant_id",
            "campaign_id",
            "snapshot_id",
            "dataset_version",
            "generated_at_utc",
            "scenario_id",
            "candidate_id",
            "territorio_id",
            "uf",
            "municipio_nome",
            "zona",
            "secao",
            "local_votacao",
            "tipo_acao_sugerida",
            "score_prioridade_final",
            "recurso_sugerido",
            "percentual_orcamento_sugerido",
            "justificativa",
            "confidence_score",
            "evidence_ids",
        ]
        return out[[column for column in keep if column in out.columns]].copy()

    def data_readiness(
        self,
        *,
        tenant_id: str,
        campaign_id: str,
        snapshot_id: str,
        dataset_version: str,
        tables: dict[str, pd.DataFrame],
        generated_at_utc: str,
        warnings: list[str],
    ) -> pd.DataFrame:
        priority = tables.get("gold_priority_score", pd.DataFrame())
        master = tables.get("gold_territorial_electoral_master_index", pd.DataFrame())
        health = tables.get("lake_health_report", pd.DataFrame())
        recs = tables.get("gold_allocation_recommendations", pd.DataFrame())
        join_confidence = (
            float(numeric(series_first(master, ["join_confidence"], 0)).mean()) if not master.empty else 0.0
        )
        health_score = (
            float(numeric(series_first(health, ["aggregate_quality_score", "quality_score"], 0)).mean())
            if not health.empty
            else 0.0
        )
        scored = 1 if not priority.empty else 0
        recommended = 1 if not recs.empty else 0
        readiness_score = round(
            min(1.0, 0.35 * scored + 0.25 * recommended + 0.25 * join_confidence + 0.15 * health_score), 6
        )
        if master.empty:
            warnings.append("Master index gold ausente; join_confidence medio ficou 0.")
        return pd.DataFrame(
            [
                {
                    "tenant_id": tenant_id,
                    "campaign_id": campaign_id,
                    "snapshot_id": snapshot_id,
                    "dataset_version": dataset_version,
                    "generated_at_utc": generated_at_utc,
                    "readiness_score": readiness_score,
                    "territories_ranked": int(priority["territorio_id"].nunique())
                    if "territorio_id" in priority
                    else 0,
                    "candidates_supported": int(priority["candidate_id"].nunique())
                    if "candidate_id" in priority
                    else 0,
                    "recommendations_available": bool(not recs.empty),
                    "avg_join_confidence": round(join_confidence, 6),
                    "lake_health_score": round(health_score, 6),
                    "join_limitations": "Setor censitario/local de votacao exigem chave explicita ou join aproximado documentado.",
                }
            ]
        )

    def zone_ranking(
        self,
        *,
        tenant_id: str,
        campaign_id: str,
        snapshot_id: str,
        dataset_version: str,
        tables: dict[str, pd.DataFrame],
        generated_at_utc: str,
        warnings: list[str],
    ) -> pd.DataFrame:
        zones = tables.get("gold_zone_priority_score", pd.DataFrame())
        if zones.empty:
            warnings.append("Not found in repo: gold_zone_priority_score; serving_zone_ranking vazio.")
            return self._empty_status(tenant_id, campaign_id, snapshot_id, dataset_version, generated_at_utc)
        out = self._stamp(zones.copy(), tenant_id, campaign_id, snapshot_id, dataset_version, generated_at_utc)
        out["rank_zona"] = (
            out.groupby("candidate_id", dropna=False)["score_prioridade_final"]
            .rank(method="first", ascending=False)
            .astype(int)
            if "candidate_id" in out.columns and "score_prioridade_final" in out.columns
            else range(1, len(out) + 1)
        )
        if "confidence_score" not in out.columns:
            out["confidence_score"] = self._confidence(out)
        keep = [
            "tenant_id",
            "campaign_id",
            "snapshot_id",
            "dataset_version",
            "generated_at_utc",
            "rank_zona",
            "candidate_id",
            "zona_id",
            "uf",
            "cod_municipio_tse",
            "cod_municipio_ibge",
            "municipio_nome",
            "zona",
            "territorios",
            "secoes",
            "locais_votacao",
            "score_prioridade_final",
            "score_disputabilidade",
            "margem_estimada",
            "base_eleitoral_score",
            "potencial_expansao_score",
            "custo_eficiencia_score",
            "join_confidence",
            "data_quality_score",
            "source_coverage_score",
            "confidence_score",
            "recomendacao_curta",
            "score_explanation",
        ]
        return out[[column for column in keep if column in out.columns]].sort_values(
            ["candidate_id", "rank_zona"], na_position="last"
        )

    def municipality_zone_detail(
        self,
        *,
        tenant_id: str,
        campaign_id: str,
        snapshot_id: str,
        dataset_version: str,
        tables: dict[str, pd.DataFrame],
        generated_at_utc: str,
        warnings: list[str],
    ) -> pd.DataFrame:
        zones = tables.get("gold_zone_priority_score", pd.DataFrame())
        if zones.empty:
            warnings.append("Not found in repo: gold_zone_priority_score; serving_municipality_zone_detail vazio.")
            return self._empty_status(tenant_id, campaign_id, snapshot_id, dataset_version, generated_at_utc)
        out = zones.copy()
        quality = tables.get("gold_section_master_index_quality", pd.DataFrame())
        if not quality.empty and {"municipio_nome", "zona"}.issubset(quality.columns):
            quality_summary = (
                quality.groupby(["municipio_nome", "zona"], dropna=False)
                .agg(
                    secoes_master=("secao", "nunique"),
                    locais_master=("local_votacao", lambda values: int(values.astype(str).str.strip().ne("").sum())),
                    section_quality_score=("section_quality_score", "mean"),
                    join_is_approximate=("join_is_approximate", "max"),
                    quality_limitation=("quality_limitation", first_non_empty),
                )
                .reset_index()
            )
            out = out.merge(quality_summary, on=["municipio_nome", "zona"], how="left")
        out = self._stamp(out, tenant_id, campaign_id, snapshot_id, dataset_version, generated_at_utc)
        if "confidence_score" not in out.columns:
            out["confidence_score"] = self._confidence(out)
        out["quality_limitation"] = series_first(out, ["quality_limitation"], "sem limitacao registrada")
        keep = [
            "tenant_id",
            "campaign_id",
            "snapshot_id",
            "dataset_version",
            "generated_at_utc",
            "candidate_id",
            "uf",
            "cod_municipio_tse",
            "cod_municipio_ibge",
            "municipio_nome",
            "zona",
            "zona_id",
            "territorios",
            "secoes",
            "secoes_master",
            "locais_votacao",
            "locais_master",
            "score_prioridade_final",
            "score_disputabilidade",
            "margem_estimada",
            "join_confidence",
            "data_quality_score",
            "section_quality_score",
            "confidence_score",
            "join_is_approximate",
            "quality_limitation",
            "recomendacao_curta",
            "score_explanation",
        ]
        return out[[column for column in keep if column in out.columns]].sort_values(
            ["municipio_nome", "score_prioridade_final"], ascending=[True, False], na_position="last"
        )

    def _stamp(
        self,
        df: pd.DataFrame,
        tenant_id: str,
        campaign_id: str,
        snapshot_id: str,
        dataset_version: str,
        generated_at_utc: str,
    ) -> pd.DataFrame:
        out = df.copy()
        out["tenant_id"] = tenant_id
        out["campaign_id"] = campaign_id
        out["snapshot_id"] = snapshot_id
        out["dataset_version"] = dataset_version
        out["generated_at_utc"] = generated_at_utc
        return out

    def _confidence(self, df: pd.DataFrame) -> pd.Series:
        if "confidence_score" in df.columns:
            return numeric(df["confidence_score"], 0.0).clip(0, 1)
        priority = numeric(series_first(df, ["score_prioridade_final", "score_prioridade"], 0.0), 0.0)
        quality = numeric(series_first(df, ["data_quality_score", "join_confidence"], 0.7), 0.7)
        return (0.65 * priority + 0.35 * quality).clip(0, 1)

    def _evidence_ids(self, df: pd.DataFrame) -> pd.Series:
        candidate = series_first(df, ["candidate_id"], "").astype(str)
        territory = series_first(df, ["territorio_id"], "").astype(str)
        scenario = series_first(df, ["scenario_id", "scenario"], "baseline").astype(str)
        return "ev:" + scenario + ":" + candidate + ":" + territory

    def _empty_status(
        self,
        tenant_id: str,
        campaign_id: str,
        snapshot_id: str,
        dataset_version: str,
        generated_at_utc: str,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "tenant_id": tenant_id,
                    "campaign_id": campaign_id,
                    "snapshot_id": snapshot_id,
                    "dataset_version": dataset_version,
                    "generated_at_utc": generated_at_utc,
                    "status": "Not found in repo",
                }
            ]
        )
