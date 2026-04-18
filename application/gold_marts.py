from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def numeric(series: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(default)


def first_existing(df: pd.DataFrame, aliases: list[str]) -> str | None:
    normalized = {str(column).lower(): str(column) for column in df.columns}
    for alias in aliases:
        if alias.lower() in normalized:
            return normalized[alias.lower()]
    return None


def series_first(df: pd.DataFrame, aliases: list[str], default: Any = "") -> pd.Series:
    column = first_existing(df, aliases)
    if column is None:
        return pd.Series([default] * len(df), index=df.index)
    return df[column]


class GoldTableSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    table_name: str
    grain: str
    business_definition: str
    metric_definitions: dict[str, str]
    source_lineage: list[str]
    refresh_policy: str
    consumers: list[str]
    data_quality_checks: list[str]


class GoldTableResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    table_name: str
    rows: int
    parquet_path: str
    manifest_path: str
    quality: dict[str, Any] = Field(default_factory=dict)


@dataclass(frozen=True)
class GoldMartBuildResult:
    tables: dict[str, pd.DataFrame]
    outputs: list[GoldTableResult]
    duckdb_path: Path | None
    sql_examples_path: Path


GOLD_TABLE_SPECS: dict[str, GoldTableSpec] = {
    "gold_candidate_context": GoldTableSpec(
        table_name="gold_candidate_context",
        grain="candidate_id",
        business_definition="Contexto consolidado do candidato para filtros, comparativos e explicacoes.",
        metric_definitions={
            "territorios_cobertos": "quantidade de territorios presentes no master index por candidato",
            "source_coverage_avg": "media da cobertura de fontes nos territorios do candidato",
        },
        source_lineage=["gold_territorial_electoral_master_index", "silver_tse_candidatos"],
        refresh_policy="por ciclo eleitoral ou quando cadastro/manual profile mudar",
        consumers=["dashboards executivos", "copiloto analitico", "relatorios comerciais"],
        data_quality_checks=["candidate_id nao nulo", "uma linha por candidate_id"],
    ),
    "gold_territory_profile": GoldTableSpec(
        table_name="gold_territory_profile",
        grain="ano_eleicao + uf + cod_municipio_tse + zona + secao quando disponivel",
        business_definition="Perfil territorial operacional por secao/zona/municipio para navegacao e segmentacao agregada.",
        metric_definitions={
            "secoes": "numero de secoes no territorio",
            "candidatos": "numero de candidatos observados",
            "join_confidence_avg": "confianca media dos joins do territorio",
        },
        source_lineage=["gold_territorial_electoral_master_index"],
        refresh_policy="semanal ou apos atualizacao silver/master index",
        consumers=["mapas", "ranking territorial", "simuladores"],
        data_quality_checks=["territorio_id unico", "join_confidence entre 0 e 1"],
    ),
    "gold_electoral_base_strength": GoldTableSpec(
        table_name="gold_electoral_base_strength",
        grain="candidate_id + territorio_id",
        business_definition="Forca de base eleitoral por candidato e territorio agregado.",
        metric_definitions={
            "base_strength_score": "percentil relativo de votos ou cobertura eleitoral observada",
            "votos_nominais": "soma de votos nominais quando disponivel",
        },
        source_lineage=["gold_territorial_electoral_master_index", "silver_tse_resultados_secao"],
        refresh_policy="apos ingestao de resultados eleitorais",
        consumers=["score", "ranking", "recomendacao de retencao"],
        data_quality_checks=["score entre 0 e 1", "candidate_id e territorio_id nao nulos"],
    ),
    "gold_competition_landscape": GoldTableSpec(
        table_name="gold_competition_landscape",
        grain="territorio_id",
        business_definition="Nivel de competicao local e concentracao de candidatos por territorio.",
        metric_definitions={
            "competition_score": "1 menos margem normalizada entre primeiro e segundo colocado",
            "candidate_count": "quantidade de candidatos com presenca no territorio",
        },
        source_lineage=["gold_electoral_base_strength"],
        refresh_policy="apos resultados ou mudanca de recorte",
        consumers=["score", "relatorios", "copiloto analitico"],
        data_quality_checks=["competition_score entre 0 e 1"],
    ),
    "gold_campaign_finance_efficiency": GoldTableSpec(
        table_name="gold_campaign_finance_efficiency",
        grain="candidate_id",
        business_definition="Eficiencia financeira da campanha com custo estimado por voto.",
        metric_definitions={
            "custo_por_voto_estimado": "despesa_total / votos_nominais",
            "finance_efficiency_score": "score inverso do custo relativo por voto",
        },
        source_lineage=["silver_tse_prestacao_contas", "gold_electoral_base_strength"],
        refresh_policy="durante janelas de prestacao de contas e apos totalizacao",
        consumers=["simuladores", "recomendacao de investimento", "relatorios comerciais"],
        data_quality_checks=["valores monetarios nao negativos", "candidate_id nao nulo"],
    ),
    "gold_thematic_affinity": GoldTableSpec(
        table_name="gold_thematic_affinity",
        grain="territorio_id + tema",
        business_definition="Aderencia tematica territorial para mensagens e narrativas agregadas.",
        metric_definitions={
            "thematic_affinity_score": "score normalizado por tema e territorio",
            "theme_evidence_count": "quantidade de evidencias tematicas agregadas",
        },
        source_lineage=["silver_ibge_agregados_censo", "fontes_tematicas", "candidate_context"],
        refresh_policy="mensal ou quando indicadores tematicos mudarem",
        consumers=["mensagem", "copiloto analitico", "recomendacao de canal"],
        data_quality_checks=["score entre 0 e 1", "tema nao nulo"],
    ),
    "gold_priority_score": GoldTableSpec(
        table_name="gold_priority_score",
        grain="candidate_id + territorio_id",
        business_definition="Score final de priorizacao territorial para decisao de campanha.",
        metric_definitions={
            "score_prioridade_final": "combinacao de base, tema, expansao, eficiencia, competicao e operacao",
            "potencial_expansao_score": "potencial em territorio ainda nao consolidado",
        },
        source_lineage=[
            "gold_electoral_base_strength",
            "gold_competition_landscape",
            "gold_campaign_finance_efficiency",
            "gold_thematic_affinity",
        ],
        refresh_policy="semanal ou sob demanda por estrategia/candidato",
        consumers=["ranking territorial", "simuladores", "recomendacao de investimento"],
        data_quality_checks=["score_prioridade_final entre 0 e 1", "sem duplicidade por candidate_id + territorio_id"],
    ),
    "gold_allocation_inputs": GoldTableSpec(
        table_name="gold_allocation_inputs",
        grain="candidate_id + territorio_id",
        business_definition="Entrada padronizada para simuladores e motores de alocacao.",
        metric_definitions={
            "allocation_weight": "peso relativo de priorizacao para distribuicao de orcamento",
            "tipo_acao_sugerida": "tipo de acao derivado dos componentes do score",
        },
        source_lineage=["gold_priority_score", "gold_territory_profile"],
        refresh_policy="sob demanda por cenario",
        consumers=["simulador de orcamento", "allocation engine"],
        data_quality_checks=["allocation_weight nao negativo"],
    ),
    "gold_allocation_recommendations": GoldTableSpec(
        table_name="gold_allocation_recommendations",
        grain="scenario_id + candidate_id + territorio_id",
        business_definition="Recomendacoes de alocacao de verba, canal e acao territorial.",
        metric_definitions={
            "recurso_sugerido": "orcamento sugerido para o territorio no cenario",
            "percentual_orcamento_sugerido": "participacao do territorio no orcamento total",
        },
        source_lineage=["gold_allocation_inputs"],
        refresh_policy="sob demanda por cenario",
        consumers=["relatorios comerciais", "dashboards executivos", "copiloto analitico"],
        data_quality_checks=["percentuais somam aproximadamente 1 por cenario"],
    ),
    "gold_territorial_clusters": GoldTableSpec(
        table_name="gold_territorial_clusters",
        grain="territorial_cluster_id",
        business_definition="Clusters territoriais para leitura executiva e agrupamento operacional.",
        metric_definitions={
            "cluster_label": "classificacao interpretavel do territorio",
            "priority_avg": "media de prioridade dos territorios do cluster",
        },
        source_lineage=["gold_priority_score", "gold_territory_profile"],
        refresh_policy="semanal ou sob demanda",
        consumers=["dashboards", "mapas", "planejamento operacional"],
        data_quality_checks=["cluster_label nao nulo"],
    ),
    "gold_candidate_comparisons": GoldTableSpec(
        table_name="gold_candidate_comparisons",
        grain="territorio_id",
        business_definition="Comparativo agregado entre candidatos por territorio.",
        metric_definitions={
            "leader_candidate_id": "candidato com maior forca estimada no territorio",
            "leader_margin_score": "diferenca entre primeiro e segundo score de base",
        },
        source_lineage=["gold_electoral_base_strength", "gold_priority_score"],
        refresh_policy="apos score ou resultados",
        consumers=["competicao", "copiloto analitico", "relatorios"],
        data_quality_checks=["territorio_id unico"],
    ),
}


class GoldMartBuilder:
    def build_all(
        self,
        *,
        master_index: pd.DataFrame,
        candidate_profiles: pd.DataFrame | None = None,
        electoral_results: pd.DataFrame | None = None,
        campaign_finance: pd.DataFrame | None = None,
        thematic_signals: pd.DataFrame | None = None,
        budget_total: float = 100000.0,
        scenario_id: str = "baseline",
    ) -> dict[str, pd.DataFrame]:
        territory_profile = self.gold_territory_profile(master_index)
        candidate_context = self.gold_candidate_context(master_index, candidate_profiles)
        base_strength = self.gold_electoral_base_strength(master_index, electoral_results)
        competition = self.gold_competition_landscape(base_strength)
        finance = self.gold_campaign_finance_efficiency(base_strength, campaign_finance)
        thematic = self.gold_thematic_affinity(territory_profile, thematic_signals)
        priority = self.gold_priority_score(base_strength, competition, finance, thematic, territory_profile)
        allocation_inputs = self.gold_allocation_inputs(priority)
        recommendations = self.gold_allocation_recommendations(
            allocation_inputs,
            budget_total=budget_total,
            scenario_id=scenario_id,
        )
        clusters = self.gold_territorial_clusters(priority, territory_profile)
        comparisons = self.gold_candidate_comparisons(base_strength, priority)
        return {
            "gold_candidate_context": candidate_context,
            "gold_territory_profile": territory_profile,
            "gold_electoral_base_strength": base_strength,
            "gold_competition_landscape": competition,
            "gold_campaign_finance_efficiency": finance,
            "gold_thematic_affinity": thematic,
            "gold_priority_score": priority,
            "gold_allocation_inputs": allocation_inputs,
            "gold_allocation_recommendations": recommendations,
            "gold_territorial_clusters": clusters,
            "gold_candidate_comparisons": comparisons,
        }

    def gold_candidate_context(self, master: pd.DataFrame, profiles: pd.DataFrame | None = None) -> pd.DataFrame:
        candidates = master[master["candidate_id"].astype(str).str.len().gt(0)].copy()
        if candidates.empty and profiles is None:
            return pd.DataFrame(columns=["candidate_id", "nome_politico", "partido", "territorios_cobertos"])
        grouped = (
            candidates.groupby("candidate_id", dropna=False)
            .agg(
                partido=("partido", first_non_empty),
                territorios_cobertos=("master_record_id", "nunique"),
                municipios_cobertos=("cod_municipio_tse", "nunique"),
                zonas_cobertas=("zona", "nunique"),
                source_coverage_avg=("source_coverage_score", "mean"),
                join_confidence_avg=("join_confidence", "mean"),
            )
            .reset_index()
        )
        if profiles is not None and not profiles.empty:
            profile = pd.DataFrame(
                {
                    "candidate_id": series_first(profiles, ["candidate_id", "SQ_CANDIDATO"]).astype(str),
                    "nome_politico": series_first(profiles, ["nome_politico", "NM_URNA_CANDIDATO", "nome"], ""),
                    "cargo": series_first(profiles, ["cargo", "DS_CARGO"], ""),
                    "partido_profile": series_first(profiles, ["partido", "SG_PARTIDO"], ""),
                }
            ).drop_duplicates("candidate_id")
            grouped = profile.merge(grouped, on="candidate_id", how="left")
            grouped["partido"] = grouped["partido"].fillna(grouped["partido_profile"])
            grouped = grouped.drop(columns=["partido_profile"])
        else:
            grouped["nome_politico"] = ""
            grouped["cargo"] = ""
        return grouped.fillna({"territorios_cobertos": 0, "source_coverage_avg": 0.0, "join_confidence_avg": 0.0})

    def gold_territory_profile(self, master: pd.DataFrame) -> pd.DataFrame:
        df = master.copy()
        df = self._ensure_territory_columns(df)
        df["territorio_id"] = self._territory_id(df)
        grouped = (
            df.groupby(
                [
                    "ano_eleicao",
                    "uf",
                    "cod_municipio_tse",
                    "cod_municipio_ibge",
                    "municipio_nome",
                    "zona",
                    "secao",
                    "local_votacao",
                ],
                dropna=False,
            )
            .agg(
                territorio_id=("territorio_id", first_non_empty),
                territorial_cluster_id=("territorial_cluster_id", first_non_empty),
                secoes=("secao", "nunique"),
                locais_votacao=("local_votacao", count_non_empty),
                candidatos=("candidate_id", count_non_empty),
                cd_setor_count=("cd_setor", count_non_empty),
                join_confidence_avg=("join_confidence", "mean"),
                source_coverage_avg=("source_coverage_score", "mean"),
            )
            .reset_index()
        )
        grouped["data_quality_score"] = (
            grouped["join_confidence_avg"] * 0.7 + grouped["source_coverage_avg"] * 0.3
        ).clip(0, 1)
        return grouped

    def gold_electoral_base_strength(self, master: pd.DataFrame, results: pd.DataFrame | None = None) -> pd.DataFrame:
        df = self._master_with_territory(master)
        if results is not None and not results.empty:
            result_frame = pd.DataFrame(
                {
                    "ano_eleicao": numeric(series_first(results, ["ano_eleicao", "ANO_ELEICAO"], 0)).astype("Int64"),
                    "uf": series_first(results, ["uf", "SIGLA_UF"], "").astype(str).str.upper().str.strip(),
                    "cod_municipio_tse": series_first(results, ["cod_municipio_tse", "COD_MUN_TSE"], "")
                    .astype(str)
                    .str.zfill(5),
                    "zona": series_first(results, ["zona", "ZONA"], "").astype(str).str.zfill(4),
                    "secao": series_first(results, ["secao", "SECAO"], "").astype(str).str.zfill(4),
                    "candidate_id": series_first(results, ["candidate_id", "SQ_CANDIDATO"], "").astype(str),
                    "votos_nominais": numeric(
                        series_first(results, ["votos_nominais", "QT_VOTOS_NOMINAIS", "votos"], 0)
                    ),
                    "total_aptos": numeric(series_first(results, ["total_aptos", "QT_APTOS"], 0)),
                }
            )
            df = df.merge(
                result_frame, on=["ano_eleicao", "uf", "cod_municipio_tse", "zona", "secao", "candidate_id"], how="left"
            )
        else:
            df["votos_nominais"] = 0.0
            df["total_aptos"] = 0.0
        grouped = (
            df.groupby(["candidate_id", "territorio_id"], dropna=False)
            .agg(
                ano_eleicao=("ano_eleicao", "max"),
                uf=("uf", first_non_empty),
                cod_municipio_tse=("cod_municipio_tse", first_non_empty),
                municipio_nome=("municipio_nome", first_non_empty),
                zona=("zona", first_non_empty),
                secao=("secao", first_non_empty),
                local_votacao=("local_votacao", first_non_empty),
                territorial_cluster_id=("territorial_cluster_id", first_non_empty),
                votos_nominais=("votos_nominais", "sum"),
                total_aptos=("total_aptos", "sum"),
                source_coverage_score=("source_coverage_score", "mean"),
                join_confidence=("join_confidence", "mean"),
            )
            .reset_index()
        )
        max_votes = max(float(grouped["votos_nominais"].max()), 1.0)
        grouped["base_strength_score"] = grouped.apply(
            lambda row: clamp01(
                float(row["votos_nominais"]) / max_votes
                if row["votos_nominais"]
                else float(row["source_coverage_score"]) * 0.5
            ),
            axis=1,
        )
        grouped["retention_score"] = grouped["base_strength_score"]
        grouped["expansion_signal"] = 1.0 - grouped["base_strength_score"]
        return grouped

    def gold_competition_landscape(self, base_strength: pd.DataFrame) -> pd.DataFrame:
        records: list[dict[str, Any]] = []
        for territorio_id, group in base_strength.groupby("territorio_id", dropna=False):
            scores = group.sort_values("base_strength_score", ascending=False)
            top = float(scores["base_strength_score"].iloc[0]) if len(scores) else 0.0
            second = float(scores["base_strength_score"].iloc[1]) if len(scores) > 1 else 0.0
            margin = max(top - second, 0.0)
            records.append(
                {
                    "territorio_id": territorio_id,
                    "candidate_count": int(group["candidate_id"].astype(str).str.len().gt(0).sum()),
                    "leader_candidate_id": str(scores["candidate_id"].iloc[0]) if len(scores) else "",
                    "leader_margin_score": round(margin, 6),
                    "competition_score": round(clamp01(1.0 - margin), 6),
                }
            )
        return pd.DataFrame(records)

    def gold_campaign_finance_efficiency(
        self, base_strength: pd.DataFrame, finance: pd.DataFrame | None = None
    ) -> pd.DataFrame:
        votes = base_strength.groupby("candidate_id", dropna=False)["votos_nominais"].sum().reset_index()
        if finance is not None and not finance.empty:
            fin = pd.DataFrame(
                {
                    "candidate_id": series_first(finance, ["candidate_id", "SQ_CANDIDATO"]).astype(str),
                    "receita_total": numeric(series_first(finance, ["receita_total", "valor_receita"], 0)),
                    "despesa_total": numeric(series_first(finance, ["despesa_total", "valor_despesa"], 0)),
                }
            )
            fin = fin.groupby("candidate_id", dropna=False).sum(numeric_only=True).reset_index()
        else:
            fin = pd.DataFrame({"candidate_id": votes["candidate_id"], "receita_total": 0.0, "despesa_total": 0.0})
        out = votes.merge(fin, on="candidate_id", how="outer").fillna(0)
        out["custo_por_voto_estimado"] = out.apply(
            lambda row: (
                float(row["despesa_total"]) / float(row["votos_nominais"]) if float(row["votos_nominais"]) > 0 else 0.0
            ),
            axis=1,
        )
        max_cost = max(float(out["custo_por_voto_estimado"].max()), 1.0)
        out["finance_efficiency_score"] = out["custo_por_voto_estimado"].map(
            lambda value: 1.0 if value == 0 else clamp01(1.0 - float(value) / max_cost)
        )
        return out

    def gold_thematic_affinity(
        self, territory_profile: pd.DataFrame, thematic_signals: pd.DataFrame | None = None
    ) -> pd.DataFrame:
        if thematic_signals is not None and not thematic_signals.empty:
            signals = pd.DataFrame(
                {
                    "territorio_id": series_first(thematic_signals, ["territorio_id"], ""),
                    "tema": series_first(thematic_signals, ["tema", "theme"], "geral").astype(str),
                    "thematic_affinity_score": numeric(
                        series_first(thematic_signals, ["thematic_affinity_score", "score"], 0.5)
                    ),
                    "theme_evidence_count": numeric(
                        series_first(thematic_signals, ["theme_evidence_count", "evidence_count"], 1)
                    ),
                }
            )
            return (
                signals.groupby(["territorio_id", "tema"], dropna=False)
                .agg(
                    thematic_affinity_score=("thematic_affinity_score", "mean"),
                    theme_evidence_count=("theme_evidence_count", "sum"),
                )
                .reset_index()
            )
        return pd.DataFrame(
            {
                "territorio_id": territory_profile["territorio_id"],
                "tema": "geral",
                "thematic_affinity_score": 0.5,
                "theme_evidence_count": 0,
            }
        )

    def gold_priority_score(
        self,
        base_strength: pd.DataFrame,
        competition: pd.DataFrame,
        finance: pd.DataFrame,
        thematic: pd.DataFrame,
        territory_profile: pd.DataFrame,
    ) -> pd.DataFrame:
        theme = thematic.groupby("territorio_id", dropna=False)["thematic_affinity_score"].mean().reset_index()
        out = base_strength.merge(competition[["territorio_id", "competition_score"]], on="territorio_id", how="left")
        out = out.merge(finance[["candidate_id", "finance_efficiency_score"]], on="candidate_id", how="left")
        out = out.merge(theme, on="territorio_id", how="left")
        out = out.merge(territory_profile[["territorio_id", "data_quality_score"]], on="territorio_id", how="left")
        out = out.fillna(
            {
                "competition_score": 0.5,
                "finance_efficiency_score": 0.5,
                "thematic_affinity_score": 0.5,
                "data_quality_score": 0.5,
            }
        )
        out["potencial_expansao_score"] = (1.0 - out["base_strength_score"]).clip(0, 1)
        out["score_prioridade_final"] = (
            0.30 * out["base_strength_score"]
            + 0.20 * out["thematic_affinity_score"]
            + 0.15 * out["potencial_expansao_score"]
            + 0.15 * out["finance_efficiency_score"]
            + 0.10 * out["competition_score"]
            + 0.10 * out["data_quality_score"]
        ).clip(0, 1)
        out["score_explanation"] = out.apply(
            lambda row: (
                f"base={row['base_strength_score']:.2f}; tema={row['thematic_affinity_score']:.2f}; "
                f"expansao={row['potencial_expansao_score']:.2f}; eficiencia={row['finance_efficiency_score']:.2f}; "
                f"competicao={row['competition_score']:.2f}"
            ),
            axis=1,
        )
        return out

    def gold_allocation_inputs(self, priority: pd.DataFrame) -> pd.DataFrame:
        out = priority.copy()
        out["tipo_acao_sugerida"] = out.apply(self._suggest_action, axis=1)
        out["allocation_weight"] = (
            out["score_prioridade_final"]
            * (0.5 + 0.3 * out["finance_efficiency_score"] + 0.2 * out["data_quality_score"])
        ).clip(lower=0)
        return out[
            [
                "candidate_id",
                "territorio_id",
                "territorial_cluster_id",
                "score_prioridade_final",
                "tipo_acao_sugerida",
                "allocation_weight",
                "data_quality_score",
                "join_confidence",
            ]
        ]

    def gold_allocation_recommendations(
        self,
        allocation_inputs: pd.DataFrame,
        *,
        budget_total: float,
        scenario_id: str,
    ) -> pd.DataFrame:
        if allocation_inputs.empty:
            return pd.DataFrame(
                columns=[
                    "scenario_id",
                    "candidate_id",
                    "territorio_id",
                    "tipo_acao_sugerida",
                    "recurso_sugerido",
                    "percentual_orcamento_sugerido",
                ]
            )
        out = allocation_inputs.sort_values("score_prioridade_final", ascending=False).copy()
        total_weight = float(out["allocation_weight"].sum())
        out["percentual_orcamento_sugerido"] = (
            out["allocation_weight"] / total_weight if total_weight > 0 else 1 / len(out)
        )
        out["recurso_sugerido"] = out["percentual_orcamento_sugerido"] * float(budget_total)
        out["scenario_id"] = scenario_id
        out["justificativa"] = out.apply(
            lambda row: (
                f"Prioridade {row['score_prioridade_final']:.2f}, qualidade {row['data_quality_score']:.2f}, acao {row['tipo_acao_sugerida']}."
            ),
            axis=1,
        )
        return out[
            [
                "scenario_id",
                "candidate_id",
                "territorio_id",
                "tipo_acao_sugerida",
                "score_prioridade_final",
                "recurso_sugerido",
                "percentual_orcamento_sugerido",
                "justificativa",
            ]
        ]

    def gold_territorial_clusters(self, priority: pd.DataFrame, territory_profile: pd.DataFrame) -> pd.DataFrame:
        base = priority.merge(
            territory_profile[["territorio_id", "secoes", "locais_votacao"]], on="territorio_id", how="left"
        )
        base["cluster_label"] = base["score_prioridade_final"].map(self._cluster_label)
        return (
            base.groupby("territorial_cluster_id", dropna=False)
            .agg(
                cluster_label=("cluster_label", first_non_empty),
                territorios=("territorio_id", "nunique"),
                priority_avg=("score_prioridade_final", "mean"),
                secoes=("secoes", "sum"),
                locais_votacao=("locais_votacao", "sum"),
            )
            .reset_index()
        )

    def gold_candidate_comparisons(self, base_strength: pd.DataFrame, priority: pd.DataFrame) -> pd.DataFrame:
        merged = base_strength[["candidate_id", "territorio_id", "base_strength_score"]].merge(
            priority[["candidate_id", "territorio_id", "score_prioridade_final"]],
            on=["candidate_id", "territorio_id"],
            how="left",
        )
        records: list[dict[str, Any]] = []
        for territorio_id, group in merged.groupby("territorio_id", dropna=False):
            ordered = group.sort_values("base_strength_score", ascending=False)
            leader_score = float(ordered["base_strength_score"].iloc[0]) if len(ordered) else 0.0
            second_score = float(ordered["base_strength_score"].iloc[1]) if len(ordered) > 1 else 0.0
            records.append(
                {
                    "territorio_id": territorio_id,
                    "leader_candidate_id": str(ordered["candidate_id"].iloc[0]) if len(ordered) else "",
                    "leader_margin_score": round(max(leader_score - second_score, 0.0), 6),
                    "candidate_count": int(len(ordered)),
                    "avg_priority_score": round(float(ordered["score_prioridade_final"].mean()), 6)
                    if len(ordered)
                    else 0.0,
                }
            )
        return pd.DataFrame(records)

    def _master_with_territory(self, master: pd.DataFrame) -> pd.DataFrame:
        out = master.copy()
        out = self._ensure_territory_columns(out)
        out["territorio_id"] = self._territory_id(out)
        return out

    def _territory_id(self, df: pd.DataFrame) -> pd.Series:
        base = (
            df["ano_eleicao"].astype(str)
            + ":"
            + df["uf"].astype(str)
            + ":"
            + df["cod_municipio_tse"].astype(str)
            + ":Z"
            + df["zona"].astype(str)
        )
        section = df["secao"].astype(str).str.strip()
        has_section = section.ne("") & section.str.lower().ne("nan")
        return base.where(~has_section, base + ":S" + section)

    def _ensure_territory_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        defaults: dict[str, Any] = {
            "ano_eleicao": 0,
            "uf": "",
            "cod_municipio_tse": "",
            "cod_municipio_ibge": "",
            "municipio_nome": "",
            "zona": "",
            "secao": "",
            "local_votacao": "",
            "candidate_id": "",
            "territorial_cluster_id": "",
            "cd_setor": "",
            "join_confidence": 0.0,
            "source_coverage_score": 0.0,
            "master_record_id": "",
        }
        for column, default in defaults.items():
            if column not in out.columns:
                out[column] = default
        out["uf"] = out["uf"].astype(str).str.upper().str.strip()
        out["cod_municipio_tse"] = out["cod_municipio_tse"].astype(str).str.strip().str.zfill(5)
        out["cod_municipio_ibge"] = out["cod_municipio_ibge"].astype(str).str.strip()
        out["zona"] = out["zona"].astype(str).str.strip().str.zfill(4)
        out["secao"] = out["secao"].astype(str).str.strip().str.zfill(4)
        out.loc[out["secao"].isin(["0000", "0nan", "None"]), "secao"] = ""
        return out

    def _suggest_action(self, row: pd.Series) -> str:
        if float(row["base_strength_score"]) >= 0.7:
            return "retencao_base"
        if float(row["competition_score"]) >= 0.7:
            return "reforco_area_competitiva"
        if float(row["potencial_expansao_score"]) >= 0.7:
            return "expansao_territorial"
        if float(row["thematic_affinity_score"]) >= 0.7:
            return "comunicacao_programatica"
        return "presenca_fisica"

    def _cluster_label(self, score: float) -> str:
        value = float(score)
        if value >= 0.75:
            return "prioridade_maxima"
        if value >= 0.6:
            return "oportunidade"
        if value >= 0.4:
            return "monitoramento"
        return "baixo_retorno"


class GoldMartWriter:
    def write_all(
        self,
        tables: dict[str, pd.DataFrame],
        *,
        output_dir: Path,
        dataset_version: str,
    ) -> GoldMartBuildResult:
        root = output_dir / dataset_version
        root.mkdir(parents=True, exist_ok=True)
        outputs: list[GoldTableResult] = []
        for table_name, df in tables.items():
            table_dir = root / table_name
            table_dir.mkdir(parents=True, exist_ok=True)
            parquet_path = table_dir / f"{table_name}.parquet"
            manifest_path = table_dir / "manifest.json"
            df.to_parquet(parquet_path, index=False)
            spec = GOLD_TABLE_SPECS[table_name]
            quality = self._quality(df, spec)
            manifest = {
                "table_name": table_name,
                "dataset_version": dataset_version,
                "created_at_utc": utc_now_iso(),
                "parquet_path": str(parquet_path),
                "spec": spec.model_dump(mode="json"),
                "schema": {column: str(dtype) for column, dtype in df.dtypes.items()},
                "quality": quality,
            }
            manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
            outputs.append(
                GoldTableResult(
                    table_name=table_name,
                    rows=int(len(df)),
                    parquet_path=str(parquet_path),
                    manifest_path=str(manifest_path),
                    quality=quality,
                )
            )
        duckdb_path = self._write_duckdb(tables, root)
        sql_examples_path = self._write_sql_examples(root)
        return GoldMartBuildResult(
            tables=tables, outputs=outputs, duckdb_path=duckdb_path, sql_examples_path=sql_examples_path
        )

    def _quality(self, df: pd.DataFrame, spec: GoldTableSpec) -> dict[str, Any]:
        quality: dict[str, Any] = {
            "rows": int(len(df)),
            "columns": int(len(df.columns)),
            "grain": spec.grain,
            "checks": spec.data_quality_checks,
        }
        score_columns = [
            column for column in df.columns if column.endswith("_score") or column == "score_prioridade_final"
        ]
        for column in score_columns:
            values = numeric(df[column], 0.0)
            quality[f"{column}_min"] = round(float(values.min()), 6) if len(values) else 0.0
            quality[f"{column}_max"] = round(float(values.max()), 6) if len(values) else 0.0
            quality[f"{column}_nulls"] = int(df[column].isna().sum())
        return quality

    def _write_duckdb(self, tables: dict[str, pd.DataFrame], root: Path) -> Path | None:
        try:
            import duckdb
        except ImportError:
            return None
        duckdb_path = root / "gold_marts.duckdb"
        try:
            with duckdb.connect(str(duckdb_path)) as con:
                for table_name, df in tables.items():
                    con.register("_gold_df", df)
                    con.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM _gold_df')  # noqa: S608
        except Exception:
            return None
        return duckdb_path

    def _write_sql_examples(self, root: Path) -> Path:
        sql = """-- DuckDB examples for gold marts
-- Top territories by priority
SELECT candidate_id, territorio_id, score_prioridade_final, score_explanation
FROM gold_priority_score
ORDER BY score_prioridade_final DESC
LIMIT 20;

-- Budget recommendations by scenario
SELECT scenario_id, candidate_id, territorio_id, recurso_sugerido, justificativa
FROM gold_allocation_recommendations
ORDER BY recurso_sugerido DESC;

-- Territory cluster executive summary
SELECT cluster_label, COUNT(*) AS clusters, AVG(priority_avg) AS priority_avg
FROM gold_territorial_clusters
GROUP BY cluster_label
ORDER BY priority_avg DESC;

-- Competition landscape
SELECT territorio_id, leader_candidate_id, leader_margin_score, candidate_count
FROM gold_candidate_comparisons
ORDER BY leader_margin_score ASC;
"""
        path = root / "duckdb_examples.sql"
        path.write_text(sql, encoding="utf-8")
        return path


def first_non_empty(values: pd.Series) -> str:
    for value in values:
        text = str(value or "").strip()
        if text and text.lower() != "nan":
            return text
    return ""


def count_non_empty(values: pd.Series) -> int:
    return int(values.astype(str).str.strip().replace("nan", "").ne("").sum())
