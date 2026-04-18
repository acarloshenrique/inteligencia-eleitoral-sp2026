from __future__ import annotations

from commercial.models import CommercialAssetSpec, TenantIsolationPolicy

COMPETITIVE_DATASETS: tuple[CommercialAssetSpec, ...] = (
    CommercialAssetSpec(
        asset_id="master_index_secao_zona",
        name="Master Index territorial-eleitoral",
        description="Conecta secao, zona, municipio, candidato, setor censitario e confianca de join.",
        impact="critical",
        source_tables=["gold_territorial_electoral_master_index"],
        supported_outputs=["parquet", "json", "csv", "markdown"],
        commercial_use_cases=["prova de granularidade", "auditoria de dados", "storytelling tecnico de venda"],
        demo_readiness=True,
        premium_report_ready=True,
        limitations=["Setor censitario depende de geocoding ou cd_setor explicito."],
    ),
    CommercialAssetSpec(
        asset_id="priority_and_allocation_marts",
        name="Marts de priorizacao e alocacao",
        description="Ranking de territorio, justificativa e recomendacao de verba por cenario.",
        impact="critical",
        source_tables=["gold_priority_score", "gold_allocation_recommendations"],
        supported_outputs=["parquet", "json", "csv", "xlsx", "markdown"],
        commercial_use_cases=["demo executiva", "simulador de orcamento", "relatorio premium"],
        demo_readiness=True,
        premium_report_ready=True,
    ),
    CommercialAssetSpec(
        asset_id="feature_store_recommendation",
        name="Feature store territorial",
        description="Features reutilizaveis para score, recomendacao e explicabilidade.",
        impact="high",
        source_tables=["territorial_recommendation_features"],
        supported_outputs=["parquet", "json", "csv"],
        commercial_use_cases=["diferenciacao de produto", "explicabilidade tecnica", "evolucao ML"],
        demo_readiness=True,
        premium_report_ready=False,
    ),
    CommercialAssetSpec(
        asset_id="semantic_registry",
        name="Semantic Registry",
        description="Contrato canonico de entidades e metricas para API, UI e relatorios.",
        impact="high",
        source_tables=["semantic_registry"],
        supported_outputs=["json", "markdown"],
        commercial_use_cases=["governanca", "consistencia entre modulos", "venda enterprise"],
        demo_readiness=True,
        premium_report_ready=False,
    ),
    CommercialAssetSpec(
        asset_id="lake_health_report",
        name="Lake Health Report",
        description="Score de qualidade, readiness e confiabilidade de joins por dataset.",
        impact="high",
        source_tables=["lake_health_report"],
        supported_outputs=["json", "markdown"],
        commercial_use_cases=["seguranca comercial", "auditoria", "pos-venda enterprise"],
        demo_readiness=True,
        premium_report_ready=True,
    ),
)


def competitive_dataset_ranking() -> list[CommercialAssetSpec]:
    order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    return sorted(COMPETITIVE_DATASETS, key=lambda item: order[item.impact])


def default_tenant_policy(*, tenant_id: str, data_root: str) -> TenantIsolationPolicy:
    return TenantIsolationPolicy(
        tenant_id=tenant_id,
        storage_root=f"{data_root}/tenants/{tenant_id}" if tenant_id != "default" else data_root,
        logical_filters={"tenant_id": tenant_id},
    )


def multi_candidate_tables() -> list[str]:
    return [
        "gold_candidate_context",
        "gold_electoral_base_strength",
        "gold_priority_score",
        "gold_allocation_inputs",
        "gold_allocation_recommendations",
        "gold_candidate_comparisons",
        "territorial_recommendation_features",
    ]


def exportable_artifacts() -> list[str]:
    return [
        "commercial_demo_summary.json",
        "commercial_demo_summary.md",
        "premium_report_tables.xlsx",
        "ranking_operacional.csv",
        "allocation_recommendations.csv",
        "lake_health_report.md",
        "semantic_registry.md",
    ]
