from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

EntityId = Literal[
    "candidato",
    "territorio",
    "secao_eleitoral",
    "local_votacao",
    "municipio",
    "cluster_territorial",
    "tema",
    "recomendacao",
    "cenario",
    "gasto",
    "base_eleitoral",
    "concorrencia",
]

MetricId = Literal[
    "forca_base",
    "potencial_expansao",
    "intensidade_competitiva",
    "aderencia_tematica",
    "eficiencia_gasto",
    "prioridade_territorial",
    "confianca_recomendacao",
    "cobertura_territorial",
    "custo_por_voto_estimado",
    "share_potencial",
]


class SemanticEntity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entity_id: EntityId
    name: str
    description: str
    primary_key: list[str]
    canonical_table: str
    dimensions: list[str] = Field(default_factory=list)
    related_entities: list[EntityId] = Field(default_factory=list)


class SemanticMetric(BaseModel):
    model_config = ConfigDict(extra="forbid")

    metric_id: MetricId
    name: str
    description: str
    formula: str
    grain: str
    source_table: str
    source_columns: list[str]
    value_type: Literal["score", "money", "ratio", "count", "text"]
    owner: str = "analytics"
    consumers: list[str] = Field(default_factory=list)
    quality_notes: str = ""


class SemanticDimension(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dimension_id: str
    name: str
    description: str
    entity_id: EntityId
    source_table: str
    source_column: str


class SemanticRegistry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str = "semantic_registry_v1"
    entities: list[SemanticEntity]
    metrics: list[SemanticMetric]
    dimensions: list[SemanticDimension]

    def metric(self, metric_id: str) -> SemanticMetric | None:
        normalized = metric_id.strip().lower()
        return next((metric for metric in self.metrics if metric.metric_id == normalized), None)

    def entity(self, entity_id: str) -> SemanticEntity | None:
        normalized = entity_id.strip().lower()
        return next((entity for entity in self.entities if entity.entity_id == normalized), None)
