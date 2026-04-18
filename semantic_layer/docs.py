from __future__ import annotations

import json
from pathlib import Path

from semantic_layer.models import SemanticRegistry
from semantic_layer.registry import build_semantic_registry


class SemanticRegistryWriter:
    def write_json(self, path: Path, registry: SemanticRegistry | None = None) -> Path:
        registry = registry or build_semantic_registry()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(registry.model_dump(mode="json"), ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def write_markdown(self, path: Path, registry: SemanticRegistry | None = None) -> Path:
        registry = registry or build_semantic_registry()
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            "# Semantic Layer Registry",
            "",
            f"- Version: `{registry.version}`",
            f"- Entities: `{len(registry.entities)}`",
            f"- Metrics: `{len(registry.metrics)}`",
            f"- Dimensions: `{len(registry.dimensions)}`",
            "",
            "## Entities",
            "",
            "| Entity | Canonical table | Primary key | Description |",
            "| --- | --- | --- | --- |",
        ]
        for entity in registry.entities:
            lines.append(
                f"| `{entity.entity_id}` | `{entity.canonical_table}` | `{', '.join(entity.primary_key)}` | {entity.description} |"
            )
        lines.extend(
            [
                "",
                "## Metrics",
                "",
                "| Metric | Source | Grain | Formula | Consumers |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for metric in registry.metrics:
            lines.append(
                f"| `{metric.metric_id}` | `{metric.source_table}` | {metric.grain} | `{metric.formula}` | {', '.join(metric.consumers)} |"
            )
        lines.extend(
            ["", "## Dimensions", "", "| Dimension | Entity | Source | Description |", "| --- | --- | --- | --- |"]
        )
        for dimension in registry.dimensions:
            lines.append(
                f"| `{dimension.dimension_id}` | `{dimension.entity_id}` | `{dimension.source_table}.{dimension.source_column}` | {dimension.description} |"
            )
        path.write_text("\n".join(lines), encoding="utf-8")
        return path
