from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from application.gold_marts import GoldMartBuilder, GoldMartWriter
from commercial.exports import CommercialExportService
from commercial.marts import CommercialMartBuilder
from commercial.snapshots import CampaignSnapshotStore, build_snapshot_spec
from data_quality.reports import DataQualityReportWriter
from data_quality.suites import DataQualityRunner
from semantic_layer.docs import SemanticRegistryWriter
from serving.builder import ServingLayerBuilder
from serving.writer import ServingLayerWriter


@dataclass(frozen=True)
class PipelineStepResult:
    step: str
    status: str
    outputs: dict[str, str]
    message: str = ""


class LakehouseOrchestrator:
    def __init__(self, paths) -> None:
        self.paths = paths

    def run(
        self,
        *,
        tenant_id: str,
        campaign_id: str,
        snapshot_id: str,
        dataset_version: str,
        budget_total: float = 200000.0,
        scenario_id: str = "hibrido",
    ) -> dict[str, Any]:
        steps: list[PipelineStepResult] = []
        tables: dict[str, pd.DataFrame] = {}
        master_path = self._find_latest("gold_territorial_electoral_master_index") or self._find_latest("master_index")
        zone_fallback_path = self._find_latest("fact_zona_eleitoral")

        if master_path is None and zone_fallback_path is None:
            steps.append(
                PipelineStepResult(
                    step="master_index",
                    status="skipped",
                    outputs={},
                    message="Not found in repo: master index gold. Execute bronze/silver/master antes do gold.",
                )
            )
            report = self._write_run_report(steps, tenant_id, campaign_id, snapshot_id, dataset_version)
            return {"status": "skipped", "report_path": str(report), "steps": [step.__dict__ for step in steps]}

        if master_path is not None:
            master = pd.read_parquet(master_path)
            source_path = master_path
            source_message = f"{len(master)} registros carregados."
        else:
            if zone_fallback_path is None:
                raise RuntimeError("fact_zona_eleitoral fallback indisponivel")
            master = self._zone_fact_as_master(pd.read_parquet(zone_fallback_path))
            source_path = zone_fallback_path
            source_message = (
                f"{len(master)} zonas carregadas via fallback fact_zona_eleitoral; "
                "limitacao: granularidade secao/local/setor nao disponivel neste fallback."
            )
        tables["gold_territorial_electoral_master_index"] = master
        steps.append(
            PipelineStepResult(
                step="master_index",
                status="ok",
                outputs={"master_index": str(source_path)},
                message=source_message,
            )
        )
        gold_tables = GoldMartBuilder().build_all(
            master_index=master,
            budget_total=budget_total,
            scenario_id=scenario_id,
        )
        gold_result = GoldMartWriter().write_all(
            gold_tables,
            output_dir=self.paths.lakehouse_root / "gold" / "marts",
            dataset_version=dataset_version,
        )
        tables.update(gold_tables)
        steps.append(
            PipelineStepResult(
                step="gold",
                status="ok",
                outputs={item.table_name: item.parquet_path for item in gold_result.outputs},
                message="Marts gold recalculados a partir do master index.",
            )
        )

        quality = DataQualityRunner().run_lake(tables)
        quality_root = self.paths.lakehouse_root / "quality" / dataset_version
        quality_json = DataQualityReportWriter().write_json(quality, quality_root / "lake_health_report.json")
        quality_md = DataQualityReportWriter().write_markdown(quality, quality_root / "lake_health_report.md")
        tables["lake_health_report"] = pd.DataFrame([quality.model_dump(mode="json")])
        steps.append(
            PipelineStepResult(
                step="quality",
                status="ok",
                outputs={"json": str(quality_json), "markdown": str(quality_md)},
                message="Data quality calculado para tabelas disponíveis.",
            )
        )

        semantic_root = self.paths.lakehouse_root / "semantic" / dataset_version
        semantic_json = SemanticRegistryWriter().write_json(semantic_root / "semantic_registry.json")
        semantic_md = SemanticRegistryWriter().write_markdown(semantic_root / "semantic_registry.md")
        steps.append(
            PipelineStepResult(
                step="semantic",
                status="ok",
                outputs={"json": str(semantic_json), "markdown": str(semantic_md)},
                message="Semantic registry exportado.",
            )
        )

        commercial_result = CommercialMartBuilder().build(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            gold_tables=tables,
        )
        snapshot_spec = build_snapshot_spec(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            candidate_ids=self._candidate_ids(tables),
            snapshot_id=snapshot_id,
            dataset_version=dataset_version,
            source_tables=list(tables),
        )
        snapshot_paths = CampaignSnapshotStore(self.paths.data_root).write_snapshot(
            spec=snapshot_spec,
            marts=commercial_result.marts,
        )
        export_manifest = CommercialExportService().export(
            marts=commercial_result.marts,
            output_dir=self.paths.artifact_root / "commercial" / tenant_id / campaign_id / snapshot_id,
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
        )
        steps.append(
            PipelineStepResult(
                step="commercial",
                status="ok",
                outputs={
                    "snapshot": str(snapshot_paths["snapshot"]),
                    "export_manifest": export_manifest.exported_files.get("commercial_export_manifest.json", ""),
                },
                message="Marts comerciais, snapshot e exports gerados.",
            )
        )

        serving_result = ServingLayerBuilder().build(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            dataset_version=dataset_version,
            tables=tables,
        )
        serving_manifest = ServingLayerWriter(self.paths.lakehouse_root).write(
            result=serving_result,
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            dataset_version=dataset_version,
            source_tables=list(tables),
        )
        steps.append(
            PipelineStepResult(
                step="serving",
                status="ok",
                outputs={"manifest": serving_manifest.outputs["_manifest"]["json"]},
                message="Outputs serving materializados para API/UI.",
            )
        )

        report = self._write_run_report(steps, tenant_id, campaign_id, snapshot_id, dataset_version)
        return {"status": "ok", "report_path": str(report), "steps": [step.__dict__ for step in steps]}

    def coverage_by_zone_section(self) -> dict[str, Any]:
        master_path = self._find_latest("gold_territorial_electoral_master_index") or self._find_latest("master_index")
        zone_fallback_path = self._find_latest("fact_zona_eleitoral")
        if master_path is None:
            if zone_fallback_path is None:
                return {"status": "skipped", "message": "Not found in repo: master index"}
            df = pd.read_parquet(zone_fallback_path)
            source_path = zone_fallback_path
        else:
            df = pd.read_parquet(master_path)
            source_path = master_path
        zone_col = next((column for column in ["zona", "ZONA", "zona_eleitoral"] if column in df.columns), None)
        section_col = next((column for column in ["secao", "SECAO"] if column in df.columns), None)
        municipio_col = next(
            (column for column in ["municipio_nome", "MUNICIPIO", "municipio"] if column in df.columns),
            None,
        )
        coverage = {
            "status": "ok",
            "source_path": str(source_path),
            "rows": int(len(df)),
            "zonas": int(df[[municipio_col, zone_col]].drop_duplicates().shape[0])
            if zone_col and municipio_col
            else int(df[zone_col].nunique())
            if zone_col
            else 0,
            "secoes": int(df[[municipio_col, zone_col, section_col]].drop_duplicates().shape[0])
            if section_col and zone_col and municipio_col
            else int(df[section_col].nunique())
            if section_col
            else 0,
            "municipios": int(df[municipio_col].nunique()) if municipio_col else 0,
            "join_confidence_avg": float(pd.to_numeric(df.get("join_confidence", 0), errors="coerce").fillna(0).mean()),
        }
        path = self.paths.lakehouse_root / "quality" / "coverage_by_zone_section.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(coverage, ensure_ascii=False, indent=2), encoding="utf-8")
        coverage["report_path"] = str(path)
        return coverage

    def _zone_fact_as_master(self, zones: pd.DataFrame) -> pd.DataFrame:
        def text(column: str, default: str = "") -> pd.Series:
            if column in zones.columns:
                return zones[column].astype(str)
            return pd.Series([default] * len(zones), index=zones.index)

        def number(column: str, default: float = 0.0) -> pd.Series:
            if column in zones.columns:
                return pd.to_numeric(zones[column], errors="coerce").fillna(default)
            return pd.Series([default] * len(zones), index=zones.index)

        out = pd.DataFrame(
            {
                "master_record_id": text("territorio_id", "zone").astype(str),
                "ano_eleicao": number("ano_eleicao", 0).astype(int),
                "uf": text("uf").str.upper(),
                "cod_municipio_tse": text("cod_tse_municipio").str.zfill(5),
                "cod_municipio_ibge": text("municipio_id_ibge7"),
                "municipio_nome": text("municipio"),
                "zona": text("zona_eleitoral").str.zfill(4),
                "secao": "",
                "local_votacao": "",
                "candidate_id": "aggregate",
                "numero_candidato": "",
                "partido": "",
                "cd_setor": "",
                "territorial_cluster_id": text("zona_id"),
                "join_strategy": text("match_method", "zone_fact_fallback"),
                "join_confidence": number("join_confidence", 0.0),
                "source_coverage_score": number("data_quality_score", 0.0),
            }
        )
        return out

    def _find_latest(self, name_fragment: str) -> Path | None:
        roots = [self.paths.lakehouse_root, self.paths.lake_root, self.paths.gold_root]
        candidates: list[Path] = []
        for root in roots:
            if root.exists():
                candidates.extend(
                    path
                    for path in root.rglob("*.parquet")
                    if name_fragment in path.name or name_fragment in str(path.parent)
                )
        return sorted(candidates, key=lambda path: path.stat().st_mtime, reverse=True)[0] if candidates else None

    def _candidate_ids(self, tables: dict[str, pd.DataFrame]) -> list[str]:
        values: set[str] = set()
        for table in tables.values():
            if "candidate_id" in table.columns:
                values.update(str(value) for value in table["candidate_id"].dropna().unique() if str(value).strip())
        return sorted(values)

    def _write_run_report(
        self,
        steps: list[PipelineStepResult],
        tenant_id: str,
        campaign_id: str,
        snapshot_id: str,
        dataset_version: str,
    ) -> Path:
        payload = {
            "tenant_id": tenant_id,
            "campaign_id": campaign_id,
            "snapshot_id": snapshot_id,
            "dataset_version": dataset_version,
            "steps": [step.__dict__ for step in steps],
        }
        path = self.paths.lakehouse_root / "manifests" / "orchestrated_runs" / f"{dataset_version}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path
