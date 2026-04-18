from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from infrastructure.tenancy import ensure_tenant_path, tenant_root_for
from serving.builder import ServingBuildResult
from serving.models import ServingOutputManifest, ServingOutputWriteResult


class ServingLayerWriter:
    def __init__(self, lake_root: Path) -> None:
        self.lake_root = lake_root

    def serving_root(self, *, tenant_id: str, campaign_id: str, snapshot_id: str) -> Path:
        tenant_root = tenant_root_for(self.lake_root, tenant_id)
        root = tenant_root / "serving" / f"campaign_id={campaign_id}" / f"snapshot_id={snapshot_id}"
        return ensure_tenant_path(tenant_root, root)

    def write(
        self,
        *,
        result: ServingBuildResult,
        tenant_id: str,
        campaign_id: str,
        snapshot_id: str,
        dataset_version: str,
        source_tables: list[str],
    ) -> ServingOutputManifest:
        root = self.serving_root(tenant_id=tenant_id, campaign_id=campaign_id, snapshot_id=snapshot_id)
        root.mkdir(parents=True, exist_ok=True)
        outputs: dict[str, dict[str, str]] = {}
        row_counts: dict[str, int] = {}
        quality: dict[str, dict[str, float | int | str | bool]] = {}
        for output_id, frame in result.outputs.items():
            write_result = self._write_frame(root=root, output_id=output_id, frame=frame)
            outputs[output_id] = {
                "parquet": write_result.parquet_path,
                "csv": write_result.csv_path,
                "json": write_result.json_path,
            }
            row_counts[output_id] = write_result.rows
            quality[output_id] = self._quality(frame)

        spec_path = root / "serving_output_specs.json"
        spec_path.write_text(
            json.dumps(
                {key: spec.model_dump(mode="json") for key, spec in result.specs.items()},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        outputs["_specs"] = {"json": str(spec_path)}

        manifest = ServingOutputManifest(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            dataset_version=dataset_version,
            generated_at_utc=result.generated_at_utc,
            outputs=outputs,
            row_counts=row_counts,
            quality=quality,
            source_tables=sorted(set(source_tables)),
            warnings=result.warnings,
        )
        manifest_path = root / "serving_manifest.json"
        manifest_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        manifest.outputs["_manifest"] = {"json": str(manifest_path)}
        return manifest

    def _write_frame(self, *, root: Path, output_id: str, frame: pd.DataFrame) -> ServingOutputWriteResult:
        output_root = root / output_id
        output_root.mkdir(parents=True, exist_ok=True)
        parquet_path = output_root / f"{output_id}.parquet"
        csv_path = output_root / f"{output_id}.csv"
        json_path = output_root / f"{output_id}.json"
        try:
            frame.to_parquet(parquet_path, index=False)
        except FileNotFoundError:
            parquet_path = output_root / "data.parquet"
            frame.to_parquet(parquet_path, index=False)
        try:
            frame.to_csv(csv_path, index=False, encoding="utf-8")
        except FileNotFoundError:
            csv_path = output_root / "data.csv"
            frame.to_csv(csv_path, index=False, encoding="utf-8")
        try:
            json_path.write_text(frame.to_json(orient="records", force_ascii=False, indent=2), encoding="utf-8")
        except FileNotFoundError:
            json_path = output_root / "data.json"
            json_path.write_text(frame.to_json(orient="records", force_ascii=False, indent=2), encoding="utf-8")
        return ServingOutputWriteResult(
            output_id=output_id,
            rows=int(len(frame)),
            parquet_path=str(parquet_path),
            csv_path=str(csv_path),
            json_path=str(json_path),
        )

    def _quality(self, frame: pd.DataFrame) -> dict[str, float | int | str | bool]:
        quality: dict[str, float | int | str | bool] = {
            "rows": int(len(frame)),
            "columns": int(len(frame.columns)),
            "empty": bool(frame.empty),
        }
        for column in ["score_prioridade_final", "confidence_score", "readiness_score", "avg_join_confidence"]:
            if column not in frame.columns:
                continue
            values = pd.to_numeric(frame[column], errors="coerce")
            quality[f"{column}_min"] = round(float(values.min()), 6) if len(values.dropna()) else 0.0
            quality[f"{column}_max"] = round(float(values.max()), 6) if len(values.dropna()) else 0.0
            quality[f"{column}_nulls"] = int(values.isna().sum())
        return quality
