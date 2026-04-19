from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


class ServingDataNotFoundError(FileNotFoundError):
    pass


@dataclass(frozen=True)
class ServingReadResult:
    output_id: str
    tenant_id: str
    campaign_id: str
    snapshot_id: str
    path: Path
    records: list[dict[str, Any]]
    row_count: int
    warnings: list[str]


class ServingOutputService:
    def __init__(self, paths) -> None:
        self.paths = paths

    def manifest(
        self,
        *,
        tenant_id: str | None = None,
        campaign_id: str | None = None,
        snapshot_id: str | None = None,
    ) -> dict[str, Any]:
        root = self._resolve_snapshot_root(
            tenant_id=tenant_id or self.paths.tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
        )
        manifest_path = root / "serving_manifest.json"
        if not manifest_path.exists():
            raise ServingDataNotFoundError(f"Not found in repo: {manifest_path}")
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def read_output(
        self,
        output_id: str,
        *,
        tenant_id: str | None = None,
        campaign_id: str | None = None,
        snapshot_id: str | None = None,
        filters: dict[str, Any] | None = None,
        limit: int = 100,
    ) -> ServingReadResult:
        effective_tenant = tenant_id or self.paths.tenant_id
        root = self._resolve_snapshot_root(
            tenant_id=effective_tenant,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
        )
        output_path = self._resolve_output_path(
            output_id,
            tenant_id=effective_tenant,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            preferred_root=root,
        )
        if not output_path.exists():
            raise ServingDataNotFoundError(f"Not found in repo: {output_id}")
        root = output_path.parent.parent

        frame = pd.read_parquet(output_path) if output_path.suffix == ".parquet" else pd.read_csv(output_path)
        frame = self._apply_filters(frame, filters or {})
        if limit > 0:
            frame = frame.head(limit)
        manifest = self._read_manifest(root)
        return ServingReadResult(
            output_id=output_id,
            tenant_id=effective_tenant,
            campaign_id=self._value_from_frame_or_root(frame, root, "campaign_id"),
            snapshot_id=self._value_from_frame_or_root(frame, root, "snapshot_id"),
            path=output_path,
            records=frame.fillna("").to_dict(orient="records"),
            row_count=int(len(frame)),
            warnings=list(manifest.get("warnings", [])),
        )

    def territory_ranking(
        self,
        *,
        tenant_id: str | None = None,
        campaign_id: str | None = None,
        snapshot_id: str | None = None,
        candidate_id: str | None = None,
        limit: int = 50,
    ) -> ServingReadResult:
        filters = {"candidate_id": candidate_id} if candidate_id else {}
        return self.read_output(
            "serving_territory_ranking",
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            filters=filters,
            limit=limit,
        )

    def allocation_recommendations(
        self,
        *,
        tenant_id: str | None = None,
        campaign_id: str | None = None,
        snapshot_id: str | None = None,
        candidate_id: str | None = None,
        scenario_id: str | None = None,
        limit: int = 50,
    ) -> ServingReadResult:
        filters: dict[str, Any] = {}
        if candidate_id:
            filters["candidate_id"] = candidate_id
        if scenario_id:
            filters["scenario_id"] = scenario_id
        return self.read_output(
            "serving_allocation_recommendations",
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            filters=filters,
            limit=limit,
        )

    def data_readiness(
        self,
        *,
        tenant_id: str | None = None,
        campaign_id: str | None = None,
        snapshot_id: str | None = None,
    ) -> ServingReadResult:
        return self.read_output(
            "serving_data_readiness",
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            limit=1,
        )

    def zone_ranking(
        self,
        *,
        tenant_id: str | None = None,
        campaign_id: str | None = None,
        snapshot_id: str | None = None,
        candidate_id: str | None = None,
        municipio_nome: str | None = None,
        limit: int = 100,
    ) -> ServingReadResult:
        filters: dict[str, Any] = {}
        if candidate_id:
            filters["candidate_id"] = candidate_id
        if municipio_nome:
            filters["municipio_nome"] = municipio_nome
        return self.read_output(
            "serving_zone_ranking",
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            filters=filters,
            limit=limit,
        )

    def municipality_zone_detail(
        self,
        *,
        tenant_id: str | None = None,
        campaign_id: str | None = None,
        snapshot_id: str | None = None,
        candidate_id: str | None = None,
        municipio_nome: str | None = None,
        limit: int = 200,
    ) -> ServingReadResult:
        filters: dict[str, Any] = {}
        if candidate_id:
            filters["candidate_id"] = candidate_id
        if municipio_nome:
            filters["municipio_nome"] = municipio_nome
        return self.read_output(
            "serving_municipality_zone_detail",
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            filters=filters,
            limit=limit,
        )

    def _apply_filters(self, frame: pd.DataFrame, filters: dict[str, Any]) -> pd.DataFrame:
        out = frame
        for column, value in filters.items():
            if value is None or column not in out.columns:
                continue
            out = out[out[column].astype(str).eq(str(value))]
        return out

    def _read_manifest(self, root: Path) -> dict[str, Any]:
        path = root / "serving_manifest.json"
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def _resolve_snapshot_root(
        self,
        *,
        tenant_id: str,
        campaign_id: str | None,
        snapshot_id: str | None,
    ) -> Path:
        existing = [path for path in self._candidate_serving_roots(tenant_id) if path.exists()]
        if not existing:
            raise ServingDataNotFoundError("Not found in repo: serving layer")
        campaign_roots = self._campaign_roots(existing, campaign_id)
        snapshot_roots = self._snapshot_roots(campaign_roots, snapshot_id)
        if not snapshot_roots:
            raise ServingDataNotFoundError("Not found in repo: serving snapshot")
        return sorted(snapshot_roots, key=lambda path: path.stat().st_mtime, reverse=True)[0]

    def _resolve_output_path(
        self,
        output_id: str,
        *,
        tenant_id: str,
        campaign_id: str | None,
        snapshot_id: str | None,
        preferred_root: Path,
    ) -> Path:
        preferred = self._output_candidates(preferred_root, output_id)
        for path in preferred:
            if path.exists():
                return path
        existing = [path for path in self._candidate_serving_roots(tenant_id) if path.exists()]
        roots = self._snapshot_roots(self._campaign_roots(existing, campaign_id), snapshot_id)
        for root in sorted(roots, key=lambda path: path.stat().st_mtime, reverse=True):
            for path in self._output_candidates(root, output_id):
                if path.exists():
                    return path
        return preferred[0]

    def _output_candidates(self, root: Path, output_id: str) -> list[Path]:
        return [
            root / output_id / f"{output_id}.parquet",
            root / output_id / "data.parquet",
            root / output_id / f"{output_id}.csv",
            root / output_id / "data.csv",
        ]

    def _candidate_serving_roots(self, tenant_id: str) -> list[Path]:
        roots = [
            self.paths.lakehouse_root / "serving",
            self.paths.data_root / "lake" / "serving",
            self.paths.data_root / "lake" / "tenants" / tenant_id / "serving",
            self.paths.data_root / "tenants" / tenant_id / "lake" / "serving",
            self.paths.gold_serving_root,
        ]
        return list(dict.fromkeys(path.resolve() for path in roots))

    def _campaign_roots(self, serving_roots: list[Path], campaign_id: str | None) -> list[Path]:
        if campaign_id:
            return [
                root / f"campaign_id={campaign_id}"
                for root in serving_roots
                if (root / f"campaign_id={campaign_id}").exists()
            ]
        roots: list[Path] = []
        for root in serving_roots:
            roots.extend(path for path in root.glob("campaign_id=*") if path.is_dir())
        return roots

    def _snapshot_roots(self, campaign_roots: list[Path], snapshot_id: str | None) -> list[Path]:
        if snapshot_id:
            return [
                root / f"snapshot_id={snapshot_id}"
                for root in campaign_roots
                if (root / f"snapshot_id={snapshot_id}").exists()
            ]
        roots: list[Path] = []
        for root in campaign_roots:
            roots.extend(path for path in root.glob("snapshot_id=*") if path.is_dir())
        return roots

    def _value_from_frame_or_root(self, frame: pd.DataFrame, root: Path, column: str) -> str:
        if column in frame.columns and not frame.empty:
            return str(frame[column].iloc[0])
        prefix = f"{column}="
        for part in root.parts:
            if part.startswith(prefix):
                return part.removeprefix(prefix)
        return ""
