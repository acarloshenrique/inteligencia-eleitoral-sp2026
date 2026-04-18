from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import pandas as pd

from commercial.models import CampaignSnapshotSpec
from infrastructure.tenancy import ensure_tenant_path, tenant_root_for


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def build_snapshot_id(*, tenant_id: str, campaign_id: str, dataset_version: str, candidate_ids: list[str]) -> str:
    raw = "|".join([tenant_id, campaign_id, dataset_version, ",".join(sorted(candidate_ids))])
    return str(uuid5(NAMESPACE_URL, f"commercial-snapshot:{raw}"))


def build_snapshot_spec(
    *,
    tenant_id: str,
    campaign_id: str,
    candidate_ids: list[str],
    snapshot_id: str,
    dataset_version: str,
    source_tables: list[str],
) -> CampaignSnapshotSpec:
    return CampaignSnapshotSpec(
        tenant_id=tenant_id,
        campaign_id=campaign_id,
        candidate_ids=sorted(set(candidate_ids)),
        snapshot_id=snapshot_id,
        dataset_version=dataset_version,
        generated_at_utc=utc_now_iso(),
        source_tables=sorted(set(source_tables)),
    )


class CampaignSnapshotBuilder:
    def build_spec(
        self,
        *,
        tenant_id: str,
        campaign_id: str,
        dataset_version: str,
        gold_tables: dict[str, pd.DataFrame],
    ) -> CampaignSnapshotSpec:
        candidate_ids = self._candidate_ids(gold_tables)
        snapshot_id = build_snapshot_id(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            dataset_version=dataset_version,
            candidate_ids=candidate_ids,
        )
        return CampaignSnapshotSpec(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            candidate_ids=candidate_ids,
            snapshot_id=snapshot_id,
            dataset_version=dataset_version,
            generated_at_utc=utc_now_iso(),
            source_tables=sorted(gold_tables),
        )

    def write_spec(self, spec: CampaignSnapshotSpec, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(spec.model_dump(mode="json"), ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def _candidate_ids(self, gold_tables: dict[str, pd.DataFrame]) -> list[str]:
        ids: set[str] = set()
        for table in gold_tables.values():
            if "candidate_id" in table.columns:
                ids.update(str(value) for value in table["candidate_id"].dropna().unique() if str(value).strip())
        return sorted(ids)


class CampaignSnapshotStore:
    """Stores immutable campaign snapshots below the tenant-specific lake root."""

    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root

    def snapshot_root(self, *, tenant_id: str, campaign_id: str, snapshot_id: str) -> Path:
        tenant_root = tenant_root_for(self.data_root, tenant_id)
        root = tenant_root / "commercial_snapshots" / campaign_id / snapshot_id
        return ensure_tenant_path(tenant_root, root)

    def write_snapshot(
        self,
        *,
        spec: CampaignSnapshotSpec,
        marts: dict[str, pd.DataFrame],
    ) -> dict[str, Path]:
        root = self.snapshot_root(
            tenant_id=spec.tenant_id,
            campaign_id=spec.campaign_id,
            snapshot_id=spec.snapshot_id,
        )
        root.mkdir(parents=True, exist_ok=True)
        written: dict[str, Path] = {}
        for name, frame in marts.items():
            path = root / f"{name}.parquet"
            frame.to_parquet(path, index=False)
            written[name] = path
        spec_path = root / "snapshot.json"
        spec_path.write_text(spec.model_dump_json(indent=2), encoding="utf-8")
        written["snapshot"] = spec_path
        return written

    def read_snapshot_manifest(
        self,
        *,
        tenant_id: str,
        campaign_id: str,
        snapshot_id: str,
    ) -> CampaignSnapshotSpec:
        root = self.snapshot_root(tenant_id=tenant_id, campaign_id=campaign_id, snapshot_id=snapshot_id)
        payload = (root / "snapshot.json").read_text(encoding="utf-8")
        return CampaignSnapshotSpec.model_validate_json(payload)
