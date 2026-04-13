from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

from config.settings import AppPaths


@dataclass(frozen=True)
class ScheduledPipeline:
    name: str
    cadence: str
    task_type: str
    enabled: bool
    schedule: dict[str, Any]
    alert_on_failure: bool = True


def build_default_schedule(*, tenant_id: str, daily_hour: int, weekly_day: str, weekly_hour: int) -> list[ScheduledPipeline]:
    return [
        ScheduledPipeline(
            name="ingestao_diaria",
            cadence="daily",
            task_type="ingest",
            enabled=True,
            schedule={"hour": int(daily_hour), "timezone": "America/Sao_Paulo"},
        ),
        ScheduledPipeline(
            name="atualizacao_semanal_gold",
            cadence="weekly",
            task_type="medallion_update",
            enabled=True,
            schedule={"day": weekly_day, "hour": int(weekly_hour), "timezone": "America/Sao_Paulo"},
        ),
        ScheduledPipeline(
            name="alertas_operacionais",
            cadence="hourly",
            task_type="ops_alerts",
            enabled=True,
            schedule={"interval_hours": 1, "timezone": "America/Sao_Paulo"},
        ),
    ]


def write_schedule_manifest(paths: AppPaths, schedules: list[ScheduledPipeline]) -> Path:
    target = paths.catalog_root / f"ops_schedule_{paths.tenant_id}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "tenant_id": paths.tenant_id,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "pipelines": [asdict(item) for item in schedules],
    }
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def load_schedule_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
