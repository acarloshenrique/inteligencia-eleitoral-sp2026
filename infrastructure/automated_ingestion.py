from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from config.settings import AppPaths
from infrastructure.load_manifest import build_load_manifest
from infrastructure.medallion_pipeline import MedallionInputs, run_medallion_pipeline
from infrastructure.open_data_pipeline import OpenDataInputs, run_open_data_crosswalk_pipeline
from infrastructure.open_data_sources import OpenDataAsset, download_asset_incremental
from infrastructure.tse_zone_pipeline import TSEZoneInputs, run_tse_zone_section_pipeline


class AutomatedIngestionError(RuntimeError):
    pass


ALLOWED_SOURCE_DOMAINS = {
    "eleitoral_oficial",
    "socioeconomico",
    "territorial",
    "midia_e_social",
    "operacoes_de_campanha",
}

ROLE_DOMAIN_DEFAULTS = {
    "base_parquet": "eleitoral_oficial",
    "mapping_csv": "eleitoral_oficial",
    "secao_csv": "eleitoral_oficial",
    "socio_csv": "socioeconomico",
    "ibge_csv": "socioeconomico",
    "seade_csv": "territorial",
    "social_csv": "midia_e_social",
    "meta_ads_csv": "midia_e_social",
    "google_ads_csv": "midia_e_social",
    "operacoes_csv": "operacoes_de_campanha",
    "fiscal_csv": "operacoes_de_campanha",
    "tse_resultados_zona_csv": "eleitoral_oficial",
    "tse_eleitorado_zona_csv": "eleitoral_oficial",
}


@dataclass(frozen=True)
class IngestionAssetSpec:
    name: str
    role: str
    url: str
    file_name: str
    format: str
    domain: str
    required: bool = True


@dataclass(frozen=True)
class IngestionCatalog:
    pipeline: str
    pipeline_version: str
    assets: tuple[IngestionAssetSpec, ...]


def _normalize_domain(value: str) -> str:
    return str(value).strip().lower()


def _validate_domain(value: str) -> str:
    domain = _normalize_domain(value)
    if domain not in ALLOWED_SOURCE_DOMAINS:
        raise AutomatedIngestionError(f"dominio de fonte invalido: {value}. Use um de {sorted(ALLOWED_SOURCE_DOMAINS)}")
    return domain


def _build_asset_spec(item: dict[str, Any], *, domain: str | None = None) -> IngestionAssetSpec:
    role = str(item["role"])
    inferred_domain = ROLE_DOMAIN_DEFAULTS.get(role, "")
    resolved_domain = _validate_domain(domain or str(item.get("domain", "")).strip() or inferred_domain)
    return IngestionAssetSpec(
        name=str(item["name"]),
        role=role,
        url=str(item["url"]),
        file_name=str(item["file_name"]),
        format=str(item.get("format", Path(str(item["file_name"])).suffix.lstrip(".") or "csv")).lower(),
        domain=resolved_domain,
        required=bool(item.get("required", True)),
    )


def _load_catalog(catalog_path: Path) -> IngestionCatalog:
    payload = json.loads(catalog_path.read_text(encoding="utf-8"))
    assets_raw = payload.get("assets", [])
    domains_raw = payload.get("domains", [])
    assets: list[IngestionAssetSpec] = []
    for item in assets_raw:
        assets.append(_build_asset_spec(item))
    for domain_entry in domains_raw:
        domain_name = _validate_domain(str(domain_entry.get("domain", "")))
        for item in domain_entry.get("assets", []):
            assets.append(_build_asset_spec(item, domain=domain_name))
    if not assets:
        raise AutomatedIngestionError("catalogo de ingestao sem assets")
    pipeline = str(payload.get("pipeline", "")).strip().lower()
    if pipeline not in {"open_data", "medallion", "tse_zone_section"}:
        raise AutomatedIngestionError("pipeline do catalogo deve ser 'open_data', 'medallion' ou 'tse_zone_section'")
    return IngestionCatalog(
        pipeline=pipeline,
        pipeline_version=str(payload.get("pipeline_version", f"{pipeline}_v1")),
        assets=assets,
    )


def _validate_downloaded_asset(asset: IngestionAssetSpec, path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not path.exists():
        raise AutomatedIngestionError(f"asset baixado nao encontrado: {asset.name}")
    fmt = asset.format.lower()
    if fmt == "parquet":
        df = pd.read_parquet(path)
    elif fmt == "csv":
        df = pd.read_csv(path)
    else:
        raise AutomatedIngestionError(f"formato nao suportado para ingestao automatizada: {asset.format}")
    if df.empty and asset.required:
        raise AutomatedIngestionError(f"asset obrigatorio vazio: {asset.name}")
    quality = {
        "status": "ok" if not df.empty else "warning",
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "null_pct": round(
            float(df.isna().sum().sum() / max(1, df.shape[0] * max(1, df.shape[1]))) * 100.0,
            3,
        )
        if not df.empty
        else 0.0,
    }
    return df, quality


def _compact_utc(value: str) -> str:
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    return dt.astimezone(UTC).strftime("%Y%m%d_%H%M%S")


def _persist_raw_asset_to_bronze(
    *,
    paths: AppPaths,
    asset: IngestionAssetSpec,
    source_path: Path,
    collected_at_utc: str,
) -> Path:
    coleta = _compact_utc(collected_at_utc)
    bronze_dir = paths.bronze_root / asset.domain / f"fonte={asset.name}" / f"coleta={coleta}"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    bronze_path = bronze_dir / source_path.name
    shutil.copy2(source_path, bronze_path)
    return bronze_path


def _download_and_validate_assets(
    *,
    paths: AppPaths,
    catalog: IngestionCatalog,
    catalog_path: Path,
) -> tuple[dict[str, Path | None], list[dict[str, Any]]]:
    download_dir = paths.ingestion_root / "downloads" / catalog_path.stem
    download_dir.mkdir(parents=True, exist_ok=True)

    role_paths: dict[str, Path | None] = {}
    asset_manifests: list[dict[str, Any]] = []

    for asset in catalog.assets:
        download_dir = paths.ingestion_root / "downloads" / asset.domain / catalog_path.stem
        download_dir.mkdir(parents=True, exist_ok=True)
        result = download_asset_incremental(
            asset=OpenDataAsset(name=asset.name, url=asset.url, file_name=asset.file_name),
            output_dir=download_dir,
        )
        asset_path = Path(str(result["path"])).resolve()
        if not asset_path.exists():
            if asset.required:
                raise AutomatedIngestionError(f"asset obrigatorio ausente apos download: {asset.name}")
            role_paths[asset.role] = None
            continue

        df, quality = _validate_downloaded_asset(asset, asset_path)
        role_paths[asset.role] = asset_path
        collected_at_utc = str(result.get("downloaded_at_utc", datetime.now(UTC).isoformat()))
        bronze_path = _persist_raw_asset_to_bronze(
            paths=paths,
            asset=asset,
            source_path=asset_path,
            collected_at_utc=collected_at_utc,
        )
        asset_manifests.append(
            {
                **build_load_manifest(
                    source_name=asset.name,
                    collected_at_utc=collected_at_utc,
                    dataset_path=asset_path,
                    df=df,
                    parser_version=f"download_http::{catalog.pipeline_version}",
                    quality={**quality, "download_status": result.get("status", "downloaded")},
                ),
                "dominio_fonte": asset.domain,
                "role": asset.role,
                "bronze_path": str(bronze_path),
            }
        )

    return role_paths, asset_manifests


def _summarize_domains(asset_manifests: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, dict[str, Any]] = {}
    for item in asset_manifests:
        domain = str(item.get("dominio_fonte", "")).strip()
        bucket = grouped.setdefault(domain, {"assets": [], "total_rows": 0})
        bucket["assets"].append(
            {
                "fonte": item.get("fonte"),
                "role": item.get("role"),
                "arquivo": item.get("arquivo"),
            }
        )
        quality = item.get("qualidade_carga", {})
        bucket["total_rows"] += int(quality.get("rows", 0) or 0)
    return grouped


def _require_path(role_paths: dict[str, Path | None], role: str) -> Path:
    path = role_paths.get(role)
    if path is None:
        raise AutomatedIngestionError(f"asset obrigatorio nao configurado para role={role}")
    return path


def run_automated_ingestion(
    *,
    paths: AppPaths,
    catalog_path: Path,
    pipeline: str | None = None,
    pipeline_version: str | None = None,
) -> dict[str, Any]:
    if not catalog_path.exists():
        raise AutomatedIngestionError(f"catalogo de ingestao nao encontrado: {catalog_path}")

    catalog = _load_catalog(catalog_path)
    selected_pipeline = (pipeline or catalog.pipeline).strip().lower()
    selected_version = str(pipeline_version or catalog.pipeline_version)
    run_id = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

    role_paths, asset_manifests = _download_and_validate_assets(paths=paths, catalog=catalog, catalog_path=catalog_path)

    if selected_pipeline == "open_data":
        pipeline_result = run_open_data_crosswalk_pipeline(
            paths=paths,
            inputs=OpenDataInputs(
                base_parquet_path=_require_path(role_paths, "base_parquet"),
                mapping_csv_path=_require_path(role_paths, "mapping_csv"),
                socio_csv_path=role_paths.get("socio_csv"),
            ),
            pipeline_version=selected_version,
        )
    elif selected_pipeline == "tse_zone_section":
        pipeline_result = run_tse_zone_section_pipeline(
            paths=paths,
            inputs=TSEZoneInputs(
                eleitorado_path=_require_path(role_paths, "tse_eleitorado_zona_csv"),
                resultados_path=role_paths.get("tse_resultados_zona_csv"),
                uf=str(getattr(paths, "uf", "SP")),
                ano_eleicao=int(2024),
                turno=int(1),
            ),
            pipeline_version=selected_version,
        )
    elif selected_pipeline == "medallion":
        pipeline_result = run_medallion_pipeline(
            paths,
            MedallionInputs(
                base_parquet_path=_require_path(role_paths, "base_parquet"),
                mapping_csv_path=_require_path(role_paths, "mapping_csv"),
                socio_csv_path=role_paths.get("socio_csv"),
                secao_csv_path=role_paths.get("secao_csv"),
                ibge_csv_path=role_paths.get("ibge_csv"),
                seade_csv_path=role_paths.get("seade_csv"),
                social_csv_path=role_paths.get("social_csv"),
                meta_ads_csv_path=role_paths.get("meta_ads_csv"),
                google_ads_csv_path=role_paths.get("google_ads_csv"),
                fiscal_csv_path=role_paths.get("fiscal_csv"),
            ),
            pipeline_version=selected_version,
        )
    else:
        raise AutomatedIngestionError("pipeline de ingestao automatizada invalido")

    orchestration_manifest = {
        "run_id": run_id,
        "pipeline": selected_pipeline,
        "pipeline_version": selected_version,
        "catalog_path": str(catalog_path),
        "executed_at_utc": datetime.now(UTC).isoformat(),
        "dominios": _summarize_domains(asset_manifests),
        "downloads": asset_manifests,
        "promotion_result": pipeline_result,
    }
    manifest_dir = paths.ingestion_root / "pipeline_runs" / "automated_ingestion" / run_id
    manifest_dir.mkdir(parents=True, exist_ok=True)
    orchestration_manifest_path = manifest_dir / "manifest.json"
    orchestration_manifest_path.write_text(
        json.dumps(orchestration_manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return {
        "run_id": run_id,
        "pipeline": selected_pipeline,
        "pipeline_version": selected_version,
        "downloads": asset_manifests,
        "manifest_path": str(orchestration_manifest_path),
        "promotion_result": pipeline_result,
    }
