from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

from config.settings import AppPaths
from domain.contracts import validate_municipios_input
from infrastructure.dataset_catalog import build_dataset_metadata, register_dataset_version
from infrastructure.env import df_municipios_vazio
from infrastructure.load_manifest import build_load_manifest

PIPELINE_VERSION = "v1"


class PipelineError(RuntimeError):
    pass


@dataclass(frozen=True)
class DagNode:
    name: str
    deps: tuple[str, ...]
    fn: Callable[[dict[str, Any]], dict[str, Any]]


class SimpleDag:
    def __init__(self, nodes: list[DagNode]):
        self._nodes = {n.name: n for n in nodes}
        if len(self._nodes) != len(nodes):
            raise PipelineError("DAG invalido: nomes de nos duplicados")

    def _topological_order(self) -> list[str]:
        indegree = {name: 0 for name in self._nodes}
        children: dict[str, list[str]] = {name: [] for name in self._nodes}

        for name, node in self._nodes.items():
            for dep in node.deps:
                if dep not in self._nodes:
                    raise PipelineError(f"DAG invalido: dependencia ausente '{dep}' em '{name}'")
                indegree[name] += 1
                children[dep].append(name)

        queue = [name for name, deg in indegree.items() if deg == 0]
        order: list[str] = []
        while queue:
            current = queue.pop(0)
            order.append(current)
            for child in children[current]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    queue.append(child)

        if len(order) != len(self._nodes):
            raise PipelineError("DAG invalido: ciclo detectado")
        return order

    def run(self, context: dict[str, Any]) -> dict[str, Any]:
        order = self._topological_order()
        context["dag_order"] = order
        for name in order:
            node = self._nodes[name]
            outputs = node.fn(context) or {}
            context[name] = outputs
        return context


def _ts_now_compact() -> str:
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df_local = df.copy()
    if "cluster" not in df_local.columns:
        at = pd.to_numeric(df_local.get("score_territorial_qt", 0), errors="coerce").fillna(0) > 70
        av = pd.to_numeric(df_local.get("VS_qt", 0), errors="coerce").fillna(0) > 70
        df_local["cluster"] = np.select(
            [at & av, ~at & av, at & ~av, ~at & ~av],
            ["Diamante", "Alavanca", "Consolidação", "Descarte"],
            "Descarte",
        )

    base = df_municipios_vazio()
    for col in base.columns:
        if col not in df_local.columns:
            df_local[col] = base[col]
    return df_local


def _node_ingest(context: dict[str, Any]) -> dict[str, Any]:
    input_path = Path(context["input_path"])
    if not input_path.exists():
        raise PipelineError(f"Arquivo de entrada nao encontrado: {input_path}")

    run_dir = Path(context["run_dir"])
    ingest_dir = run_dir / "ingest"
    ingest_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_path)
    raw_copy = ingest_dir / "ingested.parquet"
    df.to_parquet(raw_copy, index=False)
    return {"rows": int(len(df)), "input_path": str(input_path), "raw_copy": str(raw_copy)}


def _node_validate(context: dict[str, Any]) -> dict[str, Any]:
    ingest_path = Path(context["ingest"]["raw_copy"])
    df = pd.read_parquet(ingest_path)
    normalized = _normalize_dataframe(df)
    validate_municipios_input(normalized)

    run_dir = Path(context["run_dir"])
    validate_dir = run_dir / "validate"
    validate_dir.mkdir(parents=True, exist_ok=True)
    validated_path = validate_dir / "validated.parquet"
    normalized.to_parquet(validated_path, index=False)
    return {"rows": int(len(normalized)), "validated_path": str(validated_path)}


def _node_transform(context: dict[str, Any]) -> dict[str, Any]:
    validated_path = Path(context["validate"]["validated_path"])
    df = pd.read_parquet(validated_path)
    transformed = df.sort_values("ranking_final").reset_index(drop=True)

    run_dir = Path(context["run_dir"])
    transform_dir = run_dir / "transform"
    transform_dir.mkdir(parents=True, exist_ok=True)
    transformed_path = transform_dir / "df_mun_transformed.parquet"
    transformed.to_parquet(transformed_path, index=False)
    return {"rows": int(len(transformed)), "transformed_path": str(transformed_path)}


def _node_publish(context: dict[str, Any]) -> dict[str, Any]:
    transformed_path = Path(context["transform"]["transformed_path"])
    paths: AppPaths = context["paths"]
    run_id = str(context["run_id"])

    paths.gold_root.mkdir(parents=True, exist_ok=True)
    publish_name = f"df_mun_{run_id}.parquet"
    published_path = paths.gold_root / publish_name

    df = pd.read_parquet(transformed_path)
    df.to_parquet(published_path, index=False)
    load_manifest = build_load_manifest(
        source_name="df_municipios",
        collected_at_utc=datetime.now(UTC).isoformat(),
        dataset_path=published_path,
        df=df,
        parser_version=str(context["pipeline_version"]),
    )

    latest_meta = {
        "pipeline_version": context["pipeline_version"],
        "run_id": run_id,
        "published_path": str(published_path),
        "rows": int(len(df)),
        "sha256": _sha256_file(published_path),
        "published_at_utc": datetime.now(UTC).isoformat(),
    }
    latest_path = paths.gold_root / "df_mun_latest.json"
    latest_path.write_text(json.dumps(latest_meta, ensure_ascii=False, indent=2), encoding="utf-8")

    catalog_metadata = build_dataset_metadata(
        dataset_name="df_municipios",
        dataset_version=run_id,
        dataset_path=published_path,
        pipeline_version=str(context["pipeline_version"]),
        run_id=run_id,
    )
    catalog_refs = register_dataset_version(paths, catalog_metadata)
    return {
        "published_path": str(published_path),
        "latest_path": str(latest_path),
        "rows": int(len(df)),
        "dataset_metadata": catalog_metadata,
        "catalog_path": catalog_refs["catalog_path"],
        "catalog_latest_index_path": catalog_refs["latest_index_path"],
        "load_manifest": load_manifest,
    }


def run_versioned_data_pipeline(
    paths: AppPaths, input_path: Path, pipeline_version: str = PIPELINE_VERSION
) -> dict[str, Any]:
    run_id = _ts_now_compact()
    runs_root = paths.ingestion_root / "pipeline_runs"
    run_dir = runs_root / pipeline_version / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    context: dict[str, Any] = {
        "pipeline_version": pipeline_version,
        "run_id": run_id,
        "paths": paths,
        "input_path": str(input_path),
        "run_dir": str(run_dir),
        "started_at_utc": datetime.now(UTC).isoformat(),
    }

    dag = SimpleDag(
        [
            DagNode(name="ingest", deps=(), fn=_node_ingest),
            DagNode(name="validate", deps=("ingest",), fn=_node_validate),
            DagNode(name="transform", deps=("validate",), fn=_node_transform),
            DagNode(name="publish", deps=("transform",), fn=_node_publish),
        ]
    )
    result = dag.run(context)
    result["finished_at_utc"] = datetime.now(UTC).isoformat()

    manifest = {
        "pipeline_version": result["pipeline_version"],
        "run_id": result["run_id"],
        "dag_order": result["dag_order"],
        "started_at_utc": result["started_at_utc"],
        "finished_at_utc": result["finished_at_utc"],
        "dataset_manifest": result["publish"]["load_manifest"],
        "steps": {
            "ingest": result["ingest"],
            "validate": result["validate"],
            "transform": result["transform"],
            "publish": result["publish"],
        },
    }
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    result["manifest_path"] = str(manifest_path)
    return result
