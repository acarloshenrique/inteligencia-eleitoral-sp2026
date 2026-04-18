from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests


class OpenDataSourceError(RuntimeError):
    pass


@dataclass(frozen=True)
class OpenDataAsset:
    name: str
    url: str
    file_name: str


def _sha256_bytes(payload: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(payload)
    return digest.hexdigest()


def _state_path(output_dir: Path) -> Path:
    return output_dir / "_source_state.json"


def _load_state(output_dir: Path) -> dict[str, Any]:
    path = _state_path(output_dir)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _save_state(output_dir: Path, state: dict[str, Any]) -> None:
    path = _state_path(output_dir)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def download_asset_incremental(
    *,
    asset: OpenDataAsset,
    output_dir: Path,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    state = _load_state(output_dir)
    previous = state.get(asset.name, {})

    headers: dict[str, str] = {}
    etag = str(previous.get("etag", "")).strip()
    last_modified = str(previous.get("last_modified", "")).strip()
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified

    response = requests.get(asset.url, headers=headers, timeout=timeout_seconds)
    if response.status_code == 304:
        target_path = output_dir / asset.file_name
        return {
            "asset": asset.name,
            "status": "not_modified",
            "path": str(target_path),
            "etag": etag,
            "last_modified": last_modified,
            "sha256": previous.get("sha256", ""),
        }
    if response.status_code != 200:
        raise OpenDataSourceError(f"falha ao baixar asset '{asset.name}' ({response.status_code})")

    payload = response.content
    target_path = output_dir / asset.file_name
    temp_path = target_path.with_suffix(target_path.suffix + ".tmp")
    temp_path.write_bytes(payload)
    temp_path.replace(target_path)

    sha256 = _sha256_bytes(payload)
    next_state = {
        "url": asset.url,
        "etag": response.headers.get("ETag", ""),
        "last_modified": response.headers.get("Last-Modified", ""),
        "sha256": sha256,
        "path": str(target_path),
        "downloaded_at_utc": datetime.now(UTC).isoformat(),
    }
    state[asset.name] = next_state
    _save_state(output_dir, state)
    return {"asset": asset.name, "status": "downloaded", **next_state}
