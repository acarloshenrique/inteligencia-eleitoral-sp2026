from __future__ import annotations

import hashlib
import shutil
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from data_catalog.models import DataSourceSpec


@dataclass(frozen=True)
class DownloadedAsset:
    path: Path
    sha256: str
    bytes_size: int
    source_url: str


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


class SourceDownloader:
    def download(
        self, *, source: DataSourceSpec, destination_dir: Path, input_path: Path | None = None
    ) -> DownloadedAsset:
        destination_dir.mkdir(parents=True, exist_ok=True)
        if input_path is not None:
            if not input_path.exists():
                raise FileNotFoundError(f"input_path nao encontrado: {input_path}")
            destination = destination_dir / input_path.name
            if not destination.exists() or sha256_file(destination) != sha256_file(input_path):
                shutil.copy2(input_path, destination)
            return DownloadedAsset(
                path=destination,
                sha256=sha256_file(destination),
                bytes_size=destination.stat().st_size,
                source_url=str(input_path),
            )

        parsed = urlparse(str(source.url))
        if parsed.scheme not in {"http", "https"}:
            raise ValueError("URL remota precisa usar http ou https")
        filename = Path(parsed.path).name or f"{source.name}.download"
        destination = destination_dir / filename
        req = Request(str(source.url), headers={"User-Agent": "inteligencia-eleitoral-ingestion/1.0"})  # noqa: S310
        with urlopen(req, timeout=60) as response:  # noqa: S310 - URL vem do catalogo governado e esquema e validado.
            destination.write_bytes(response.read())
        return DownloadedAsset(
            path=destination,
            sha256=sha256_file(destination),
            bytes_size=destination.stat().st_size,
            source_url=str(source.url),
        )
