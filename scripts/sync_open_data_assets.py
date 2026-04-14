from __future__ import annotations

import argparse

from config.settings import get_settings
from infrastructure.open_data_sources import OpenDataAsset, download_asset_incremental


def _parse_asset(raw: str) -> OpenDataAsset:
    parts = raw.split("|")
    if len(parts) != 3:
        raise ValueError("asset invalido. Use: nome|url|arquivo.csv")
    return OpenDataAsset(name=parts[0].strip(), url=parts[1].strip(), file_name=parts[2].strip())


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sincroniza assets de dados abertos com cache incremental (ETag/Last-Modified)."
    )
    parser.add_argument(
        "--asset",
        action="append",
        default=[],
        help="Defina um asset como nome|url|arquivo.csv. Pode repetir a flag.",
    )
    args = parser.parse_args()

    if not args.asset:
        raise ValueError("informe pelo menos um --asset nome|url|arquivo.csv")

    settings = get_settings()
    paths = settings.build_paths()
    raw_dir = (paths.data_root / "open_data" / "raw").resolve()
    raw_dir.mkdir(parents=True, exist_ok=True)

    for raw in args.asset:
        asset = _parse_asset(raw)
        result = download_asset_incremental(asset=asset, output_dir=raw_dir)
        print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
