import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from infrastructure.open_data_sources import OpenDataAsset, download_asset_incremental


class _FakeResponse:
    def __init__(self, status_code: int, content: bytes, headers: dict[str, str] | None = None):
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}


def test_download_asset_incremental_respects_not_modified(monkeypatch):
    calls: list[dict] = []

    def fake_get(url, headers=None, timeout=30):
        calls.append({"url": url, "headers": headers or {}, "timeout": timeout})
        if len(calls) == 1:
            return _FakeResponse(200, b"codigo_ibge,valor\n3550308,1\n", {"ETag": "abc", "Last-Modified": "Mon"})
        return _FakeResponse(304, b"", {})

    monkeypatch.setattr("infrastructure.open_data_sources.requests.get", fake_get)

    with tempfile.TemporaryDirectory() as tmp:
        out_dir = Path(tmp)
        asset = OpenDataAsset(name="ibge_socio", url="https://example.com/socio.csv", file_name="socio.csv")

        first = download_asset_incremental(asset=asset, output_dir=out_dir)
        second = download_asset_incremental(asset=asset, output_dir=out_dir)

        assert first["status"] == "downloaded"
        assert second["status"] == "not_modified"
        assert calls[1]["headers"].get("If-None-Match") == "abc"
