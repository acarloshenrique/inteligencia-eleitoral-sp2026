import tempfile
from pathlib import Path

import pytest

from infrastructure.artifact_store import LocalArtifactStore


@pytest.mark.unit
def test_local_artifact_store_put_file():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        src = root / "source.txt"
        src.write_text("abc", encoding="utf-8")
        store = LocalArtifactStore(root / "artifacts")
        uri = store.put_file(src, "exports/source.txt")
        assert Path(uri).exists()
        assert Path(uri).read_text(encoding="utf-8") == "abc"
