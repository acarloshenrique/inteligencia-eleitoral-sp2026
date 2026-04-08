from pathlib import Path
import tempfile

import pytest

from infrastructure.metadata_db import MetadataDb


@pytest.mark.unit
def test_metadata_db_job_lifecycle():
    with tempfile.TemporaryDirectory() as tmp:
        db = MetadataDb(Path(tmp) / "jobs.sqlite3")
        db.create_job("j1", "reindex", {"a": 1})
        db.set_status("j1", "running")
        db.set_result("j1", {"ok": True})
        out = db.get_job("j1")
        assert out is not None
        assert out["status"] == "finished"
        assert out["result"]["ok"] is True
