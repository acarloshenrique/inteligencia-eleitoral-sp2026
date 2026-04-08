from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


class ArtifactStore(Protocol):
    def put_file(self, local_path: Path, artifact_key: str) -> str: ...


@dataclass
class LocalArtifactStore:
    root_dir: Path

    def __post_init__(self):
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def put_file(self, local_path: Path, artifact_key: str) -> str:
        target = self.root_dir / artifact_key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(local_path.read_bytes())
        return str(target)


@dataclass
class S3ArtifactStore:
    bucket: str
    endpoint_url: str
    access_key: str
    secret_key: str
    region: str

    def put_file(self, local_path: Path, artifact_key: str) -> str:
        import boto3

        client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url or None,
            aws_access_key_id=self.access_key or None,
            aws_secret_access_key=self.secret_key or None,
            region_name=self.region or None,
        )
        client.upload_file(str(local_path), self.bucket, artifact_key)
        return f"s3://{self.bucket}/{artifact_key}"
