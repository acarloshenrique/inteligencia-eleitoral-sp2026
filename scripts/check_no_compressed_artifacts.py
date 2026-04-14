from __future__ import annotations

import subprocess
import sys
from pathlib import Path

BLOCKED_SUFFIXES = {
    ".7z",
    ".bz2",
    ".db",
    ".duckdb",
    ".gz",
    ".joblib",
    ".parquet",
    ".pickle",
    ".pkl",
    ".rar",
    ".sqlite",
    ".sqlite3",
    ".xz",
    ".zip",
}

MAGIC_HEADERS = {
    b"\x1f\x8b": "gzip",
    b"PK\x03\x04": "zip",
    b"PK\x05\x06": "zip",
    b"PK\x07\x08": "zip",
    b"BZh": "bzip2",
    b"\xfd7zXZ\x00": "xz",
    b"7z\xbc\xaf\x27\x1c": "7zip",
    b"Rar!\x1a\x07\x00": "rar",
    b"Rar!\x1a\x07\x01\x00": "rar",
    b"PAR1": "parquet",
    b"SQLite format 3\x00": "sqlite",
}


def _tracked_files() -> list[str]:
    result = subprocess.run(
        ["git", "ls-files"],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    return [line for line in result.stdout.splitlines() if line]


def _detect_magic(path: Path) -> str | None:
    try:
        header = path.read_bytes()[:32]
    except OSError:
        return None
    for magic, label in MAGIC_HEADERS.items():
        if header.startswith(magic):
            return label
    return None


def main(argv: list[str]) -> int:
    candidates = argv or _tracked_files()
    violations: list[str] = []
    for raw in candidates:
        path = Path(raw)
        if not path.is_file():
            continue
        suffix = path.suffix.lower()
        magic = _detect_magic(path)
        if suffix in BLOCKED_SUFFIXES:
            violations.append(f"{path}: blocked extension {suffix}")
        elif magic is not None:
            violations.append(f"{path}: compressed/binary data format detected ({magic})")

    if violations:
        print("Blocked compressed or binary data artifacts:", file=sys.stderr)
        for violation in violations:
            print(f"- {violation}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
