from __future__ import annotations

import argparse
from pathlib import Path

from semantic_layer import SemanticRegistryWriter, build_semantic_registry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export semantic registry for API, UI and reports.")
    parser.add_argument("--output-dir", type=Path, default=Path("lake/semantic/registry"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    registry = build_semantic_registry()
    writer = SemanticRegistryWriter()
    json_path = writer.write_json(args.output_dir / "semantic_registry.json", registry)
    md_path = writer.write_markdown(args.output_dir / "semantic_registry.md", registry)
    print(f"semantic_registry_json={json_path}")
    print(f"semantic_registry_markdown={md_path}")


if __name__ == "__main__":
    main()
