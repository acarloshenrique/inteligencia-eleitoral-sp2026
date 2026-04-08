import json

from config.settings import get_settings
from infrastructure.rag_metrics import RagMetricsTracker


def main() -> int:
    settings = get_settings()
    paths = settings.build_paths()
    tracker = RagMetricsTracker(paths=paths)
    snapshot = tracker.get_snapshot()
    print(json.dumps(snapshot, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
