from __future__ import annotations

from config.settings import get_settings
from infrastructure.env import validate_prod_runtime_hardening
from infrastructure.queue_rq import get_redis_connection


def main() -> int:
    settings = get_settings()
    paths = settings.build_paths()
    hardening_errors = validate_prod_runtime_hardening(settings, paths)
    if hardening_errors:
        raise RuntimeError("; ".join(hardening_errors))
    redis_conn = get_redis_connection(settings.redis_url)
    from rq import Worker

    worker = Worker([settings.rq_queue_name], connection=redis_conn)
    worker.work(with_scheduler=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
