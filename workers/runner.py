from __future__ import annotations

from config.settings import get_settings
from infrastructure.queue_rq import get_redis_connection


def main() -> int:
    settings = get_settings()
    redis_conn = get_redis_connection(settings.redis_url)
    from rq import Worker

    worker = Worker([settings.rq_queue_name], connection=redis_conn)
    worker.work(with_scheduler=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
