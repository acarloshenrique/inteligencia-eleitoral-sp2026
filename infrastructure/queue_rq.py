from __future__ import annotations


def get_redis_connection(redis_url: str):
    from redis import Redis

    return Redis.from_url(redis_url)


def get_queue(redis_url: str, queue_name: str):
    from rq import Queue

    conn = get_redis_connection(redis_url)
    return Queue(queue_name, connection=conn)
