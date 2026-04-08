from infrastructure.rag_cache import TimedLruCache


def test_timed_lru_cache_eviction_by_capacity():
    cache = TimedLruCache[str, int](maxsize=2, ttl_seconds=60)
    cache.set("a", 1)
    cache.set("b", 2)
    cache.get("a")
    cache.set("c", 3)

    assert cache.get("a") == 1
    assert cache.get("b") is None
    assert cache.get("c") == 3
