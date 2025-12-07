
from redis.commands.search.query import Query

from .redis_client import get_redis


class RedisRepository:
    def __init__(self):
        self.redis = get_redis()
        self.idx = self.redis.ft("idx:media")

    async def search(
        self,
        query_str: str,
        limit: int = 10,
        sort_by: str = "popularity",
        sort_asc: bool = False,
    ):
        """
        Search the media index.

        Args:
            query_str: Redis Search query string
            limit: Maximum results to return
            sort_by: Field to sort by (popularity, rating, year)
            sort_asc: Sort ascending if True, descending if False
        """
        query = Query(query_str).paging(0, limit)

        # Sort by the specified field (default: popularity descending)
        if sort_by:
            query = query.sort_by(sort_by, asc=sort_asc)

        return await self.idx.search(query)

    async def set_document(self, key: str, value: dict) -> None:
        await self.redis.json().set(key, "$", value)  # type: ignore[misc]

    async def stats(self):
        info = await self.redis.info()
        dbsize = await self.redis.dbsize()

        # Get index document count
        try:
            index_info = await self.idx.info()
            # index_info is a dict with 'num_docs' key
            num_docs = int(index_info.get("num_docs", 0))
        except Exception:
            num_docs = 0

        # Count keys by prefix using optimized SCAN with pattern matching
        cache_breakdown = {}

        # Count each prefix separately (more efficient than scanning all keys)
        prefix_patterns = [
            ("media", "media:*"),
            ("tmdb_request", "tmdb_request:*"),
            ("tmdb", "tmdb:*"),
        ]

        for prefix_name, pattern in prefix_patterns:
            count = 0
            cursor = 0
            while True:
                cursor, keys = await self.redis.scan(
                    cursor=cursor, match=pattern, count=10000
                )
                count += len(keys)
                if cursor == 0:
                    break
            cache_breakdown[prefix_name] = count

        # Calculate 'other' as the remainder
        total_counted = sum(cache_breakdown.values())
        cache_breakdown["other"] = dbsize - total_counted

        return {
            "info": info,
            "dbsize": dbsize,
            "num_docs": num_docs,
            "cache_breakdown": cache_breakdown,
        }
