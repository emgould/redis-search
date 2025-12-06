
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
        return {"info": info, "dbsize": dbsize}
