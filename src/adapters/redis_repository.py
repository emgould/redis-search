
from redis.commands.search.query import Query

from .redis_client import get_redis


class RedisRepository:
    def __init__(self):
        self.redis = get_redis()
        self.idx = self.redis.ft("idx:media")
        self.people_idx = self.redis.ft("idx:people")

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

    async def search_people(
        self,
        query_str: str,
        limit: int = 10,
        sort_by: str = "popularity",
        sort_asc: bool = False,
    ):
        """
        Search the people index.

        Args:
            query_str: Redis Search query string
            limit: Maximum results to return
            sort_by: Field to sort by (popularity)
            sort_asc: Sort ascending if True, descending if False
        """
        query = Query(query_str).paging(0, limit)

        # Sort by the specified field (default: popularity descending)
        if sort_by:
            query = query.sort_by(sort_by, asc=sort_asc)

        return await self.people_idx.search(query)

    async def set_document(self, key: str, value: dict) -> None:
        await self.redis.json().set(key, "$", value)  # type: ignore[misc]

    async def stats(self):
        info = await self.redis.info()
        dbsize = await self.redis.dbsize()

        # Get media index document count and memory stats
        index_stats = {}
        num_docs = 0
        try:
            index_info = await self.idx.info()
            # index_info is a dict with various stats
            num_docs = int(index_info.get("num_docs", 0))
            # Get index memory usage in bytes (convert from MB if needed)
            inverted_sz_mb = float(index_info.get("inverted_sz_mb", 0))
            offset_vectors_sz_mb = float(index_info.get("offset_vectors_sz_mb", 0))
            doc_table_size_mb = float(index_info.get("doc_table_size_mb", 0))
            sortable_values_size_mb = float(
                index_info.get("sortable_values_size_mb", 0)
            )
            key_table_size_mb = float(index_info.get("key_table_size_mb", 0))

            # Calculate total index memory in bytes
            total_index_mb = (
                inverted_sz_mb
                + offset_vectors_sz_mb
                + doc_table_size_mb
                + sortable_values_size_mb
                + key_table_size_mb
            )
            index_memory_bytes = int(total_index_mb * 1024 * 1024)

            index_stats = {
                "num_docs": num_docs,
                "index_memory_bytes": index_memory_bytes,
                "inverted_sz_mb": inverted_sz_mb,
                "offset_vectors_sz_mb": offset_vectors_sz_mb,
                "doc_table_size_mb": doc_table_size_mb,
                "sortable_values_size_mb": sortable_values_size_mb,
                "key_table_size_mb": key_table_size_mb,
            }
        except Exception:
            num_docs = 0
            index_stats = {"num_docs": 0, "index_memory_bytes": 0}

        # Get people index document count and memory stats
        people_index_stats = {}
        people_num_docs = 0
        try:
            people_index_info = await self.people_idx.info()
            people_num_docs = int(people_index_info.get("num_docs", 0))
            # Get index memory usage in bytes
            inverted_sz_mb = float(people_index_info.get("inverted_sz_mb", 0))
            offset_vectors_sz_mb = float(people_index_info.get("offset_vectors_sz_mb", 0))
            doc_table_size_mb = float(people_index_info.get("doc_table_size_mb", 0))
            sortable_values_size_mb = float(
                people_index_info.get("sortable_values_size_mb", 0)
            )
            key_table_size_mb = float(people_index_info.get("key_table_size_mb", 0))

            total_index_mb = (
                inverted_sz_mb
                + offset_vectors_sz_mb
                + doc_table_size_mb
                + sortable_values_size_mb
                + key_table_size_mb
            )
            index_memory_bytes = int(total_index_mb * 1024 * 1024)

            people_index_stats = {
                "num_docs": people_num_docs,
                "index_memory_bytes": index_memory_bytes,
                "inverted_sz_mb": inverted_sz_mb,
                "offset_vectors_sz_mb": offset_vectors_sz_mb,
                "doc_table_size_mb": doc_table_size_mb,
                "sortable_values_size_mb": sortable_values_size_mb,
                "key_table_size_mb": key_table_size_mb,
            }
        except Exception:
            people_num_docs = 0
            people_index_stats = {"num_docs": 0, "index_memory_bytes": 0}

        # Count keys by prefix using optimized SCAN with pattern matching
        cache_breakdown = {}

        # Count each prefix separately (more efficient than scanning all keys)
        prefix_patterns = [
            ("media", "media:*"),
            ("person", "person:*"),
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
            "index_stats": index_stats,
            "people_num_docs": people_num_docs,
            "people_index_stats": people_index_stats,
            "cache_breakdown": cache_breakdown,
        }
