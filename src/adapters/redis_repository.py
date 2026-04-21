import asyncio
import re
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from redis.commands.search.query import Query

from .redis_client import get_redis

_SOURCE_INDEX_ATTR: dict[str, str] = {
    "tv": "idx",
    "movie": "idx",
    "person": "people_idx",
    "podcast": "podcasts_idx",
    "author": "author_idx",
    "book": "book_idx",
}

_TAG_ESCAPE_RE = re.compile(r"([^A-Za-z0-9])")


def _escape_tag_value(value: str) -> str:
    """Escape a scalar value for use inside a RediSearch TAG query."""
    return _TAG_ESCAPE_RE.sub(r"\\\1", value)


def _normalize_spotify_url(url: str) -> str:
    """Normalize Spotify URLs for exact-match TAG lookups."""
    parsed = urlsplit(url.strip())
    path = parsed.path.rstrip("/")
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), path, "", ""))


class RedisRepository:
    def __init__(self):
        self.redis = get_redis()
        self.idx = self.redis.ft("idx:media")
        self.people_idx = self.redis.ft("idx:people")
        self.podcasts_idx = self.redis.ft("idx:podcasts")
        self.author_idx = self.redis.ft("idx:author")
        self.book_idx = self.redis.ft("idx:book")

    async def search(
        self,
        query_str: str,
        limit: int = 10,
        sort_by: str | None = "popularity",
        sort_asc: bool = False,
    ):
        """
        Search the media index.

        Args:
            query_str: Redis Search query string
            limit: Maximum results to return
            sort_by: Field to sort by (popularity, rating, year), or None for query relevance
            sort_asc: Sort ascending if True, descending if False
        """
        query = Query(query_str).paging(0, limit)

        # Sort by the specified field (default: popularity descending)
        # Pass sort_by=None to use query relevance scoring instead
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

    async def search_podcasts(
        self,
        query_str: str,
        limit: int = 10,
        sort_by: str = "popularity",
        sort_asc: bool = False,
    ):
        """
        Search the podcasts index.

        Args:
            query_str: Redis Search query string
            limit: Maximum results to return
            sort_by: Field to sort by (popularity, episode_count)
            sort_asc: Sort ascending if True, descending if False
        """
        query = Query(query_str).paging(0, limit)

        # Sort by the specified field (default: popularity descending)
        if sort_by:
            query = query.sort_by(sort_by, asc=sort_asc)

        return await self.podcasts_idx.search(query)

    async def get_podcast_docs_by_spotify_urls(
        self, spotify_urls: Sequence[str]
    ) -> list[dict[str, Any]]:
        """Return stored podcast JSON docs matched by exact Spotify show URL."""
        normalized_urls: list[str] = []
        seen_urls: set[str] = set()

        for spotify_url in spotify_urls:
            normalized = _normalize_spotify_url(spotify_url)
            if normalized and normalized not in seen_urls:
                seen_urls.add(normalized)
                normalized_urls.append(normalized)

        if not normalized_urls:
            return []

        queries = [
            self.podcasts_idx.search(
                Query(f"@spotify_url:{{{_escape_tag_value(spotify_url)}}}").paging(0, 1)
            )
            for spotify_url in normalized_urls
        ]
        return await self._collect_podcast_docs_from_queries(queries)

    async def get_podcast_docs_by_spotify_ids(
        self, spotify_ids: Sequence[str]
    ) -> list[dict[str, Any]]:
        """Return stored podcast JSON docs matched by exact Spotify show id.

        ``spotify_id`` is the trailing path segment of a Spotify show URL
        (e.g. ``2k3X2cTt5uc0oZyOrRA7bS``).  Lookup is an O(1) TAG match on the
        indexed ``$.spotify_id`` field so URL escaping is unnecessary.
        """
        unique_ids: list[str] = []
        seen_ids: set[str] = set()

        for spotify_id in spotify_ids:
            cleaned = spotify_id.strip()
            if not cleaned or cleaned in seen_ids:
                continue
            seen_ids.add(cleaned)
            unique_ids.append(cleaned)

        if not unique_ids:
            return []

        queries = [
            self.podcasts_idx.search(
                Query(f"@spotify_id:{{{_escape_tag_value(spotify_id)}}}").paging(0, 1)
            )
            for spotify_id in unique_ids
        ]
        return await self._collect_podcast_docs_from_queries(queries)

    async def _collect_podcast_docs_from_queries(
        self, queries: Sequence[Any]
    ) -> list[dict[str, Any]]:
        """Run a list of podcast search queries and JSON.MGET the matched docs."""
        search_results = await asyncio.gather(*queries)

        redis_keys: list[str] = []
        seen_keys: set[str] = set()
        for result in search_results:
            for doc in result.docs:
                redis_key = getattr(doc, "id", None)
                if isinstance(redis_key, str) and redis_key and redis_key not in seen_keys:
                    seen_keys.add(redis_key)
                    redis_keys.append(redis_key)

        if not redis_keys:
            return []

        raw_docs: list[object] = await self.redis.json().mget(redis_keys, "$")  # type: ignore[misc]

        docs: list[dict[str, Any]] = []
        for raw_doc in raw_docs:
            if isinstance(raw_doc, list) and raw_doc and isinstance(raw_doc[0], dict):
                docs.append(raw_doc[0])
            elif isinstance(raw_doc, dict):
                docs.append(raw_doc)

        return docs

    async def search_authors(
        self,
        query_str: str,
        limit: int = 10,
        sort_by: str | None = None,
        sort_asc: bool = False,
    ):
        """
        Search the authors index (OpenLibrary authors).

        Args:
            query_str: Redis Search query string
            limit: Maximum results to return
            sort_by: Field to sort by (optional, no default for authors)
            sort_asc: Sort ascending if True, descending if False
        """
        query = Query(query_str).paging(0, limit)

        # Sort by the specified field if provided
        if sort_by:
            query = query.sort_by(sort_by, asc=sort_asc)

        return await self.author_idx.search(query)

    async def search_books(
        self,
        query_str: str,
        limit: int = 10,
        sort_by: str | None = None,
        sort_asc: bool = False,
    ):
        """
        Search the books index (OpenLibrary books/works).

        Args:
            query_str: Redis Search query string
            limit: Maximum results to return
            sort_by: Field to sort by (first_publish_year, ratings_average, etc.)
            sort_asc: Sort ascending if True, descending if False
        """
        # Use BM25 scorer for better ranking - normalizes by document length
        # so shorter exact matches rank higher than longer titles containing the terms
        query = Query(query_str).paging(0, limit).scorer("BM25")

        # Sort by the specified field if provided
        if sort_by:
            query = query.sort_by(sort_by, asc=sort_asc)

        return await self.book_idx.search(query)

    async def get_books_by_author_olid(
        self,
        author_olid: str,
        limit: int = 50,
        sort_by: str = "ratings_average",
        sort_asc: bool = False,
    ):
        """
        Get books by author OpenLibrary ID using O(1) TagField lookup.

        This is the most performant way to find all books by an author.
        Uses the indexed author_olid TagField for instant relational queries.

        Args:
            author_olid: OpenLibrary author ID (e.g., "OL2162284A" for Stephen King)
            limit: Maximum results to return
            sort_by: Field to sort by (ratings_average, first_publish_year, etc.)
            sort_asc: Sort ascending if True, descending if False

        Returns:
            Search results with books by the specified author
        """
        # TagField query uses inverted index for O(1) lookup
        query_str = f"@author_olid:{{{author_olid}}}"
        query = Query(query_str).paging(0, limit)

        # Sort by the specified field (default: highest rated first)
        if sort_by:
            query = query.sort_by(sort_by, asc=sort_asc)

        return await self.book_idx.search(query)

    async def search_projected(
        self,
        source: str,
        query_str: str,
        fields: list[str],
        limit: int = 10,
        sort_by: str | None = "popularity",
        sort_asc: bool = False,
    ) -> Any:
        """
        Search an index returning only the requested JSON fields.

        Uses FT.SEARCH RETURN with ``$.field AS field`` aliases so Redis
        transfers only the projected payload instead of full documents.

        Args:
            source: Logical source name (tv, movie, person, podcast, author, book).
            query_str: Redis Search query string.
            fields: JSON field names to return (e.g. ["mc_id", "search_title"]).
            limit: Maximum results to return.
            sort_by: Field to sort by, or None for relevance scoring.
            sort_asc: Sort ascending if True, descending if False.
        """
        attr = _SOURCE_INDEX_ATTR.get(source)
        if attr is None:
            raise ValueError(f"Unknown source for projected search: {source}")

        idx = getattr(self, attr)
        query = Query(query_str).paging(0, limit)

        for field in fields:
            query.return_field(f"$.{field}", as_field=field)

        if sort_by:
            query = query.sort_by(sort_by, asc=sort_asc)

        return await idx.search(query)

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
            sortable_values_size_mb = float(index_info.get("sortable_values_size_mb", 0))
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
            sortable_values_size_mb = float(people_index_info.get("sortable_values_size_mb", 0))
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

        # Get podcasts index document count and memory stats
        podcasts_index_stats = {}
        podcasts_num_docs = 0
        try:
            podcasts_index_info = await self.podcasts_idx.info()
            podcasts_num_docs = int(podcasts_index_info.get("num_docs", 0))
            # Get index memory usage in bytes
            inverted_sz_mb = float(podcasts_index_info.get("inverted_sz_mb", 0))
            offset_vectors_sz_mb = float(podcasts_index_info.get("offset_vectors_sz_mb", 0))
            doc_table_size_mb = float(podcasts_index_info.get("doc_table_size_mb", 0))
            sortable_values_size_mb = float(podcasts_index_info.get("sortable_values_size_mb", 0))
            key_table_size_mb = float(podcasts_index_info.get("key_table_size_mb", 0))

            total_index_mb = (
                inverted_sz_mb
                + offset_vectors_sz_mb
                + doc_table_size_mb
                + sortable_values_size_mb
                + key_table_size_mb
            )
            index_memory_bytes = int(total_index_mb * 1024 * 1024)

            podcasts_index_stats = {
                "num_docs": podcasts_num_docs,
                "index_memory_bytes": index_memory_bytes,
                "inverted_sz_mb": inverted_sz_mb,
                "offset_vectors_sz_mb": offset_vectors_sz_mb,
                "doc_table_size_mb": doc_table_size_mb,
                "sortable_values_size_mb": sortable_values_size_mb,
                "key_table_size_mb": key_table_size_mb,
            }
        except Exception:
            podcasts_num_docs = 0
            podcasts_index_stats = {"num_docs": 0, "index_memory_bytes": 0}

        # Get author index document count and memory stats
        author_index_stats = {}
        author_num_docs = 0
        try:
            author_index_info = await self.author_idx.info()
            author_num_docs = int(author_index_info.get("num_docs", 0))
            # Get index memory usage in bytes
            inverted_sz_mb = float(author_index_info.get("inverted_sz_mb", 0))
            offset_vectors_sz_mb = float(author_index_info.get("offset_vectors_sz_mb", 0))
            doc_table_size_mb = float(author_index_info.get("doc_table_size_mb", 0))
            sortable_values_size_mb = float(author_index_info.get("sortable_values_size_mb", 0))
            key_table_size_mb = float(author_index_info.get("key_table_size_mb", 0))

            total_index_mb = (
                inverted_sz_mb
                + offset_vectors_sz_mb
                + doc_table_size_mb
                + sortable_values_size_mb
                + key_table_size_mb
            )
            index_memory_bytes = int(total_index_mb * 1024 * 1024)

            author_index_stats = {
                "num_docs": author_num_docs,
                "index_memory_bytes": index_memory_bytes,
                "inverted_sz_mb": inverted_sz_mb,
                "offset_vectors_sz_mb": offset_vectors_sz_mb,
                "doc_table_size_mb": doc_table_size_mb,
                "sortable_values_size_mb": sortable_values_size_mb,
                "key_table_size_mb": key_table_size_mb,
            }
        except Exception:
            author_num_docs = 0
            author_index_stats = {"num_docs": 0, "index_memory_bytes": 0}

        # Get book index document count and memory stats
        book_index_stats = {}
        book_num_docs = 0
        try:
            book_index_info = await self.book_idx.info()
            book_num_docs = int(book_index_info.get("num_docs", 0))
            # Get index memory usage in bytes
            inverted_sz_mb = float(book_index_info.get("inverted_sz_mb", 0))
            offset_vectors_sz_mb = float(book_index_info.get("offset_vectors_sz_mb", 0))
            doc_table_size_mb = float(book_index_info.get("doc_table_size_mb", 0))
            sortable_values_size_mb = float(book_index_info.get("sortable_values_size_mb", 0))
            key_table_size_mb = float(book_index_info.get("key_table_size_mb", 0))

            total_index_mb = (
                inverted_sz_mb
                + offset_vectors_sz_mb
                + doc_table_size_mb
                + sortable_values_size_mb
                + key_table_size_mb
            )
            index_memory_bytes = int(total_index_mb * 1024 * 1024)

            book_index_stats = {
                "num_docs": book_num_docs,
                "index_memory_bytes": index_memory_bytes,
                "inverted_sz_mb": inverted_sz_mb,
                "offset_vectors_sz_mb": offset_vectors_sz_mb,
                "doc_table_size_mb": doc_table_size_mb,
                "sortable_values_size_mb": sortable_values_size_mb,
                "key_table_size_mb": key_table_size_mb,
            }
        except Exception:
            book_num_docs = 0
            book_index_stats = {"num_docs": 0, "index_memory_bytes": 0}

        # Index doc counts for key breakdown (fast, no SCAN needed)
        total_index_keys = (
            num_docs + people_num_docs + podcasts_num_docs + author_num_docs + book_num_docs
        )
        # API cache keys = everything in Redis that isn't an index document
        api_cache_keys = max(0, dbsize - total_index_keys)

        cache_breakdown = {
            "media": num_docs,
            "person": people_num_docs,
            "podcast": podcasts_num_docs,
            "author": author_num_docs,
            "book": book_num_docs,
            "api_cache": api_cache_keys,
        }

        # Extract memory policy and eviction stats from INFO
        maxmemory = info.get("maxmemory", 0)
        maxmemory_policy = info.get("maxmemory_policy", "unknown")
        evicted_keys = info.get("evicted_keys", 0)

        return {
            "info": info,
            "dbsize": dbsize,
            "num_docs": num_docs,
            "index_stats": index_stats,
            "people_num_docs": people_num_docs,
            "people_index_stats": people_index_stats,
            "podcasts_num_docs": podcasts_num_docs,
            "podcasts_index_stats": podcasts_index_stats,
            "author_num_docs": author_num_docs,
            "author_index_stats": author_index_stats,
            "book_num_docs": book_num_docs,
            "book_index_stats": book_index_stats,
            "cache_breakdown": cache_breakdown,
            "maxmemory": maxmemory,
            "maxmemory_policy": maxmemory_policy,
            "evicted_keys": evicted_keys,
        }
