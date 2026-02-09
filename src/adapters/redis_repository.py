from redis.commands.search.query import Query

from .redis_client import get_redis


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

        # Use index doc counts for cache breakdown (fast, no SCAN needed)
        # This is accurate because each index corresponds to keys with that prefix
        cache_breakdown = {
            "media": num_docs,  # media:* keys
            "person": people_num_docs,  # person:* keys
            "podcast": podcasts_num_docs,  # podcast:* keys
            "author": author_num_docs,  # author:* keys
            "book": book_num_docs,  # book:* keys
            "tmdb_request": 0,  # Cache keys, not indexed
            "tmdb": 0,  # Cache keys, not indexed
        }

        # Calculate 'other' as the remainder (includes tmdb cache keys, etc.)
        total_counted = sum(cache_breakdown.values())
        cache_breakdown["other"] = max(0, dbsize - total_counted)

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
        }
