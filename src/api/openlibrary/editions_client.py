"""
OpenLibrary Editions Client - Fetch edition counts for works.

Used to determine book popularity based on number of editions published.
More editions = more popular book.
"""

import asyncio
from typing import Any

import aiohttp

from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)

# Cache for edition counts - 7 days TTL (edition counts are stable)
EditionsCache = RedisCache(
    defaultTTL=7 * 24 * 60 * 60,  # 7 days
    prefix="openlibrary_editions",
    verbose=False,
    isClassMethod=True,
    version="1.0.0",
)


class OpenLibraryEditionsClient:
    """
    Client for fetching edition counts from OpenLibrary.

    Uses /works/{id}/editions.json endpoint to get edition count.
    Includes Redis caching and rate limiting.
    """

    # Rate limiting: Be respectful to OpenLibrary (community-run)
    _rate_limit_semaphore: asyncio.Semaphore | None = None
    _max_concurrent = 20  # Max concurrent requests

    def __init__(self):
        """Initialize the editions client."""
        self.base_url = "https://openlibrary.org"
        self._session: aiohttp.ClientSession | None = None

    @classmethod
    def _get_semaphore(cls) -> asyncio.Semaphore:
        """Get or create the rate limiting semaphore."""
        if cls._rate_limit_semaphore is None:
            cls._rate_limit_semaphore = asyncio.Semaphore(cls._max_concurrent)
        return cls._rate_limit_semaphore

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create the aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"User-Agent": "mediacircle/1.0 (gould@emgtrading.net)"},
                timeout=aiohttp.ClientTimeout(total=30),
            )
        return self._session

    async def close(self) -> None:
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()

    @RedisCache.use_cache(EditionsCache, prefix="edition_count")
    async def get_edition_count(self, work_id: str) -> int:
        """
        Get the edition count for a work.

        Args:
            work_id: OpenLibrary work ID (e.g., "OL472549W" or "/works/OL472549W")

        Returns:
            Number of editions, or 0 if not found/error
        """
        # Normalize work_id - remove "/works/" prefix if present
        if work_id.startswith("/works/"):
            work_id = work_id[7:]

        url = f"{self.base_url}/works/{work_id}/editions.json"
        params = {"limit": 1}  # We only need the count, not the actual editions

        semaphore = self._get_semaphore()

        async with semaphore:
            try:
                session = await self._get_session()
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data: dict[str, Any] = await response.json()
                        count = data.get("size", 0)
                        return int(count) if count else 0
                    elif response.status == 404:
                        logger.debug(f"Work not found: {work_id}")
                        return 0
                    else:
                        logger.warning(
                            f"Error fetching editions for {work_id}: HTTP {response.status}"
                        )
                        return 0
            except TimeoutError:
                logger.warning(f"Timeout fetching editions for {work_id}")
                return 0
            except Exception as e:
                logger.error(f"Error fetching editions for {work_id}: {e}")
                return 0

    async def get_edition_counts_batch(
        self, work_ids: list[str], progress_callback: Any | None = None
    ) -> dict[str, int]:
        """
        Get edition counts for multiple works concurrently.

        Args:
            work_ids: List of OpenLibrary work IDs
            progress_callback: Optional callback(completed, total) for progress updates

        Returns:
            Dict mapping work_id -> edition_count
        """
        results: dict[str, int] = {}
        total = len(work_ids)
        completed = 0

        # Process in batches to avoid overwhelming the API
        batch_size = 100
        for i in range(0, total, batch_size):
            batch = work_ids[i : i + batch_size]
            tasks = [self.get_edition_count(wid) for wid in batch]
            counts = await asyncio.gather(*tasks)

            for wid, count in zip(batch, counts, strict=True):
                # Normalize work_id for consistent keys
                if wid.startswith("/works/"):
                    wid = wid[7:]
                results[wid] = count

            completed += len(batch)
            if progress_callback:
                progress_callback(completed, total)

        return results


# Singleton instance for reuse
_client: OpenLibraryEditionsClient | None = None


def get_editions_client() -> OpenLibraryEditionsClient:
    """Get the singleton editions client instance."""
    global _client
    if _client is None:
        _client = OpenLibraryEditionsClient()
    return _client
