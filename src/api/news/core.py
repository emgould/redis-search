"""
News Core Service - Base service for NewsAPI operations
Handles core API communication and article processing.
"""

import asyncio
import random
from datetime import datetime
from typing import Any

import aiohttp

from api.news.auth import Auth
from api.news.models import MCNewsItem, NewsSource
from utils.get_logger import get_logger
from utils.rate_limiter import get_rate_limiter

logger = get_logger(__name__)


class NewsService(Auth):
    """
    Core news service for NewsAPI operations.
    Provides foundation for search and other operations.
    """

    def __init__(self):
        """Initialize News service. API keys are accessed from secrets at runtime."""
        super().__init__()

    async def _make_request(
        self, url: str, params: dict[str, Any] | None = None, max_retries: int = 3
    ) -> tuple[dict[str, Any], int | None]:
        """
        Make an async request to the OpenLibrary API with rate limiting and retry logic.

        Args:
            url: URL to request
            params: Optional query parameters
            max_retries: Maximum number of retry attempts (default: 3)

        Returns:
            tuple: (response_data, error_code) - error_code is None on success
        """
        timeout = aiohttp.ClientTimeout(
            total=60
        )  # 60 second timeout matches OpenLibrary query limit
        headers = {"User-Agent": "mediacircle/1.0 (gould@emgtrading.net)"}
        # Get rate limiter for current event loop
        if "covers" in url:
            rate_limiter = get_rate_limiter(
                6, 1
            )  # cover zone: 400 requests per minute ≈ 6.67 per second
        else:
            rate_limiter = get_rate_limiter(
                3, 1
            )  # api zone: 180 requests per minute = 3 per second

        for attempt in range(max_retries + 1):
            try:
                # Acquire rate limit token before making request
                async with (
                    rate_limiter,
                    aiohttp.ClientSession() as session,
                    session.get(url, params=params, timeout=timeout, headers=headers) as response,
                ):
                    if response.status == 200:
                        data: dict[str, Any] = await response.json()
                        return data, None
                    elif response.status == 429:
                        # Rate limit exceeded - implement exponential backoff
                        retry_after = response.headers.get("Retry-After")

                        if attempt < max_retries:
                            # Calculate backoff delay with jitter
                            base_delay = 2**attempt  # Exponential: 1s, 2s, 4s...
                            jitter = random.uniform(0.1, 0.5)
                            delay = base_delay + jitter

                            # Use Retry-After header if available
                            if retry_after:
                                try:
                                    delay = max(delay, int(retry_after))
                                except (ValueError, TypeError):
                                    pass

                            logger.warning(
                                f"OpenLibrary rate limit exceeded (attempt {attempt + 1}/{max_retries + 1}). "
                                f"Retrying in {delay:.2f}s..."
                            )
                            await asyncio.sleep(delay)
                            continue
                        else:
                            logger.error(
                                f"OpenLibrary rate limit exceeded after {max_retries} retries"
                            )
                            return (
                                {
                                    "error": "Rate limit exceeded",
                                    "retry_after": retry_after,
                                },
                                429,
                            )
                    elif response.status == 404:
                        return {"error": "Not found"}, 404
                    elif response.status == 403:
                        # 403 Forbidden - often rate limit related for Covers API
                        error_text = await response.text()
                        logger.warning(f"OpenLibrary API returned 403 Forbidden: {error_text}")
                        if attempt < max_retries:
                            # Retry with backoff for 403 (might be rate limit)
                            delay = 2**attempt + random.uniform(0.1, 0.5)
                            logger.warning(
                                f"Retrying 403 error (attempt {attempt + 1}/{max_retries + 1}) "
                                f"in {delay:.2f}s..."
                            )
                            await asyncio.sleep(delay)
                            continue
                        return {"error": "Access forbidden", "details": error_text}, 403
                    else:
                        error_text = await response.text()
                        logger.warning(
                            f"OpenLibrary API returned status {response.status}: {error_text}"
                        )
                        return {"error": f"API request failed: {error_text}"}, response.status

            except (TimeoutError, aiohttp.ClientError, asyncio.CancelledError) as e:
                is_last_attempt = attempt == max_retries

                if is_last_attempt:
                    logger.error(
                        f"Network error in OpenLibrary API request after {max_retries + 1} attempts: {e}"
                    )
                    return {"error": "Network request failed"}, 500

                # Retry on network errors with exponential backoff
                delay = 2**attempt + random.uniform(0.1, 0.5)
                logger.warning(
                    f"Network error (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                    f"Retrying in {delay:.2f}s..."
                )
                await asyncio.sleep(delay)
                continue

            except Exception as e:
                logger.error(f"Unexpected error in OpenLibrary API request: {e}")
                return {"error": "Internal server error"}, 500

        # Should never reach here, but just in case
        return {"error": "Request failed after all retries"}, 500

    def _process_article_item(self, article_data: dict) -> MCNewsItem:
        """
        Process and normalize an article item from NewsAPI.

        Args:
            article_data: Raw article data from NewsAPI

        Returns:
            MCNewsItem with standardized fields
        """
        try:
            # Extract source information
            source_data = article_data.get("source", {})
            # Handle empty string case - if name is empty or None, use default
            source_name = source_data.get("name") or "Unknown Source"
            if isinstance(source_name, str) and source_name.strip() == "":
                source_name = "Unknown Source"
            source = NewsSource(id=source_data.get("id"), name=source_name)

            # Process published date
            published_at = article_data.get("publishedAt")
            if published_at:
                try:
                    # Parse ISO format date
                    pub_date = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                    published_at = pub_date.isoformat()
                except Exception:
                    # Keep original if parsing fails
                    pass

            # Create article with Pydantic model
            # Use news_source= (the actual field name); it serializes as "source" via Field alias
            article = MCNewsItem(  # type: ignore[call-arg]
                title=article_data.get("title", ""),
                description=article_data.get("description"),
                content=article_data.get("content"),
                url=article_data.get("url", ""),
                url_to_image=article_data.get("urlToImage"),
                published_at=published_at,
                author=article_data.get("author"),
                news_source=source,  # Serializes as "source" in API response
            )

            return article

        except Exception as e:
            logger.error(f"Error processing article item: {e}")
            # Return minimal valid article
            return MCNewsItem(  # type: ignore[call-arg]
                title=article_data.get("title", "Error processing article"),
                description="Error processing article data",
                url=article_data.get("url", ""),
                news_source=NewsSource(
                    name="Unknown Source"
                ),  # Serializes as "source" in API response
            )
