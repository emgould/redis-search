"""
YouTube Core Service - Base service for YouTube Data API operations.
Handles trending videos, search, and other YouTube-related functionality.
"""

import re
from datetime import UTC, datetime

# YouTube Data API client
from googleapiclient.errors import HttpError

from api.youtube.auth import Auth
from api.youtube.dynamic import (
    get_channel_videos,
    get_person,
    get_person_details,
    search_videos_async,
)
from api.youtube.models import (
    VideoSearchResponse,
    YouTubeCategoriesResponse,
    YouTubePopularResponse,
    YouTubeSearchResponse,
    YouTubeTrendingResponse,
    YouTubeVideo,
)
from contracts.models import MCSearchResponse, MCType
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

# Cache configuration - 24 hours to conserve API quota
# YouTube Data API has strict quota limits: 10,000 units/day
# Each search costs ~110 units (search.list=100 + videos.list=10)
# This allows only ~90 searches per day, so aggressive caching is essential
CacheExpiration = 60 * 60 * 24  # 24 hours
YouTubeCache = RedisCache(
    defaultTTL=CacheExpiration,
    prefix="youtube",
    verbose=False,
    isClassMethod=True,
    version="2.0.3",  # Version bump for Redis migration
)

logger = get_logger(__name__)


class YouTubeService(Auth):
    """
    Base YouTube service for all YouTube Data API operations.
    Provides core utilities for video processing and API interactions.
    """

    def __init__(self):
        """
        Initialize YouTube service with API key.
        """
        super().__init__()

    def _process_video_item(self, video_data: dict) -> YouTubeVideo:
        """
        Process and normalize a video item from YouTube API.

        Args:
            video_data: Raw video data from YouTube API

        Returns:
            YouTubeVideo model instance
        """
        try:
            snippet = video_data.get("snippet", {})
            statistics = video_data.get("statistics", {})

            # Extract thumbnail with fallback hierarchy
            thumbnails = snippet.get("thumbnails", {})
            thumbnail_url = None
            for quality in ["maxres", "high", "medium", "default"]:
                if quality in thumbnails:
                    thumbnail_url = thumbnails[quality]["url"]
                    break

            # Process duration from contentDetails if available
            duration = None
            if "contentDetails" in video_data:
                duration = video_data["contentDetails"].get("duration")

            video_id = video_data.get("id")
            if isinstance(video_id, dict):
                video_id = video_id.get("videoId")

            # Validate video_id - must be a non-empty string
            if not video_id or not isinstance(video_id, str) or not video_id.strip():
                raise ValueError(f"Invalid video_id: {video_id}")

            processed_video_dict = {
                "id": video_id,
                "video_id": video_id,
                "title": snippet.get("title", ""),
                "description": snippet.get("description", ""),
                "channel_title": snippet.get("channelTitle", ""),
                "channel_id": snippet.get("channelId", ""),
                "published_at": snippet.get("publishedAt", ""),
                "thumbnail_url": thumbnail_url,
                "view_count": int(statistics.get("viewCount", 0)),
                "like_count": int(statistics.get("likeCount", 0)),
                "comment_count": int(statistics.get("commentCount", 0)),
                "duration": duration,
                "tags": snippet.get("tags", []),
                "category_id": snippet.get("categoryId"),
                "default_language": snippet.get("defaultLanguage"),
                "url": f"https://www.youtube.com/watch?v={video_id}",
            }

            # Create YouTubeVideo model (MCBaseItem will auto-generate mc_id and mc_type)
            return YouTubeVideo(**processed_video_dict)

        except Exception as e:
            logger.error(f"Error processing video item: {e}")
            # Return minimal video with error
            return YouTubeVideo(
                id=video_data.get("id", ""),
                video_id=video_data.get("id", ""),
                title="Error processing video",
                url="",
                error=str(e),
            )

    def _clean_title_part(self, part: str) -> str:
        """
        Clean a title part by removing specific words and parenthetical content.

        Args:
            part: Title part to clean

        Returns:
            Cleaned title part with preserved casing
        """
        # Remove anything in parentheses (inclusive)
        part = re.sub(r"\([^)]*\)", "", part)

        # Remove "Official", "Trailer", "Teaser" (case-insensitive, whole words only)
        # Use word boundaries to avoid partial matches
        words_to_remove = ["Official", "Trailer", "Teaser"]
        for word in words_to_remove:
            # Case-insensitive replacement with word boundaries
            part = re.sub(rf"\b{re.escape(word)}\b", "", part, flags=re.IGNORECASE)

        # Clean up extra whitespace
        part = " ".join(part.split())

        return part.strip()

    def _process_title(self, title: str) -> str:
        """
        Process a video title by splitting on "|", taking first/last parts,
        and cleaning them.

        Args:
            title: Original title to process

        Returns:
            Processed title
        """
        # Split on "|"
        parts = [p.strip() for p in title.split("|")]

        # If length > 2, take first and last
        if len(parts) > 2:
            parts = [parts[0], parts[-1]]

        # Clean each part
        cleaned_parts = [self._clean_title_part(part) for part in parts]

        # Filter out empty parts
        cleaned_parts = [part for part in cleaned_parts if part]

        # Join with space
        return " ".join(cleaned_parts)

    async def get_trending_videos(
        self,
        region_code: str = "US",
        language: str = "en",
        max_results: int = 50,
        category_id: str | None = None,
        query: str | None = None,
    ) -> YouTubeTrendingResponse:
        """
        Get trending videos from YouTube.

        Args:
            region_code: ISO 3166-1 alpha-2 country code (default: US)
            language: Language code for results (default: en)
            max_results: Maximum number of results to return (1-50)
            category_id: Optional category ID to filter by
            query: Optional search query to filter trending videos

        Returns:
            dict containing trending videos data with keys:
                - videos: List of processed video items
                - total_results: Number of videos returned
                - region_code: Region code used
                - language: Language code used
                - category_id: Category ID if provided
                - query: Search query if provided
                - fetched_at: ETag from response
                - next_page_token: Token for next page if available
                - prev_page_token: Token for previous page if available
        """
        try:
            logger.info(
                f"🔍 Starting trending videos search: query='official trailer | official promo | official teaser', "
                f"max_results={max_results}, region={region_code}, language={language}, category=24"
            )

            search_response = await self.search_videos(
                query='"official trailer" | "official promo" | "official teaser"',
                max_results=max_results,
                region_code=region_code,
                language=language,
                category_id="24",
            )

            logger.info(
                f"📊 Search completed: {len(search_response.results)} videos returned, "
                f"error={search_response.error is not None}"
            )

            # Check if search returned an error
            if search_response.error:
                return YouTubeTrendingResponse(
                    date=datetime.now(UTC).strftime("%Y-%m-%d"),
                    videos=[],
                    total_results=0,
                    region_code=region_code,
                    language=language,
                    category_id=category_id,
                    query=query,
                    fetched_at="",
                    error=search_response.error,
                )

            # Deduplicate videos based on video_id (unique identifier)
            # This prevents the same video from appearing multiple times
            seen_video_ids = set()
            deduplicated_videos = []
            skipped_no_id = 0
            for video in search_response.results:
                if video.video_id and video.video_id not in seen_video_ids:
                    seen_video_ids.add(video.video_id)
                    deduplicated_videos.append(video)
                elif not video.video_id:
                    # Log videos without video_id for debugging
                    skipped_no_id += 1
                    logger.warning(f"Skipping video without video_id: {video.title}")

            logger.info(
                f"🔄 Deduplication: {len(deduplicated_videos)} unique videos "
                f"(skipped {skipped_no_id} without video_id)"
            )

            # Process titles: split on "|", take first/last if length > 2, clean and rejoin
            processed_videos = []
            for video in deduplicated_videos:
                processed_title = self._process_title(video.title)
                # Create new video instance with processed title
                processed_video = YouTubeVideo(
                    **video.model_dump(exclude={"title"}),
                    title=processed_title,
                )
                processed_videos.append(processed_video)

            # Filter videos with less than 20K views
            filtered_videos = [video for video in processed_videos if video.view_count >= 20000]
            filtered_out = len(processed_videos) - len(filtered_videos)

            if filtered_out > 0:
                logger.info(
                    f"📉 View filter: {filtered_out} videos filtered out (< 20K views), "
                    f"{len(filtered_videos)} remaining"
                )

            result = YouTubeTrendingResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                videos=filtered_videos,
                total_results=len(filtered_videos),
                region_code=region_code,
                language=language,
                category_id=category_id,
                query=query,
                fetched_at=datetime.now(UTC).strftime("%Y-%m-%d"),
            )

            logger.info(
                f"Successfully fetched {len(filtered_videos)} trending videos "
                f"(filtered from {len(processed_videos)} after deduplication, "
                f"original: {len(search_response.results)} results)"
            )

            # Log warning if very few videos returned
            if len(filtered_videos) < 5:
                logger.warning(
                    f"Low video count: {len(filtered_videos)} videos after filtering. "
                    f"Original search returned {len(search_response.results)} videos, "
                    f"after deduplication: {len(deduplicated_videos)}, "
                    f"after view filter: {len(filtered_videos)}"
                )
            return result

        except HttpError as e:
            logger.error(f"YouTube API error: {e}")
            return YouTubeTrendingResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                videos=[],
                total_results=0,
                region_code=region_code,
                language=language,
                category_id=category_id,
                query=query,
                fetched_at="",
                error=f"YouTube API error: {str(e)}",
            )

        except Exception as e:
            logger.exception("Unhandled exception in YouTube service.")
            return YouTubeTrendingResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                videos=[],
                total_results=0,
                region_code=region_code,
                language=language,
                category_id=category_id,
                query=query,
                fetched_at="",
                error=str(e),
            )

    async def search_videos(
        self,
        query: str,
        max_results: int = 5,
        order: str = "relevance",
        published_after: str | None = None,
        region_code: str = "US",
        language: str = "en",
        category_id: str | None = None,
    ) -> YouTubeSearchResponse:
        """
        Search for videos on YouTube.

        QUOTA WARNING: This method is expensive!
        - search().list() costs 100 quota units
        - videos().list() costs 1 unit per video
        - Total: ~110 units per search with max_results=10
        - Daily quota: 10,000 units = ~90 searches/day
        - Cache aggressively (24h TTL) to conserve quota

        Args:
            query: Search query string
            max_results: Maximum number of results to return (1-50, default 5)
            order: Sort order (relevance, date, rating, viewCount, title)
            published_after: RFC 3339 formatted date-time value
            region_code: ISO 3166-1 alpha-2 country code
            language: Language code for results (default: en)

        Returns:
            dict containing search results with keys:
                - videos: List of processed video items
                - total_results: Number of videos returned
                - query: Search query used
                - order: Sort order used
                - region_code: Region code used
                - language: Language code used
                - next_page_token: Token for next page if available
                - prev_page_token: Token for previous page if available
        """
        try:
            logger.info(
                f"⚠️  YouTube API call (costs ~{100 + max_results} quota units): "
                f"query='{query}', max_results={max_results}"
            )

            # Build request parameters
            request_params = {
                "part": "snippet",
                "q": query,
                "type": "video",
                "maxResults": min(max_results, 50),
                "order": order,
                "regionCode": region_code,
                "relevanceLanguage": language,  # Language relevance parameter
            }

            if category_id:
                request_params["videoCategoryId"] = category_id

            if published_after:
                request_params["publishedAfter"] = published_after

            # Execute the search request
            search_request = self.youtube.search().list(**request_params)
            search_response = search_request.execute()

            # Get video IDs for detailed information - safely extract videoId
            video_ids = []
            for item in search_response.get("items", []):
                try:
                    # Handle different response structures
                    item_id = item.get("id", {})
                    if isinstance(item_id, dict):
                        video_id = item_id.get("videoId")
                    else:
                        video_id = item_id

                    if video_id:
                        video_ids.append(video_id)
                except Exception as e:
                    logger.warning(f"Could not extract video ID from item: {e}")
                    continue

            if not video_ids:
                logger.warning(f"No valid video IDs found for query: {query}")
                return YouTubeSearchResponse(
                    date=datetime.now(UTC).strftime("%Y-%m-%d"),
                    results=[],
                    total_results=0,
                    query=query,
                )

            # Get detailed video information
            videos_request = self.youtube.videos().list(
                part="snippet,statistics,contentDetails", id=",".join(video_ids)
            )
            videos_response = videos_request.execute()

            videos = []
            for item in videos_response.get("items", []):
                try:
                    processed_video = self._process_video_item(item)
                    # Skip videos that failed processing (have empty url or invalid video_id)
                    if processed_video.video_id and processed_video.url:
                        videos.append(processed_video)
                    else:
                        logger.warning(
                            f"Skipping video with invalid data: video_id={processed_video.video_id}, "
                            f"url={processed_video.url}, title={processed_video.title}"
                        )
                except Exception as e:
                    logger.warning(f"Failed to process video item, skipping: {e}")
                    continue

            result = YouTubeSearchResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                results=videos,
                total_results=len(videos),
                query=query,
            )

            logger.info(f"Successfully found {len(videos)} videos for query: {query}")
            return result

        except HttpError as e:
            logger.error(f"YouTube API error during search: {e}")
            return YouTubeSearchResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                results=[],
                total_results=0,
                query=query,
                error=f"YouTube API error: {str(e)}",
            )

        except Exception as e:
            logger.exception("Unhandled exception in YouTube service.")
            return YouTubeSearchResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                results=[],
                total_results=0,
                query=query,
                error=str(e),
            )

    async def get_video_categories(
        self, region_code: str = "US", language: str = "en"
    ) -> YouTubeCategoriesResponse:
        """
        Get available video categories for a region.

        Args:
            region_code: ISO 3166-1 alpha-2 country code
            language: Language code for results (default: en)

        Returns:
            dict containing video categories with keys:
                - categories: List of category objects
                - region_code: Region code used
                - language: Language code used
        """
        try:
            logger.info(f"Fetching video categories for region {region_code}, language {language}")

            request = self.youtube.videoCategories().list(
                part="snippet",
                regionCode=region_code,
                hl=language,  # Host language parameter
            )
            response = request.execute()

            from api.youtube.models import YouTubeCategory

            categories = []
            for item in response.get("items", []):
                category = YouTubeCategory(
                    id=item.get("id", ""),
                    title=item["snippet"].get("title", ""),
                    assignable=item["snippet"].get("assignable", False),
                )
                categories.append(category)

            result = YouTubeCategoriesResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                categories=categories,
                region_code=region_code,
                language=language,
            )

            logger.info(f"Successfully fetched {len(categories)} categories")
            return result

        except HttpError as e:
            logger.error(f"YouTube API error fetching categories: {e}")
            return YouTubeCategoriesResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                categories=[],
                region_code=region_code,
                language=language,
                error=f"YouTube API error: {str(e)}",
            )

        except Exception as e:
            logger.exception("Unhandled exception in YouTube service.")
            return YouTubeCategoriesResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                categories=[],
                region_code=region_code,
                language=language,
                error=str(e),
            )

    async def get_popular_videos(
        self,
        query: str | None = None,
        max_results: int = 50,
        region_code: str = "US",
        language: str = "en",
        published_after: str | None = None,
    ) -> YouTubePopularResponse:
        """
        Get popular videos using search API (alternative to trending when mostPopular is restricted).

        Args:
            query: Optional search query, defaults to popular terms if not provided
            max_results: Maximum number of results to return (1-50)
            region_code: ISO 3166-1 alpha-2 country code
            language: Language code for results (default: en)
            published_after: RFC 3339 formatted date-time value

        Returns:
            dict containing popular videos data with keys:
                - videos: List of processed video items
                - total_results: Number of videos returned
                - query: Search query used
                - type: "popular_videos"
                - method: "search_with_viewcount_ordering"
                - note: Explanation of method
                - region_code: Region code used
                - language: Language code used
                - next_page_token: Token for next page if available
                - prev_page_token: Token for previous page if available
        """
        try:
            # If no query provided, use popular search terms
            if not query:
                popular_queries = ["trending", "viral", "popular", "top videos", "best of"]
                query = popular_queries[0]  # Use 'trending' as default

            logger.info(
                f"Fetching popular videos for query: {query}, "
                f"region: {region_code}, language: {language}"
            )

            # Use search with viewCount ordering to get popular videos
            search_result = await self.search_videos(
                query=query,
                max_results=max_results,
                order="viewCount",  # Order by view count for popularity
                published_after=published_after,
                region_code=region_code,
                language=language,
            )

            # Convert search result to popular response
            if search_result.error:
                return YouTubePopularResponse(
                    date=datetime.now(UTC).strftime("%Y-%m-%d"),
                    videos=[],
                    total_results=0,
                    query=query or "",
                    region_code=region_code,
                    language=language,
                    error=search_result.error,
                )

            result = YouTubePopularResponse(
                date=search_result.date or datetime.now(UTC).strftime("%Y-%m-%d"),
                videos=search_result.results,
                total_results=search_result.total_results,
                query=search_result.query,
                type="popular_videos",
                method="search_with_viewcount_ordering",
                note="Popular videos fetched via search API due to trending API restrictions",
                region_code="US",
                language="en",
            )

            logger.info("Successfully fetched popular videos using search method")
            return result

        except Exception as e:
            logger.exception("Unhandled exception in YouTube service.")
            return YouTubePopularResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                videos=[],
                total_results=0,
                query=query or "",
                region_code=region_code,
                language=language,
                error=str(e),
            )

    async def search_videos_async(
        self, query: str, max_results: int = 10, enrich: bool = True
    ) -> VideoSearchResponse:
        """
        Search for videos on YouTube asynchronously using unofficial INNERTUBE API.

        Args:
            query: Search query string
            max_results: Maximum number of results to return
            enrich: If True, fetch additional details for each video (slower but more data).
                    If False, return basic info only (faster, good for autocomplete).
        """
        videos = await search_videos_async(query, max_results, enrich=enrich)
        return VideoSearchResponse(
            date=datetime.now(UTC).strftime("%Y-%m-%d"),
            results=videos,
            total_results=len(videos),
            query=query,
        )

    async def get_channel_videos(
        self, source_id: str, limit: int | None = None
    ) -> MCSearchResponse:
        """Get videos from a channel

        Args:
            source_id: Source ID of the channel
            limit: Maximum number of results to return
        """
        videos = await get_channel_videos(source_id, limit or 10)
        return MCSearchResponse(
            results=list(videos),  # type: ignore[arg-type]
            total_results=len(videos),
            query=source_id,
            data_source="YouTube",
        )

    async def get_person_details(
        self, source_id: str, limit: int | None = None
    ) -> MCSearchResponse:
        """Get details about a creator

        Args:
            source_id: Source ID of the creator
            limit: Maximum number of results to return (unused, kept for API compatibility)
        """
        person = await get_person_details(source_id)
        return MCSearchResponse(
            results=[person],  # type: ignore[arg-type]
            total_results=1,
            data_type=MCType.PERSON,
            query=source_id,
            data_source="YouTube",
        )

    async def search_people(
        self,
        query: str,
        limit: int | None = None,
    ) -> MCSearchResponse:
        """Search for content given a query

        Args:
            query: Search query
            limit: Maximum number of results to return

        Returns:
            MCSearchResponse with results of type YouTubeVideo
        """

        try:
            # Validate that this is a TMDB person
            persons = await get_person(query, limit or 1)
            return MCSearchResponse(
                results=list(persons),  # type: ignore[arg-type]
                total_results=len(persons),
                data_type=MCType.VIDEO,
                query=query,
                data_source="YouTube",
            )

        except Exception as e:
            logger.exception("Unhandled exception in YouTube service.")
            return MCSearchResponse(
                results=[],
                total_results=0,
                data_type=MCType.VIDEO,
                query=query,
                data_source="YouTube",
                status_code=500,
                error=str(e),
            )


# Module-level service instance - lazy initialization to avoid import-time errors
class _LazyYouTubeService:
    """Lazy wrapper for YouTubeService to avoid import-time initialization errors."""

    _instance: YouTubeService | None = None

    def __getattr__(self, name: str):
        """Lazy-load the service instance on first access."""
        if self._instance is None:
            self._instance = YouTubeService()
        return getattr(self._instance, name)


youtube_service = _LazyYouTubeService()
