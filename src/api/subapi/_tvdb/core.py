"""
TVDB Core Service - Base service for TVDB API operations.
Handles TV show search, details, images, and trending content.
"""

from typing import Any

from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

# Cache configuration
CacheExpiration = 30 * 24 * 60 * 60  # 30 days
TVDBMonthCache = RedisCache(
    defaultTTL=CacheExpiration,
    prefix="tvdb",
    verbose=False,
    isClassMethod=True,
    version="2.0.1",  # Version bump for Redis migration
)

TVDBCache = RedisCache(
    defaultTTL=CacheExpiration,
    prefix="tvdb",
    verbose=False,
    isClassMethod=True,
    version="2.0.1",  # Version bump for Redis migration
)

logger = get_logger(__name__)

try:
    from tvdb_v4_official import TVDB
except ImportError:
    logger.error(
        "tvdb_v4_official package not installed. Please install it with: pip install tvdb_v4_official"
    )
    TVDB = None


class TVDBService:
    """
    Base TVDB service for all TVDB API operations.
    Provides core utilities for TV show search, details, and image retrieval.
    """

    def __init__(self, api_key: str):
        """
        Initialize TVDB service with API key.

        Args:
            api_key: TVDB API key (required)

        Raises:
            ValueError: If API key is not provided
            ImportError: If tvdb_v4_official package is not installed
        """
        if TVDB is None:
            raise ImportError(
                "tvdb_v4_official package is required. Install with: pip install tvdb_v4_official"
            )

        if not api_key:
            raise ValueError("TVDB API key is required")

        self.api_key = api_key

        try:
            self.client = TVDB(self.api_key)
            self.artwork_types = self.client.get_artwork_types()
            self.art_type_map = {item["id"]: item["name"] for item in self.artwork_types}

        except Exception as e:
            logger.error(f"Failed to initialize TVDB client: {e}")
            raise

    def search(self, query: str, search_type: str = "series", limit: int = 10) -> list[dict]:
        """
        Search for TV shows by name using TVDB V4 API official SDK.

        Args:
            query: The search query (show name)
            search_type: Search type (default: "series")
            limit: Maximum number of results to return

        Returns:
            list of matching shows with their IDs and metadata

        Raises:
            ValueError: If query is empty
            Exception: If the API request fails
        """
        if not query:
            raise ValueError("Search query cannot be empty")

        try:
            # Use the official SDK search method
            results = self.client.search(query, type=search_type, limit=limit)

            # Handle different response formats from the SDK
            if isinstance(results, list):
                shows_list = results
            elif isinstance(results, dict):
                shows_list = results.get("data", [])
            else:
                logger.error(f"Unexpected response format from SDK: {type(results)}")
                return []

            # Process and format the results
            formatted_results = []
            for show in shows_list:
                if not isinstance(show, dict):
                    logger.warning(f"Skipping non-dict show result: {show}")
                    continue

                # Get ID and ensure it's an integer
                show_id = show.get("tvdb_id") or show.get("id")
                if show_id is not None:
                    show_id = int(show_id)

                formatted_show = {
                    "id": show_id,
                    "name": show.get("name"),
                    "overview": show.get("overview", ""),
                    "first_air_date": show.get("first_air_time") or show.get("first_air_date"),
                    "status": show.get("status", "Unknown"),
                    "network": show.get("network", "Unknown"),
                    "original_language": show.get("primary_language", "Unknown"),
                    "score": show.get("score", 0),
                }
                formatted_results.append(formatted_show)

            return formatted_results

        except Exception as e:
            logger.error(f"Failed to search TVDB: {e}")
            raise

    def get_show_details(self, tvdb_id: int, extended: bool = False) -> dict | None:
        """
        Get detailed information about a specific show using TVDB V4 API official SDK.

        Args:
            tvdb_id: The TVDB ID of the show
            extended: Whether to fetch extended information with all available fields

        Returns:
            Detailed show information or None if not found

        Raises:
            Exception: If the API request fails
        """
        try:
            if extended:
                # Use extended endpoint for comprehensive data
                show_data = self.client.get_series_extended(tvdb_id)
            else:
                # Use basic series endpoint
                show_data = self.client.get_series(tvdb_id)

            if not show_data:
                return None

            # Basic information always available
            result = {
                "id": show_data.get("id"),
                "tvdb_id": show_data.get("id"),
                "name": show_data.get("name"),
                "slug": show_data.get("slug"),
                "overview": show_data.get("overview", ""),
                "year": show_data.get("year"),
                "score": show_data.get("score", 0),
            }

            # Air dates and status
            result.update(
                {
                    "first_aired": show_data.get("firstAired"),
                    "last_aired": show_data.get("lastAired"),
                    "next_aired": show_data.get("nextAired"),
                    "status": (
                        show_data.get("status", {}).get("name", "Unknown")
                        if show_data.get("status")
                        else "Unknown"
                    ),
                    "airs_time": show_data.get("airsTime"),
                    "airs_days": show_data.get("airsDays"),
                }
            )

            # Geographic and language info
            result.update(
                {
                    "original_country": show_data.get("originalCountry"),
                    "original_language": show_data.get("originalLanguage"),
                    "primary_language": show_data.get("primary_language"),
                }
            )

            # Content details
            result.update(
                {
                    "average_runtime": show_data.get("averageRuntime"),
                    "is_order_randomized": show_data.get("isOrderRandomized"),
                    "default_season_type": show_data.get("defaultSeasonType"),
                }
            )

            # Networks
            original_network = show_data.get("originalNetwork")
            latest_network = show_data.get("latestNetwork")
            result.update(
                {
                    "original_network": (
                        original_network.get("name") if original_network else "Unknown"
                    ),
                    "latest_network": (latest_network.get("name") if latest_network else "Unknown"),
                    "network": (
                        latest_network.get("name")
                        if latest_network
                        else original_network.get("name")
                        if original_network
                        else "Unknown"
                    ),
                }
            )

            # Rich content (only available in extended mode)
            if extended:
                # Genres
                genres = show_data.get("genres", [])
                result["genres"] = [genre.get("name") for genre in genres]

                # Content ratings
                content_ratings = show_data.get("contentRatings", [])
                result["content_ratings"] = [
                    {
                        "rating": rating.get("name"),
                        "country": rating.get("country"),
                        "description": rating.get("description"),
                    }
                    for rating in content_ratings
                ]

                # Trailers
                trailers = show_data.get("trailers", [])
                result["trailers"] = [
                    {
                        "id": trailer.get("id"),
                        "name": trailer.get("name"),
                        "url": trailer.get("url"),
                        "language": trailer.get("language"),
                    }
                    for trailer in trailers
                ]

                # Characters
                characters = show_data.get("characters", [])
                result["characters"] = [
                    {
                        "id": char.get("id"),
                        "name": char.get("name"),
                        "person_name": char.get("personName"),
                        "sort": char.get("sort"),
                    }
                    for char in characters[:10]  # Limit to first 10 main characters
                ]

                # Companies
                companies = show_data.get("companies", [])
                result["companies"] = [
                    {
                        "id": company.get("company", {}).get("id"),
                        "name": company.get("company", {}).get("name"),
                        "type": company.get("companyType", {}).get("companyTypeName"),
                    }
                    for company in companies
                ]

                # Seasons and episodes summary
                seasons = show_data.get("seasons", []) or []
                episodes = show_data.get("episodes", []) or []
                result.update(
                    {
                        "seasons_count": len(seasons),
                        "episodes_count": len(episodes),
                        "seasons": [
                            {
                                "id": season.get("id"),
                                "number": season.get("number"),
                                "name": season.get("name"),
                                "type": season.get("type", {}).get("name"),
                            }
                            for season in seasons
                        ],
                    }
                )

                # External IDs
                remote_ids = show_data.get("remoteIds", []) or []
                result["external_ids"] = {
                    remote_id.get("sourceName", "unknown").lower(): remote_id.get("id")
                    for remote_id in remote_ids
                }

                # Tags
                tags = show_data.get("tags", []) or []
                result["tags"] = [
                    {
                        "id": tag.get("id"),
                        "name": tag.get("name"),
                        "tag_name": tag.get("tagName"),
                    }
                    for tag in tags
                ]

                # Image URL
                result["image"] = show_data.get("image")

                # Last updated
                result["last_updated"] = show_data.get("lastUpdated")

            else:
                # Basic mode - include minimal additional data
                result.update(
                    {
                        "genres": [genre.get("name") for genre in show_data.get("genres", [])],
                        "seasons": show_data.get("seasons", []),
                    }
                )

            return result

        except Exception as e:
            logger.error(f"Failed to get show details for ID {tvdb_id}: {e}")
            raise

    def get_show_images(
        self,
        query: str,
        tvdb_id: int | None = None,
        lang: str = "eng",
        image_types: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Search for a show and return its various image URLs based on requested image types.

        Args:
            query: The show name to search for (used if tvdb_id is not provided)
            tvdb_id: The TVDB ID of the show (bypasses search if provided)
            lang: The language code for images (default: "eng")
            image_types: list of image types to fetch. Options include:
                - poster: Show posters (type 2)
                - banner: Show banners (type 1)
                - background: Show backgrounds/fanart (type 3)
                - logo: Show clear logos (type 23)
                - clearart: Show clear art (type 22)
                - icon: Show icons (type 5)
                - season_posters: Season posters (type 7)
                - season_banners: Season banners (type 6)
                - all: All available image types

        Returns:
            dict with keys for each requested image type, and values as either the URL or None.
            Also includes tvdbid, platform, and show_name keys.
        """
        # Default to these image types if none specified
        if not image_types:
            image_types = ["poster", "logo"]
        elif "all" in image_types:
            image_types = [
                "poster",
                "banner",
                "background",
                "logo",
                "clearart",
                "icon",
                "season_posters",
                "season_banners",
            ]

        # Define mapping from friendly names to TVDB type IDs
        type_id_map = {
            "poster": 2,  # Series poster
            "banner": 1,  # Series banner
            "background": 3,  # Series background/fanart
            "logo": 23,  # Series clear logo
            "clearart": 22,  # Series clear art
            "icon": 5,  # Series icon
            "season_posters": 7,  # Season posters
            "season_banners": 6,  # Season banners
        }

        # Initialize results dictionary with None values for all requested types
        results = dict.fromkeys(image_types)
        results["tvdbid"] = None
        results["platform"] = "Unknown"
        results["show_name"] = None

        try:
            # Search for the show if tvdb_id not provided
            if not tvdb_id:
                search_results = self.search(query, limit=10)
                if not search_results:
                    logger.warning(f"No results found for show: {query}")
                    return results
                show = search_results[0]
                tvdb_id = show.get("id")
                if not tvdb_id:
                    logger.warning(f"No ID found for show: {query}")
                    return results
            else:
                show_details = self.get_show_details(tvdb_id)
                if not show_details:
                    logger.warning(f"No show found for ID: {tvdb_id}")
                    return results
                show = show_details

            # Get extended series info (includes artworks)
            details = self.client.get_series_extended(tvdb_id)
            artworks = details.get("artworks", [])

            # Update basic info
            results["tvdbid"] = tvdb_id
            results["platform"] = show.get("network", "Unknown")
            results["show_name"] = show.get("name")

            # First pass - get all artwork of each requested type
            artwork_by_type: dict[str, list[dict[str, Any]]] = {}
            for image_type in image_types:
                if image_type in type_id_map:
                    type_id = type_id_map[image_type]
                    artwork_by_type[image_type] = []

                    for art in artworks:
                        art_type_id = art.get("type")
                        art_language = art.get("language", "")

                        # Match by type ID and prefer requested language
                        if art_type_id == type_id:
                            artwork_by_type[image_type].append(
                                {
                                    "url": art.get("image"),
                                    "thumbnail": art.get("thumbnail"),
                                    "language": art_language,
                                    "score": art.get("score", 0),
                                    "width": art.get("width", 0),
                                    "height": art.get("height", 0),
                                }
                            )

            # Second pass - select best image for each type based on language and score
            for image_type, images in artwork_by_type.items():
                if not images:
                    continue

                # First try to find images matching the requested language
                matching_lang = [img for img in images if img.get("language") == lang]

                if matching_lang:
                    # Sort by score descending
                    matching_lang.sort(key=lambda x: x.get("score", 0), reverse=True)
                    best_image = matching_lang[0]
                elif images:  # Fallback to any language, highest score
                    images.sort(key=lambda x: x.get("score", 0), reverse=True)
                    best_image = images[0]
                else:  # No images of this type
                    continue

                # Store both full image and thumbnail
                results[image_type] = best_image.get("url")
                results[f"{image_type}_thumbnail"] = best_image.get("thumbnail")

            return results

        except Exception as e:
            logger.error(f"Failed to get show images for '{query}': {e}")
            return results

    def get_all_images(self, tvdb_id: int, lang: str = "eng") -> dict[str, list[dict]]:
        """
        Get all available images for a series, organized by type.

        Args:
            tvdb_id: The TVDB ID of the show
            lang: The language code for images (default: "eng")

        Returns:
            dict with image types as keys and lists of image metadata as values.
        """
        try:
            details = self.client.get_series_extended(tvdb_id)
            artworks = details.get("artworks", [])

            # Group images by type
            images_by_type: dict[str, list[dict[str, Any]]] = {}
            for art in artworks:
                art_type_id = art.get("type")
                art_type_name = self.art_type_map.get(art_type_id, f"type_{art_type_id}")

                if art_type_name not in images_by_type:
                    images_by_type[art_type_name] = []

                images_by_type[art_type_name].append(
                    {
                        "id": art.get("id"),
                        "url": art.get("image"),
                        "thumbnail": art.get("thumbnail"),
                        "language": art.get("language", ""),
                        "score": art.get("score", 0),
                        "width": art.get("width", 0),
                        "height": art.get("height", 0),
                        "includes_text": art.get("includesText", False),
                    }
                )

            # Sort each type by score descending
            for art_type in images_by_type:
                images_by_type[art_type].sort(key=lambda x: x.get("score", 0), reverse=True)

            return images_by_type

        except Exception as e:
            logger.error(f"Failed to get all images for ID {tvdb_id}: {e}")
            return {}

    def get_show_complete(self, tvdb_id: int, lang: str = "eng") -> dict | None:
        """
        Get complete show information including all metadata, images, and related content.

        Args:
            tvdb_id: The TVDB ID of the show
            lang: Language preference

        Returns:
            Complete show information or None if not found
        """
        try:
            # Get extended series details
            show_details = self.get_show_details(tvdb_id, extended=True)
            if not show_details:
                return None

            # Get all images organized by type
            all_images = self.get_all_images(tvdb_id, lang=lang)

            # Get best images for common types
            best_images = self.get_show_images(
                query="",
                tvdb_id=tvdb_id,
                lang=lang,
                image_types=["poster", "banner", "background", "logo", "clearart", "icon"],
            )

            # Combine all data
            complete_data = {**show_details, "images": {"all": all_images, "best": best_images}}

            return complete_data

        except Exception as e:
            logger.error(f"Failed to get complete show data for ID {tvdb_id}: {e}")
            return None

    def search_by_external_id(self, external_id: str, source: str = "imdb") -> dict | None:
        """
        Search for a show by external ID (IMDB, etc.).

        Args:
            external_id: The external ID (e.g., IMDB ID like 'tt0386676')
            source: The source of the ID (imdb, themoviedb, etc.)

        Returns:
            Show information or None if not found
        """
        try:
            # Use the search by remote ID method
            results = self.client.search_by_remote_id(external_id)

            if not results:
                logger.warning(f"No results found for external ID: {external_id}")
                return None

            # Return the first result with basic formatting
            show = results[0]

            # Handle both dict and object responses
            if not isinstance(show, dict):
                logger.warning(f"Result is not a dict, type: {type(show)}, converting to dict")
                # Try to convert object to dict if it has __dict__
                if hasattr(show, "__dict__"):
                    show = show.__dict__
                else:
                    logger.error(f"Cannot convert result to dict. Type: {type(show)}")
                    return None

            # Get ID and ensure it's an integer
            # Try multiple possible ID field names
            show_id = (
                show.get("tvdb_id")
                or show.get("id")
                or show.get("series_id")
                or show.get("objectID")
            )

            if show_id is not None:
                try:
                    show_id = int(show_id)
                except (ValueError, TypeError) as e:
                    logger.error(f"Failed to convert ID to int: {show_id}, error: {e}")
                    show_id = None

            if show_id is None:
                # Log the keys and first few items to help debug
                logger.error(
                    f"No ID found in external search result for {external_id}. "
                    f"Available keys: {list(show.keys())}, "
                    f"Sample data: {dict(list(show.items())[:5])}"
                )

            return {
                "id": show_id,
                "name": show.get("name"),
                "overview": show.get("overview", ""),
                "first_air_date": show.get("first_air_time") or show.get("first_air_date"),
                "status": show.get("status", "Unknown"),
                "network": show.get("network", "Unknown"),
                "external_id": external_id,
                "external_source": source,
            }

        except Exception as e:
            logger.error(f"Failed to search by external ID {external_id}: {e}", exc_info=True)
            return None

    def search_tmdb_multi(
        self, query: str, tmdb_token: str, page: int = 1, limit: int = 20
    ) -> dict:
        """
        Search TMDB using the multi search endpoint that returns movies and TV shows in a unified list.

        Args:
            query: Search query
            tmdb_token: TMDB API bearer token
            page: Page number for pagination
            limit: Maximum number of results per page to return

        Returns:
            Unified search results with pagination metadata
        """
        try:
            import requests

            # TMDB multi search endpoint
            tmdb_url = "https://api.themoviedb.org/3/search/multi"

            headers = {
                "Authorization": f"Bearer {tmdb_token}",
                "Content-Type": "application/json",
            }

            params: dict[str, Any] = {"query": query, "language": "en-US", "page": page}

            response = requests.get(tmdb_url, headers=headers, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()
            results = data.get("results", [])

            # Process and normalize results
            unified_results = []

            for item in results:
                media_type = item.get("media_type")

                # Skip people results, only process movies and TV shows
                if media_type not in ["movie", "tv"]:
                    continue

                # Normalize data structure for both movies and TV shows
                if media_type == "movie":
                    normalized_item = {
                        "id": item.get("id"),
                        "tmdb_id": item.get("id"),
                        "name": item.get("title"),
                        "title": item.get("title"),
                        "overview": item.get("overview", ""),
                        "release_date": item.get("release_date"),
                        "first_air_date": None,  # For consistency
                        "vote_average": item.get("vote_average", 0),
                        "vote_count": item.get("vote_count", 0),
                        "popularity": item.get("popularity", 0),
                        "poster_path": item.get("poster_path"),
                        "backdrop_path": item.get("backdrop_path"),
                        "genre_ids": item.get("genre_ids", []),
                        "original_language": item.get("original_language"),
                        "media_type": "movie",
                        "content_type": "movie",
                        "status": "released",  # Default status for movies
                        "adult": item.get("adult", False),
                    }
                elif media_type == "tv":
                    normalized_item = {
                        "id": item.get("id"),
                        "tmdb_id": item.get("id"),
                        "name": item.get("name"),
                        "title": item.get("name"),
                        "overview": item.get("overview", ""),
                        "release_date": None,  # For consistency
                        "first_air_date": item.get("first_air_date"),
                        "vote_average": item.get("vote_average", 0),
                        "vote_count": item.get("vote_count", 0),
                        "popularity": item.get("popularity", 0),
                        "poster_path": item.get("poster_path"),
                        "backdrop_path": item.get("backdrop_path"),
                        "genre_ids": item.get("genre_ids", []),
                        "origin_country": item.get("origin_country", []),
                        "original_language": item.get("original_language"),
                        "media_type": "tv",
                        "content_type": "tv",
                        "status": "unknown",  # Default status
                    }

                unified_results.append(normalized_item)

            # Apply enhanced sorting
            query_lower = query.lower()
            unified_results.sort(
                key=lambda item: (
                    # 1. Exact name matches first
                    item.get("name", "").lower() != query_lower,
                    # 2. Names that start with query
                    not item.get("name", "").lower().startswith(query_lower),
                    # 3. TV shows with continuing/returning status
                    not (
                        item.get("content_type") == "tv"
                        and item.get("status", "").lower() in ["continuing", "returning"]
                    ),
                    # 4. More recent content first
                    self._get_sort_date(item),
                    # 5. Higher popularity first
                    -(item.get("popularity", 0)),
                    # 6. Higher rating first
                    -(item.get("vote_average", 0)),
                )
            )

            # Limit results per page
            limited_results = unified_results[:limit]

            return {
                "results": limited_results,
                "page": data.get("page", page),
                "total_pages": data.get("total_pages", 1),
                "total_results": len(limited_results),
                "total_api_results": data.get("total_results", 0),
                "query": query,
                "data_source": "TMDB Multi Search",
            }

        except Exception as e:
            logger.error(f"Failed to search TMDB multi for '{query}' (page {page}): {e}")
            return {
                "results": [],
                "page": page,
                "total_pages": 0,
                "total_results": 0,
                "total_api_results": 0,
                "query": query,
                "error": str(e),
                "data_source": "TMDB Multi Search (Error)",
            }

    def _get_sort_date(self, item: dict) -> float:
        """
        Helper method to get a sortable date for content.
        Returns negative timestamp for reverse chronological sorting.
        """
        date_str = item.get("release_date") or item.get("first_air_date") or "1900-01-01"
        try:
            from datetime import datetime

            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            return -date_obj.timestamp()
        except (ValueError, TypeError):
            return 0.0
