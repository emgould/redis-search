import asyncio
import json
import re
from typing import Any

from pydantic import BaseModel

from api.tmdb.core import TMDBService
from api.tmdb.wrappers import get_person_credits_async
from contracts.models import MCType
from src.adapters.redis_client import get_redis
from src.adapters.redis_repository import RedisRepository
from src.core.search_queries import (
    build_autocomplete_query,
    build_fuzzy_fulltext_query,
)
from utils.get_logger import get_logger

logger = get_logger(__name__)


# Lazy initialization
_repo = None


def get_repo():
    global _repo
    if _repo is None:
        _repo = RedisRepository()
    return _repo


def reset_repo():
    """Reset the repository to pick up new Redis connection."""
    global _repo
    _repo = None


def parse_doc(doc):
    """Parse Redis Search document, handling JSON documents."""
    result = {"id": doc.id}

    # For JSON documents, parse the 'json' attribute
    if hasattr(doc, "json") and doc.json:
        try:
            parsed = json.loads(doc.json)
            result.update(parsed)
        except (json.JSONDecodeError, TypeError):
            pass

    # Also include any direct attributes
    for key, value in doc.__dict__.items():
        if key not in ("id", "payload", "json") and value is not None:
            result[key] = value

    # Fix legacy person data that may be missing tmdb_ prefix and source_id
    doc_id = result.get("id", "")
    mc_type = result.get("mc_type", "")

    if mc_type == "person":
        # Fix id if missing tmdb_ prefix (e.g., "person_17419" -> "tmdb_person_17419")
        if doc_id.startswith("person_") and not doc_id.startswith("tmdb_"):
            result["id"] = f"tmdb_{doc_id}"
            doc_id = result["id"]

        # Extract source_id from id if not present
        if not result.get("source_id"):
            # Extract numeric ID from "tmdb_person_17419" or "person_17419"
            match = re.search(r"_(\d+)$", doc_id)
            if match:
                result["source_id"] = match.group(1)

    return result


def build_people_autocomplete_query(q: str) -> str:
    """
    Build a prefix search query for people autocomplete.
    Searches both search_title (name) and also_known_as fields.
    """
    words = q.lower().split()
    # Filter out stopwords and empty strings
    stopwords = {
        "the",
        "a",
        "an",
        "and",
        "or",
        "but",
        "in",
        "on",
        "at",
        "to",
        "for",
        "of",
        "is",
        "it",
    }
    words = [w for w in words if w and w not in stopwords]

    if not words:
        return "*"

    # For multi-word: match documents containing all words (last word as prefix)
    if len(words) == 1:
        # Search both name and also_known_as
        return f"(@search_title:{words[0]}*) | (@also_known_as:{words[0]}*)"
    else:
        # All words except last should be exact, last word is prefix
        exact_words = " ".join(words[:-1])
        prefix_word = words[-1]
        name_query = f"@search_title:({exact_words} {prefix_word}*)"
        aka_query = f"@also_known_as:({exact_words} {prefix_word}*)"
        return f"({name_query}) | ({aka_query})"


async def autocomplete(q: str) -> dict[str, list]:
    """
    Autocomplete search that returns categorized results.

    Returns:
        dict with keys: tv, movie, person - each containing list of results
    """
    if not q or len(q) < 2:
        return {"tv": [], "movie": [], "person": []}

    repo = get_repo()

    # Build queries
    media_query = build_autocomplete_query(q)
    people_query = build_people_autocomplete_query(q)

    # Search both indexes concurrently
    try:
        media_task = repo.search(media_query, limit=20)
        people_task = repo.search_people(people_query, limit=10)

        media_res, people_res = await asyncio.gather(
            media_task, people_task, return_exceptions=True
        )
    except Exception:
        # If concurrent search fails, try individually
        try:
            media_res = await repo.search(media_query, limit=20)
        except Exception:
            media_res = type("obj", (object,), {"docs": []})()

        try:
            people_res = await repo.search_people(people_query, limit=10)
        except Exception:
            people_res = type("obj", (object,), {"docs": []})()

    # Handle exceptions from gather
    if isinstance(media_res, Exception):
        media_res = type("obj", (object,), {"docs": []})()
    if isinstance(people_res, Exception):
        people_res = type("obj", (object,), {"docs": []})()

    # Parse and categorize media results
    tv_results = []
    movie_results = []

    for doc in media_res.docs:
        parsed = parse_doc(doc)
        mc_type = parsed.get("mc_type", "")
        if mc_type == "tv":
            tv_results.append(parsed)
        elif mc_type == "movie":
            movie_results.append(parsed)

    # Parse person results
    person_results = [parse_doc(doc) for doc in people_res.docs]

    return {
        "tv": tv_results[:10],  # Limit each category
        "movie": movie_results[:10],
        "person": person_results[:10],
    }


async def full_search(q: str) -> dict[str, list]:
    """
    Full-text search that returns categorized results.

    Returns:
        dict with keys: tv, movie, person - each containing list of results
    """
    if not q:
        return {"tv": [], "movie": [], "person": []}

    repo = get_repo()

    # Build queries
    media_query = build_fuzzy_fulltext_query(q)
    # For people, use a simpler fuzzy query on both fields
    words = q.lower().split()
    stopwords = {
        "the",
        "a",
        "an",
        "and",
        "or",
        "but",
        "in",
        "on",
        "at",
        "to",
        "for",
        "of",
        "is",
        "it",
    }
    words = [w for w in words if w and w not in stopwords]

    if words:
        fuzzy_terms = " ".join(f"%{w}%" for w in words)
        people_query = f"(@search_title:({fuzzy_terms})) | (@also_known_as:({fuzzy_terms}))"
    else:
        people_query = "*"

    # Search both indexes concurrently
    try:
        media_res, people_res = await asyncio.gather(
            repo.search(media_query, limit=20),
            repo.search_people(people_query, limit=10),
            return_exceptions=True,
        )
    except Exception:
        media_res = type("obj", (object,), {"docs": []})()
        people_res = type("obj", (object,), {"docs": []})()

    # Handle exceptions from gather
    if isinstance(media_res, Exception):
        media_res = type("obj", (object,), {"docs": []})()
    if isinstance(people_res, Exception):
        people_res = type("obj", (object,), {"docs": []})()

    # Parse and categorize media results
    tv_results = []
    movie_results = []

    for doc in media_res.docs:
        parsed = parse_doc(doc)
        mc_type = parsed.get("mc_type", "")
        if mc_type == "tv":
            tv_results.append(parsed)
        elif mc_type == "movie":
            movie_results.append(parsed)

    # Parse person results
    person_results = [parse_doc(doc) for doc in people_res.docs]

    return {
        "tv": tv_results,
        "movie": movie_results,
        "person": person_results,
    }


class DetailsRequest(BaseModel):
    """Request model for get_details endpoint."""

    mc_id: str
    source_id: str  # tmdb_id as string
    mc_type: str  # "tv", "movie", or "person"
    mc_subtype: str | None = None


async def get_details(request: DetailsRequest) -> dict[str, Any]:
    """
    Get detailed metadata for a media item or person.

    For tv/movie:
        - First tries to get from Redis index
        - If found, enriches with watch providers from TMDB
        - If not found, fetches from TMDB API with full enhancement

    For person:
        - Gets person metadata from Redis index
        - Fetches movie and TV credits from TMDB

    Args:
        request: DetailsRequest with mc_id, source_id, mc_type, mc_subtype

    Returns:
        Dict with detailed metadata including:
        - All indexed fields
        - For tv/movie: watch_providers, main_cast with images
        - For person: movie_credits, tv_credits
    """
    redis = get_redis()
    mc_type = request.mc_type.lower()

    # Determine the Redis key prefix and index based on type
    if mc_type == "person":
        key_prefix = "person:"
    else:
        key_prefix = "media:"

    # Try to get from Redis index first
    key = f"{key_prefix}{request.mc_id}"
    index_data: dict | None = None

    try:
        raw_data = await redis.json().get(key)  # type: ignore[misc]
        if isinstance(raw_data, dict):
            index_data = raw_data
    except Exception as e:
        logger.warning(f"Error fetching from Redis: {e}")

    # Handle different content types
    if mc_type == "person":
        return await _get_person_details(request, index_data)
    else:
        return await _get_media_details(request, index_data)


async def _get_media_details(request: DetailsRequest, index_data: dict | None) -> dict[str, Any]:
    """
    Get detailed metadata for a movie or TV show.

    If data exists in index, enriches with watch providers.
    If not in index, fetches full details from TMDB.
    """
    tmdb_service = TMDBService()

    # Convert mc_type string to MCType enum
    if request.mc_type.lower() == "tv":
        mc_type_enum = MCType.TV_SERIES
    else:
        mc_type_enum = MCType.MOVIE

    # source_id should be the numeric TMDB ID
    try:
        tmdb_id = int(request.source_id)
    except ValueError:
        return {"error": f"Invalid source_id: {request.source_id}. Expected numeric TMDB ID."}

    if index_data:
        # Data found in index - enrich with watch providers and cast details
        logger.info(f"Found {request.mc_id} in index, enriching with watch providers")

        # Get watch providers and full cast from TMDB
        try:
            detailed = await tmdb_service.get_media_details(
                tmdb_id=tmdb_id,
                media_type=mc_type_enum,
                include_cast=True,
                include_videos=True,
                include_watch_providers=True,
                include_keywords=True,
                cast_limit=20,  # Get more cast for detailed view
            )

            # Merge index data with enriched data
            result = {**index_data}
            result["id"] = request.mc_id
            result["tmdb_id"] = tmdb_id

            # Add enriched fields from TMDB
            if detailed:
                result["watch_providers"] = detailed.watch_providers or {}
                result["streaming_platform"] = detailed.streaming_platform
                result["main_cast"] = detailed.main_cast or []
                result["tmdb_cast"] = detailed.tmdb_cast or {}
                result["tmdb_videos"] = detailed.tmdb_videos or {}
                result["primary_trailer"] = detailed.primary_trailer
                result["trailers"] = detailed.trailers or []
                result["keywords"] = detailed.keywords or []
                result["genres"] = detailed.genres or []
                result["status"] = detailed.status
                result["backdrop_path"] = detailed.backdrop_path
                # Optional attributes that may not exist on all media types
                result["tagline"] = getattr(detailed, "tagline", None)
                result["runtime"] = getattr(detailed, "runtime", None)
                result["number_of_seasons"] = getattr(detailed, "number_of_seasons", None)
                result["number_of_episodes"] = getattr(detailed, "number_of_episodes", None)
                result["director"] = getattr(detailed, "director", None)

                # Use full overview from TMDB if available
                if detailed.overview:
                    result["full_overview"] = detailed.overview

            return result

        except Exception as e:
            logger.error(f"Error enriching media details: {e}")
            # Return index data as fallback
            return {**index_data, "id": request.mc_id, "tmdb_id": tmdb_id}
    else:
        # Not in index - fetch full details from TMDB
        logger.info(f"Not found in index, fetching from TMDB: {tmdb_id}")

        try:
            detailed = await tmdb_service.get_media_details(
                tmdb_id=tmdb_id,
                media_type=mc_type_enum,
                include_cast=True,
                include_videos=True,
                include_watch_providers=True,
                include_keywords=True,
                cast_limit=20,
            )

            if detailed.error:
                return {"error": detailed.error, "status_code": detailed.status_code}

            # Convert to dict and add identifiers
            tmdb_result: dict[str, Any] = detailed.model_dump()
            tmdb_result["id"] = request.mc_id
            tmdb_result["mc_type"] = request.mc_type
            tmdb_result["search_title"] = str(detailed.title or detailed.name or "")

            return tmdb_result

        except Exception as e:
            logger.error(f"Error fetching TMDB details: {e}")
            return {"error": str(e), "status_code": 500}


async def _get_person_details(request: DetailsRequest, index_data: dict | None) -> dict[str, Any]:
    """
    Get detailed metadata for a person (actor/director).

    Gets person data from index and fetches movie/TV credits from TMDB.
    """
    # source_id should be the numeric TMDB ID
    try:
        tmdb_id = int(request.source_id)
    except ValueError:
        return {"error": f"Invalid source_id: {request.source_id}. Expected numeric TMDB ID."}

    # Start with index data or empty dict
    result: dict[str, Any] = {}
    if index_data:
        result = {**index_data}

    # Always set id and mc_type
    result["id"] = request.mc_id
    result["tmdb_id"] = tmdb_id
    result["mc_type"] = "person"

    # Fetch credits from TMDB
    try:
        credits_response = await get_person_credits_async(tmdb_id, limit=50)

        if credits_response.status_code == 200 and credits_response.results:
            credits_result = credits_response.results[0]

            # Add person details if not in index
            if credits_result.person and not index_data:
                person = credits_result.person
                result["search_title"] = person.name
                result["name"] = person.name
                result["overview"] = person.biography
                result["known_for_department"] = person.known_for_department
                result["birthday"] = person.birthday
                result["deathday"] = person.deathday
                result["place_of_birth"] = person.place_of_birth
                result["also_known_as"] = person.also_known_as
                result["popularity"] = person.popularity

                # Add profile image
                if person.profile_path:
                    result["image"] = f"https://image.tmdb.org/t/p/w185{person.profile_path}"
                    result["profile_images"] = {
                        "small": f"https://image.tmdb.org/t/p/w45{person.profile_path}",
                        "medium": f"https://image.tmdb.org/t/p/w185{person.profile_path}",
                        "large": f"https://image.tmdb.org/t/p/h632{person.profile_path}",
                        "original": f"https://image.tmdb.org/t/p/original{person.profile_path}",
                    }
            elif credits_result.person:
                # Supplement index data with additional person info
                person = credits_result.person
                result["name"] = person.name
                result["known_for_department"] = person.known_for_department
                result["birthday"] = person.birthday
                result["deathday"] = person.deathday
                result["place_of_birth"] = person.place_of_birth
                result["also_known_as_list"] = person.also_known_as
                result["full_overview"] = person.biography

                if person.profile_path:
                    result["profile_images"] = {
                        "small": f"https://image.tmdb.org/t/p/w45{person.profile_path}",
                        "medium": f"https://image.tmdb.org/t/p/w185{person.profile_path}",
                        "large": f"https://image.tmdb.org/t/p/h632{person.profile_path}",
                        "original": f"https://image.tmdb.org/t/p/original{person.profile_path}",
                    }

            # Add movie credits
            movie_credits = []
            for movie in credits_result.movies[:20]:  # Limit to top 20
                movie_dict = movie.model_dump()
                movie_credits.append(movie_dict)
            result["movie_credits"] = movie_credits

            # Add TV credits
            tv_credits = []
            for tv_show in credits_result.tv_shows[:20]:  # Limit to top 20
                tv_dict = tv_show.model_dump()
                tv_credits.append(tv_dict)
            result["tv_credits"] = tv_credits

            # Add metadata
            result["credits_metadata"] = credits_result.metadata

        else:
            logger.warning(
                f"Failed to fetch credits for person {tmdb_id}: {credits_response.error}"
            )
            if not index_data:
                return {
                    "error": credits_response.error or "Person not found",
                    "status_code": credits_response.status_code or 404,
                }

    except Exception as e:
        logger.error(f"Error fetching person credits: {e}")
        if not index_data:
            return {"error": str(e), "status_code": 500}

    return result
