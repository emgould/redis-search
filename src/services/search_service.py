import asyncio
import json
import re
import time
from collections.abc import AsyncIterator
from typing import Any

from pydantic import BaseModel

from adapters.redis_client import get_redis
from adapters.redis_repository import RedisRepository
from api.newsai.wrappers import newsai_wrapper
from api.rottentomatoes.wrappers import rottentomatoes_wrapper
from api.subapi.spotify.wrappers import spotify_wrapper
from api.tmdb.core import TMDBService
from api.tmdb.wrappers import get_person_credits_async
from api.youtube.wrappers import youtube_wrapper
from contracts.models import MCType
from core.search_queries import (
    build_autocomplete_query,
    build_filter_query,
    build_fuzzy_fulltext_query,
    escape_redis_search_term,
)
from utils.get_logger import get_logger
from utils.soft_comparison import is_autocomplete_match

logger = get_logger(__name__)


def _rank_person_result(person: dict, query: str) -> tuple[int, int, float]:
    """
    Generate a sort key for ranking person results.

    Prioritizes:
    1. Exact matches (query equals name exactly)
    2. Prefix matches (name starts with query)
    3. Shorter names (when both match similarly)
    4. Higher popularity as tiebreaker

    Returns tuple for sorting: (match_type, name_length, -popularity)
    - match_type: 0 = exact, 1 = prefix, 2 = contains
    - name_length: shorter names rank higher
    - -popularity: higher popularity ranks higher (negative for ascending sort)
    """
    name = (person.get("search_title", "") or person.get("name", "") or "").lower().strip()
    query_lower = query.lower().strip()
    popularity = float(person.get("popularity", 0) or 0)

    # Exact match - highest priority
    if name == query_lower:
        return (0, len(name), -popularity)

    # Prefix match - name starts with query
    if name.startswith(query_lower):
        return (1, len(name), -popularity)

    # Contains match / word match
    return (2, len(name), -popularity)


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

    # Ensure mc_id is always set - prefer parsed id over doc.id (which has Redis key prefix)
    if "mc_id" not in result:
        result["mc_id"] = result.get("id", doc.id)

    # Also include any direct attributes
    for key, value in doc.__dict__.items():
        if key not in ("id", "payload", "json") and value is not None:
            result[key] = value

    # Fix legacy person data that may be missing tmdb_ prefix and source_id
    # BUT skip this for OpenLibrary authors (mc_subtype === "author")
    doc_id = result.get("id", "")
    mc_type = result.get("mc_type", "")
    mc_subtype = result.get("mc_subtype", "")

    if mc_type == "person" and mc_subtype != "author":
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

    Note: RediSearch requires minimum 2 characters for prefix search.
    Single-character last words are handled by searching complete words only
    and relying on post-query filtering.
    """
    # Split on both spaces and colons, then flatten
    parts = q.replace(":", " : ").split()
    words = [w.lower() for w in parts if w and w != ":"]
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

    # Escape special characters in search terms
    escaped_words = [escape_redis_search_term(w) for w in words]

    # For multi-word: match documents containing all words (last word as prefix)
    if len(escaped_words) == 1:
        # Single word - use as prefix (must be 2+ chars for RediSearch)
        if len(words[0]) >= 2:
            return f"(@search_title:{escaped_words[0]}*) | (@also_known_as:{escaped_words[0]}*)"
        else:
            # Single character - too short for prefix, return broad match
            return "*"
    else:
        # Multi-word query
        prefix_word = escaped_words[-1]
        original_prefix_word = words[-1]

        # RediSearch minimum prefix length is 2 characters
        # If last word is only 1 char, search on complete words only
        # Post-query filtering will handle the prefix matching
        if len(original_prefix_word) < 2:
            # Only use complete words, skip the 1-char prefix
            exact_words = " ".join(escaped_words[:-1])
            name_query = f"@search_title:({exact_words})"
            aka_query = f"@also_known_as:({exact_words})"
        else:
            # All words except last should be exact, last word is prefix
            exact_words = " ".join(escaped_words[:-1])
            name_query = f"@search_title:({exact_words} {prefix_word}*)"
            aka_query = f"@also_known_as:({exact_words} {prefix_word}*)"

        return f"({name_query}) | ({aka_query})"


def build_podcasts_autocomplete_query(q: str) -> str:
    """
    Build a prefix search query for podcasts autocomplete.
    Searches both search_title (podcast name) and author fields.
    """
    # Split on both spaces and colons, then flatten
    parts = q.replace(":", " : ").split()
    words = [w.lower() for w in parts if w and w != ":"]
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
        "podcast",
        "show",
    }
    words = [w for w in words if w and w not in stopwords]

    if not words:
        return "*"

    # Escape special characters in search terms
    escaped_words = [escape_redis_search_term(w) for w in words]

    # For multi-word: match documents containing all words (last word as prefix)
    if len(escaped_words) == 1:
        # Search both title and author
        return f"(@search_title:{escaped_words[0]}*) | (@author:{escaped_words[0]}*)"
    else:
        # All words except last should be exact, last word is prefix
        exact_words = " ".join(escaped_words[:-1])
        prefix_word = escaped_words[-1]
        title_query = f"@search_title:({exact_words} {prefix_word}*)"
        author_query = f"@author:({exact_words} {prefix_word}*)"
        return f"({title_query}) | ({author_query})"


def build_authors_autocomplete_query(q: str) -> str:
    """
    Build a prefix search query for authors (OpenLibrary) autocomplete.
    Searches both search_title (name) and name fields.
    """
    # Split on both spaces and colons, then flatten
    parts = q.replace(":", " : ").split()
    words = [w.lower() for w in parts if w and w != ":"]
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

    # Escape special characters in search terms
    escaped_words = [escape_redis_search_term(w) for w in words]

    # For multi-word: match documents containing all words (last word as prefix)
    if len(escaped_words) == 1:
        # Search both search_title and name
        return f"(@search_title:{escaped_words[0]}*) | (@name:{escaped_words[0]}*)"
    else:
        # All words except last should be exact, last word is prefix
        exact_words = " ".join(escaped_words[:-1])
        prefix_word = escaped_words[-1]
        title_query = f"@search_title:({exact_words} {prefix_word}*)"
        name_query = f"@name:({exact_words} {prefix_word}*)"
        return f"({title_query}) | ({name_query})"


def build_books_autocomplete_query(q: str) -> str:
    """
    Build a search query for books (OpenLibrary works) autocomplete.
    Uses simple word matching - BM25 scorer in repository handles ranking.
    """
    # Split on both spaces and colons, then flatten
    parts = q.replace(":", " : ").split()
    words = [w.lower() for w in parts if w and w != ":"]
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

    # Escape special characters in search terms
    escaped_words = [escape_redis_search_term(w) for w in words]

    if len(escaped_words) == 1:
        # Single word - use prefix for autocomplete
        return f"@search_title:{escaped_words[0]}*"
    else:
        # Multiple words - require all words, last word as prefix for autocomplete
        # BM25 scorer will rank shorter/exact matches higher
        parts = [f"@search_title:{w}" for w in escaped_words[:-1]]
        parts.append(f"@search_title:{escaped_words[-1]}*")
        return " ".join(parts)


async def autocomplete(q: str, sources: set[str] | None = None) -> dict[str, list]:
    """
    Autocomplete search that returns categorized results.

    Args:
        q: Search query string
        sources: Optional set of sources to search. If None, searches all sources.
                 Valid sources: tv, movie, person, podcast, author, book, news, video, ratings, artist, album

    Returns:
        dict with keys: tv, movie, person, podcast, author, book, news, video, ratings, artist, album
        Note: news, video, ratings, artist, album results come from external APIs (cached via Redis), not from Redis search indices
    """
    all_sources = {
        "tv",
        "movie",
        "person",
        "podcast",
        "author",
        "book",
        "news",
        "video",
        "ratings",
        "artist",
        "album",
    }
    if sources is None:
        sources = all_sources

    if not q or len(q) < 2:
        return {
            "tv": [],
            "movie": [],
            "person": [],
            "podcast": [],
            "author": [],
            "book": [],
            "news": [],
            "video": [],
            "ratings": [],
            "artist": [],
            "album": [],
        }

    repo = get_repo()

    # Build queries
    media_query = build_autocomplete_query(q)
    people_query = build_people_autocomplete_query(q)
    podcasts_query = build_podcasts_autocomplete_query(q)
    authors_query = build_authors_autocomplete_query(q)
    books_query = build_books_autocomplete_query(q)

    # Debug logging for query with colons
    if ":" in q:
        logger.info(f"Autocomplete query for '{q}': media_query='{media_query}'")

    # Create empty result placeholder
    empty_result = type("obj", (object,), {"docs": []})()

    # Search all indexes AND external APIs concurrently (with timing)
    # Note: News, video, ratings, artist, album are fetched via APIs (cached in Redis) rather than from Redis search indices
    total_start = time.perf_counter()
    try:
        # External API timeout - YouTube INNERTUBE API takes ~1.2s minimum, add buffer for Docker network
        api_timeout = 2.5

        # Build task list based on requested sources
        timed_tasks = []

        # RediSearch (local) - no timeout needed
        # "media" covers both tv and movie
        if "tv" in sources or "movie" in sources:
            timed_tasks.append(timed_task("media", repo.search(media_query, limit=20)))
        if "person" in sources:
            # Fetch more results for post-query filtering (handles 1-char prefix case)
            timed_tasks.append(timed_task("person", repo.search_people(people_query, limit=20)))
        if "podcast" in sources:
            timed_tasks.append(
                timed_task("podcast", repo.search_podcasts(podcasts_query, limit=10))
            )
        if "author" in sources:
            timed_tasks.append(timed_task("author", repo.search_authors(authors_query, limit=10)))
        if "book" in sources:
            timed_tasks.append(timed_task("book", repo.search_books(books_query, limit=10)))

        # External APIs - apply timeout
        if "news" in sources:
            timed_tasks.append(
                timed_task("news", newsai_wrapper.search_news(query=q, page_size=10), api_timeout)
            )
        if "video" in sources:
            timed_tasks.append(
                timed_task(
                    "video", youtube_wrapper.search_videos(query=q, max_results=10), api_timeout
                )
            )
        if "ratings" in sources:
            timed_tasks.append(
                timed_task(
                    "ratings", rottentomatoes_wrapper.search_content(query=q, limit=10), api_timeout
                )
            )
        if "artist" in sources:
            timed_tasks.append(
                timed_task("artist", spotify_wrapper.search_artists(query=q, limit=10), api_timeout)
            )
        if "album" in sources:
            timed_tasks.append(
                timed_task("album", spotify_wrapper.search_albums(query=q, limit=10), api_timeout)
            )

        timed_results = await asyncio.gather(*timed_tasks, return_exceptions=True)
        total_elapsed = (time.perf_counter() - total_start) * 1000

        # Parse timed results and log
        timing_map: dict[str, float] = {}
        results_by_name: dict[str, Any] = {}
        for item in timed_results:
            if isinstance(item, tuple) and len(item) == 3:
                name, result, elapsed = item
                results_by_name[name] = result
                timing_map[name] = elapsed

        timing_parts = [
            f"{k}={v:.0f}ms" for k, v in sorted(timing_map.items(), key=lambda x: -x[1])
        ]
        logger.info(
            f"Autocomplete '{q}' latency: total={total_elapsed:.0f}ms | {' | '.join(timing_parts)}"
        )

        # Map to original variable names
        media_res = results_by_name.get("media", empty_result)
        people_res = results_by_name.get("person", empty_result)
        podcasts_res = results_by_name.get("podcast", empty_result)
        authors_res = results_by_name.get("author", empty_result)
        books_res = results_by_name.get("book", empty_result)
        news_res = results_by_name.get("news")
        video_res = results_by_name.get("video")
        ratings_res = results_by_name.get("ratings")
        artist_res = results_by_name.get("artist")
        album_res = results_by_name.get("album")
    except Exception:
        # If concurrent search fails, try individually
        try:
            media_res = await repo.search(media_query, limit=20)
        except Exception:
            media_res = empty_result

        try:
            people_res = await repo.search_people(people_query, limit=20)
        except Exception:
            people_res = empty_result

        try:
            podcasts_res = await repo.search_podcasts(podcasts_query, limit=10)
        except Exception:
            podcasts_res = empty_result

        try:
            authors_res = await repo.search_authors(authors_query, limit=10)
        except Exception:
            authors_res = empty_result

        try:
            books_res = await repo.search_books(books_query, limit=10)
        except Exception:
            books_res = empty_result

        try:
            news_res = await newsai_wrapper.search_news(query=q, page_size=10)
        except Exception:
            news_res = None

        try:
            video_res = await youtube_wrapper.search_videos(query=q, max_results=10)
        except Exception:
            video_res = None

        try:
            ratings_res = await rottentomatoes_wrapper.search_content(query=q, limit=10)
        except Exception:
            ratings_res = None

        try:
            artist_res = await spotify_wrapper.search_artists(query=q, limit=10)
        except Exception:
            artist_res = None

        try:
            album_res = await spotify_wrapper.search_albums(query=q, limit=10)
        except Exception:
            album_res = None

    # Handle exceptions from gather
    if isinstance(media_res, BaseException):
        media_res = empty_result
    if isinstance(people_res, BaseException):
        people_res = empty_result
    if isinstance(podcasts_res, BaseException):
        podcasts_res = empty_result
    if isinstance(authors_res, BaseException):
        authors_res = empty_result
    if isinstance(books_res, BaseException):
        books_res = empty_result
    if isinstance(news_res, BaseException):
        news_res = None
    if isinstance(video_res, BaseException):
        video_res = None
    if isinstance(ratings_res, BaseException):
        ratings_res = None
    if isinstance(artist_res, BaseException):
        artist_res = None
    if isinstance(album_res, BaseException):
        album_res = None

    # Parse and categorize media results (respecting source filters)
    tv_results: list[dict] = []
    movie_results: list[dict] = []

    for doc in media_res.docs:  # type: ignore[union-attr]
        parsed = parse_doc(doc)
        mc_type = parsed.get("mc_type", "")
        if mc_type == "tv" and "tv" in sources:
            tv_results.append(parsed)
        elif mc_type == "movie" and "movie" in sources:
            movie_results.append(parsed)

    # Parse person results and filter using autocomplete prefix matching
    # This ensures "Rhea S" matches "Rhea Seehorn" even when RediSearch can't handle 1-char prefix
    person_results_raw = [parse_doc(doc) for doc in people_res.docs]  # type: ignore[union-attr]
    person_results_filtered = [
        p
        for p in person_results_raw
        if is_autocomplete_match(q, p.get("search_title", "") or p.get("name", ""))
    ]
    # Re-rank to prioritize exact matches and shorter names over pure popularity
    person_results = sorted(person_results_filtered, key=lambda p: _rank_person_result(p, q))

    # Parse podcast results
    podcast_results = [parse_doc(doc) for doc in podcasts_res.docs]  # type: ignore[union-attr]

    # Parse author results
    author_results = [parse_doc(doc) for doc in authors_res.docs]  # type: ignore[union-attr]

    # Parse book results
    book_results = [parse_doc(doc) for doc in books_res.docs]  # type: ignore[union-attr]

    # Parse news results (from API, not index)
    news_results: list[dict] = []
    if (
        news_res
        and not isinstance(news_res, Exception)
        and news_res.status_code == 200
        and news_res.results
    ):
        news_results = [article.model_dump() for article in news_res.results[:10]]

    # Parse video results (from YouTube API, cached with 24h TTL)
    video_results: list[dict] = []
    if (
        video_res
        and not isinstance(video_res, Exception)
        and video_res.status_code == 200
        and video_res.results
    ):
        video_results = [video.model_dump() for video in video_res.results[:10]]

    # Parse ratings results (from RottenTomatoes API, cached with 72h TTL)
    ratings_results: list[dict] = []
    if (
        ratings_res
        and not isinstance(ratings_res, Exception)
        and ratings_res.status_code == 200
        and ratings_res.results
    ):
        ratings_results = [item.model_dump() for item in ratings_res.results[:10]]

    # Parse artist results (from Spotify API, cached with 24h TTL)
    artist_results: list[dict] = []
    if (
        artist_res
        and not isinstance(artist_res, Exception)
        and artist_res.status_code == 200
        and artist_res.results
    ):
        artist_results = [artist.model_dump() for artist in artist_res.results[:10]]

    # Parse album results (from Spotify API, cached with 24h TTL)
    album_results: list[dict] = []
    if (
        album_res
        and not isinstance(album_res, Exception)
        and album_res.status_code == 200
        and album_res.results
    ):
        album_results = [album.model_dump() for album in album_res.results[:10]]

    return {
        "tv": tv_results[:10],  # Limit each category
        "movie": movie_results[:10],
        "person": person_results[:10],
        "podcast": podcast_results[:10],
        "author": author_results[:10],
        "book": book_results[:10],
        "news": news_results,  # From API (cached in Redis), not indexed
        "video": video_results,  # From YouTube API (24h cache), not indexed
        "ratings": ratings_results,  # From RottenTomatoes API (72h cache), not indexed
        "artist": artist_results,  # From Spotify API (24h cache), not indexed
        "album": album_results,  # From Spotify API (24h cache), not indexed
    }


async def autocomplete_stream(
    q: str, sources: set[str] | None = None
) -> AsyncIterator[tuple[str, list, float]]:
    """
    Streaming autocomplete that yields results as they become available.

    Uses asyncio.as_completed() to return fast sources first.
    Each yield contains: (source_name, results_list, latency_ms)

    Args:
        q: Search query string
        sources: Optional set of sources to search. If None, searches all sources.
                 Valid sources: tv, movie, person, podcast, author, book, news, video, ratings, artist, album

    Yields:
        tuple of (source_name, results, latency_ms) as each source completes
    """
    all_sources = {
        "tv",
        "movie",
        "person",
        "podcast",
        "author",
        "book",
        "news",
        "video",
        "ratings",
        "artist",
        "album",
    }
    if sources is None:
        sources = all_sources

    if not q or len(q) < 2:
        return

    repo = get_repo()

    # Build queries
    media_query = build_autocomplete_query(q)
    people_query = build_people_autocomplete_query(q)
    podcasts_query = build_podcasts_autocomplete_query(q)
    authors_query = build_authors_autocomplete_query(q)
    books_query = build_books_autocomplete_query(q)

    # External API timeout - YouTube INNERTUBE API takes ~1.2s minimum, add buffer for Docker network
    api_timeout = 2.5

    # Create named tasks based on requested sources
    tasks_dict: dict[asyncio.Task, str] = {}  # type: ignore[type-arg]

    # RediSearch tasks (local) - no timeout
    # "media" covers both tv and movie
    if "tv" in sources or "movie" in sources:
        tasks_dict[asyncio.create_task(timed_task("media", repo.search(media_query, limit=20)))] = (
            "media"
        )
    if "person" in sources:
        # Fetch more results for post-query filtering (handles 1-char prefix case)
        tasks_dict[
            asyncio.create_task(timed_task("person", repo.search_people(people_query, limit=20)))
        ] = "person"
    if "podcast" in sources:
        tasks_dict[
            asyncio.create_task(
                timed_task("podcast", repo.search_podcasts(podcasts_query, limit=10))
            )
        ] = "podcast"
    if "author" in sources:
        tasks_dict[
            asyncio.create_task(timed_task("author", repo.search_authors(authors_query, limit=10)))
        ] = "author"
    if "book" in sources:
        tasks_dict[
            asyncio.create_task(timed_task("book", repo.search_books(books_query, limit=10)))
        ] = "book"

    # External API tasks - with timeout
    if "news" in sources:
        tasks_dict[
            asyncio.create_task(
                timed_task("news", newsai_wrapper.search_news(query=q, page_size=10), api_timeout)
            )
        ] = "news"
    if "video" in sources:
        tasks_dict[
            asyncio.create_task(
                timed_task(
                    "video", youtube_wrapper.search_videos(query=q, max_results=10), api_timeout
                )
            )
        ] = "video"
    if "ratings" in sources:
        tasks_dict[
            asyncio.create_task(
                timed_task(
                    "ratings", rottentomatoes_wrapper.search_content(query=q, limit=10), api_timeout
                )
            )
        ] = "ratings"
    if "artist" in sources:
        tasks_dict[
            asyncio.create_task(
                timed_task("artist", spotify_wrapper.search_artists(query=q, limit=10), api_timeout)
            )
        ] = "artist"
    if "album" in sources:
        tasks_dict[
            asyncio.create_task(
                timed_task("album", spotify_wrapper.search_albums(query=q, limit=10), api_timeout)
            )
        ] = "album"

    # If no tasks, return early
    if not tasks_dict:
        return

    total_start = time.perf_counter()
    timing_parts: list[str] = []

    # Yield results as they complete (fastest first)
    for completed_task in asyncio.as_completed(tasks_dict.keys()):
        try:
            result = await completed_task
            if not isinstance(result, tuple) or len(result) != 3:
                continue

            name, data, elapsed = result
            timing_parts.append(f"{name}={elapsed:.0f}ms")

            # Parse results based on source type
            parsed_results: list[dict] = []

            if name == "media":
                if data and not isinstance(data, BaseException) and hasattr(data, "docs"):
                    tv_results = []
                    movie_results = []
                    for doc in data.docs:
                        parsed = parse_doc(doc)
                        mc_type = parsed.get("mc_type", "")
                        if mc_type == "tv" and "tv" in sources:
                            tv_results.append(parsed)
                        elif mc_type == "movie" and "movie" in sources:
                            movie_results.append(parsed)
                    # Yield TV and movie separately (only if requested)
                    if tv_results and "tv" in sources:
                        yield ("tv", tv_results[:10], elapsed)
                    if movie_results and "movie" in sources:
                        yield ("movie", movie_results[:10], elapsed)
                continue  # Already yielded tv/movie

            elif name == "person":
                # Person results need autocomplete prefix filtering and re-ranking
                if data and not isinstance(data, BaseException) and hasattr(data, "docs"):
                    parsed_all = [parse_doc(doc) for doc in data.docs]
                    filtered = [
                        p
                        for p in parsed_all
                        if is_autocomplete_match(q, p.get("search_title", "") or p.get("name", ""))
                    ]
                    # Re-rank to prioritize exact matches and shorter names
                    parsed_results = sorted(filtered, key=lambda p: _rank_person_result(p, q))[:10]

            elif name in ("podcast", "author", "book"):
                if data and not isinstance(data, BaseException) and hasattr(data, "docs"):
                    parsed_results = [parse_doc(doc) for doc in data.docs][:10]

            elif name in ("news", "video", "ratings", "artist", "album"):
                if (
                    data
                    and not isinstance(data, BaseException)
                    and hasattr(data, "status_code")
                    and data.status_code == 200
                    and hasattr(data, "results")
                    and data.results
                ):
                    parsed_results = [item.model_dump() for item in data.results[:10]]

            if parsed_results:
                yield (name, parsed_results, elapsed)

        except Exception as e:
            logger.warning(f"Error processing streaming result: {e}")
            continue

    # Log total timing
    total_elapsed = (time.perf_counter() - total_start) * 1000
    logger.info(
        f"Autocomplete stream '{q}' latency: total={total_elapsed:.0f}ms | {' | '.join(timing_parts)}"
    )


# Valid source types for the search API
VALID_SOURCES = {
    # Indexed in RediSearch
    "tv",
    "movie",
    "person",
    "podcast",
    "author",
    "book",
    # Brokered via Redis-cached API calls
    "artist",
    "album",
    "video",
    "news",
    "ratings",
}


async def timed_task(
    name: str, coro: Any, timeout_seconds: float | None = None
) -> tuple[str, Any, float]:
    """Wrap a coroutine to measure its execution time with optional timeout."""
    start = time.perf_counter()
    try:
        if timeout_seconds:
            result = await asyncio.wait_for(coro, timeout=timeout_seconds)
        else:
            result = await coro
        elapsed = (time.perf_counter() - start) * 1000  # ms
        return (name, result, elapsed)
    except TimeoutError:
        elapsed = (time.perf_counter() - start) * 1000
        logger.warning(f"Task '{name}' timed out after {elapsed:.0f}ms (limit: {timeout_seconds}s)")
        return (name, None, elapsed)
    except Exception as e:
        elapsed = (time.perf_counter() - start) * 1000
        return (name, e, elapsed)


async def search(
    q: str | None,
    sources: set[str] | None = None,
    limit: int = 10,
    genre_ids: list[str] | None = None,
    genre_match: str = "any",
    cast_ids: list[str] | None = None,
    cast_match: str = "any",
    year_min: int | None = None,
    year_max: int | None = None,
    rating_min: float | None = None,
    rating_max: float | None = None,
    mc_type: str | None = None,
    ratings_sort: str = "popularity",
) -> dict[str, list]:
    """
    Unified search API that returns categorized results.

    Supports text search, field filtering, or both combined.

    Args:
        q: Search query string (optional if filters provided)
        sources: Set of source types to search. If None, searches all sources.
                 Valid sources: tv, movie, person, podcast, author, book, artist, album, video, news, ratings
        limit: Maximum results per source (default: 10)
        genre_ids: List of TMDB genre IDs to filter by
        genre_match: "any" for OR logic (default), "all" for AND logic
        cast_ids: List of TMDB person IDs to filter by
        cast_match: "any" for OR logic (default), "all" for AND logic
        year_min: Minimum release year (inclusive)
        year_max: Maximum release year (inclusive)
        rating_min: Minimum rating 0-10 (inclusive)
        rating_max: Maximum rating 0-10 (inclusive)
        mc_type: Filter by media type (movie, tv)
        ratings_sort: Sort order for ratings results when ratings source is requested.
                     Options: "popularity" (default), "audience_score", "critics_score".
                     Sorts in descending order (highest first).

    Returns:
        dict with keys for each requested source, each containing list of MCBaseItem-compliant results
    """
    # Check if we have filters or query
    has_filters = any([genre_ids, cast_ids, year_min, year_max, rating_min, rating_max, mc_type])
    has_query = q and len(q) >= 2

    if not has_query and not has_filters:
        # Return empty dict with all requested source keys
        requested = sources if sources else VALID_SOURCES
        return {src: [] for src in requested}

    # Default to all sources if none specified
    requested_sources = sources if sources else VALID_SOURCES
    # Filter to only valid sources
    requested_sources = requested_sources & VALID_SOURCES

    if not requested_sources:
        return {}

    repo = get_repo()

    # Build queries for indexed sources
    # Use filter query if we have filters OR if we have a query to combine with filters
    if has_filters:
        media_query = build_filter_query(
            q=q if has_query else None,
            genre_ids=genre_ids,
            genre_match=genre_match,
            cast_ids=cast_ids,
            cast_match=cast_match,
            year_min=year_min,
            year_max=year_max,
            rating_min=rating_min,
            rating_max=rating_max,
            mc_type=mc_type,
        )
    elif has_query and q:  # q check needed for type narrowing
        media_query = build_autocomplete_query(q)
    else:
        media_query = "*"

    # For other indices, only build query if we have a text query
    if has_query and q:  # q check needed for type narrowing
        people_query = build_people_autocomplete_query(q)
        podcasts_query = build_podcasts_autocomplete_query(q)
        authors_query = build_authors_autocomplete_query(q)
        books_query = build_books_autocomplete_query(q)
    else:
        # Filter-only mode: use match-all for indices without specific filters
        people_query = "*"
        podcasts_query = "*"
        authors_query = "*"
        books_query = "*"

    # Create empty result placeholder for indexed searches
    empty_result = type("obj", (object,), {"docs": []})()

    # Build task list based on requested sources (wrapped with timing)
    # External API timeout - YouTube INNERTUBE API takes ~1.2s minimum, add buffer for Docker network
    api_timeout = 2.5
    timed_tasks: list[Any] = []

    # Indexed sources (RediSearch) - no timeout needed
    if "tv" in requested_sources or "movie" in requested_sources:
        timed_tasks.append(timed_task("media", repo.search(media_query, limit=limit * 2)))
    if "person" in requested_sources:
        # Fetch more results for post-query filtering (handles 1-char prefix case)
        timed_tasks.append(timed_task("person", repo.search_people(people_query, limit=limit * 2)))
    if "podcast" in requested_sources:
        timed_tasks.append(timed_task("podcast", repo.search_podcasts(podcasts_query, limit=limit)))
    if "author" in requested_sources:
        timed_tasks.append(timed_task("author", repo.search_authors(authors_query, limit=limit)))
    if "book" in requested_sources:
        timed_tasks.append(timed_task("book", repo.search_books(books_query, limit=limit)))

    # Brokered sources (Redis-cached API calls) - apply timeout
    # Ratings can be enriched from indexed results when no query is provided
    # Other sources require a text query - skip if no query provided
    if has_query:
        if "news" in requested_sources:
            timed_tasks.append(
                timed_task("news", newsai_wrapper.search_news(query=q, page_size=limit), api_timeout)
            )
        if "video" in requested_sources:
            timed_tasks.append(
                timed_task(
                    "video", youtube_wrapper.search_videos(query=q, max_results=limit), api_timeout
                )
            )
        if "ratings" in requested_sources:
            timed_tasks.append(
                timed_task(
                    "ratings", rottentomatoes_wrapper.search_content(query=q, limit=limit), api_timeout
                )
            )
        if "artist" in requested_sources:
            timed_tasks.append(
                timed_task("artist", spotify_wrapper.search_artists(query=q, limit=limit), api_timeout)
            )
        if "album" in requested_sources:
            timed_tasks.append(
                timed_task("album", spotify_wrapper.search_albums(query=q, limit=limit), api_timeout)
            )

    # Execute all tasks concurrently
    total_start = time.perf_counter()
    try:
        timed_results = await asyncio.gather(*timed_tasks, return_exceptions=True)
    except Exception:
        # If gather itself fails, return empty results
        return {src: [] for src in requested_sources}
    total_elapsed = (time.perf_counter() - total_start) * 1000

    # Log timing for each source
    results_map: dict[str, Any] = {}
    timing_map: dict[str, float] = {}
    for item in timed_results:
        if isinstance(item, tuple) and len(item) == 3:
            name, result, elapsed = item
            results_map[name] = result
            timing_map[name] = elapsed
        elif isinstance(item, BaseException):
            logger.error(f"Task failed: {item}")

    # Sort by slowest first to highlight bottlenecks
    timing_parts = [f"{k}={v:.0f}ms" for k, v in sorted(timing_map.items(), key=lambda x: -x[1])]
    query_desc = f"'{q}'" if q else "[filters only]"
    logger.info(f"Search {query_desc} latency: total={total_elapsed:.0f}ms | {' | '.join(timing_parts)}")

    # Handle exceptions and parse results
    final_results: dict[str, list] = {}

    # Process media (tv/movie) results
    if "media" in results_map:
        media_res = results_map["media"]
        if isinstance(media_res, BaseException):
            media_res = empty_result

        tv_results: list[dict] = []
        movie_results: list[dict] = []

        for doc in media_res.docs:
            parsed = parse_doc(doc)
            mc_type = parsed.get("mc_type", "")
            if mc_type == "tv" and "tv" in requested_sources:
                tv_results.append(parsed)
            elif mc_type == "movie" and "movie" in requested_sources:
                movie_results.append(parsed)

        if "tv" in requested_sources:
            final_results["tv"] = tv_results[:limit]
        if "movie" in requested_sources:
            final_results["movie"] = movie_results[:limit]

    # Process person results with autocomplete prefix filtering and re-ranking
    if "person" in results_map:
        person_res = results_map["person"]
        if isinstance(person_res, BaseException):
            person_res = empty_result
        parsed_people = [parse_doc(doc) for doc in person_res.docs]

        if has_query and q:
            # Filter using autocomplete prefix matching (handles 1-char prefix case)
            filtered_people = [
                p
                for p in parsed_people
                if is_autocomplete_match(q, p.get("search_title", "") or p.get("name", ""))
            ]
            # Re-rank to prioritize exact matches and shorter names over pure popularity
            final_results["person"] = sorted(
                filtered_people, key=lambda p: _rank_person_result(p, q)
            )[:limit]
        else:
            # Filter-only mode: return results as-is (sorted by popularity from Redis)
            final_results["person"] = parsed_people[:limit]

    # Process podcast results
    if "podcast" in results_map:
        podcast_res = results_map["podcast"]
        if isinstance(podcast_res, BaseException):
            podcast_res = empty_result
        final_results["podcast"] = [parse_doc(doc) for doc in podcast_res.docs][:limit]

    # Process author results
    if "author" in results_map:
        author_res = results_map["author"]
        if isinstance(author_res, BaseException):
            author_res = empty_result
        final_results["author"] = [parse_doc(doc) for doc in author_res.docs][:limit]

    # Process book results
    if "book" in results_map:
        book_res = results_map["book"]
        if isinstance(book_res, BaseException):
            book_res = empty_result
        final_results["book"] = [parse_doc(doc) for doc in book_res.docs][:limit]

    # Process news results (API)
    if "news" in results_map:
        news_res = results_map["news"]
        news_results: list[dict] = []
        if (
            news_res
            and not isinstance(news_res, BaseException)
            and news_res.status_code == 200
            and news_res.results
        ):
            news_results = [article.model_dump() for article in news_res.results[:limit]]
        final_results["news"] = news_results

    # Process video results (API)
    if "video" in results_map:
        video_res = results_map["video"]
        video_results: list[dict] = []
        if (
            video_res
            and not isinstance(video_res, BaseException)
            and video_res.status_code == 200
            and video_res.results
        ):
            video_results = [video.model_dump() for video in video_res.results[:limit]]
        final_results["video"] = video_results

    # Process ratings results (API)
    # Two scenarios:
    # 1. With query: ratings come from direct RT search (already in results_map)
    # 2. Without query: enrich from indexed tv/movie results
    ratings_results: list[dict] = []

    if "ratings" in results_map:
        # Scenario 1: Ratings from direct search (has query)
        ratings_res = results_map["ratings"]
        if (
            ratings_res
            and not isinstance(ratings_res, BaseException)
            and ratings_res.status_code == 200
            and ratings_res.results
        ):
            ratings_results = [item.model_dump() for item in ratings_res.results[:limit]]
    elif not has_query and "ratings" in requested_sources and ("tv" in requested_sources or "movie" in requested_sources):
        # Scenario 2: Enrich ratings from indexed results (no query, filter-only)
        # Collect titles from indexed tv/movie results
        indexed_titles: list[tuple[str, int | None, str]] = []  # (title, year, mc_type)

        if "tv" in final_results:
            for item in final_results["tv"]:
                title = item.get("search_title") or item.get("title") or ""
                year = item.get("year")
                if title:
                    indexed_titles.append((title, year, "tv"))

        if "movie" in final_results:
            for item in final_results["movie"]:
                title = item.get("search_title") or item.get("title") or ""
                year = item.get("year")
                if title:
                    indexed_titles.append((title, year, "movie"))

        # Search RottenTomatoes for each title and match by title/year
        if indexed_titles:
            logger.info(f"Enriching {len(indexed_titles)} titles with RottenTomatoes ratings")
            rt_tasks = []
            # Limit to avoid too many API calls
            titles_to_enrich = indexed_titles[:limit * 2]
            for title, _year, mc_type_str in titles_to_enrich:
                mc_type_enum = MCType.TV_SERIES if mc_type_str == "tv" else MCType.MOVIE
                rt_tasks.append(
                    timed_task(
                        f"rt_{title[:20]}",
                        rottentomatoes_wrapper.search_content(
                            query=title, limit=5, media_type=mc_type_enum
                        ),
                        api_timeout,
                    )
                )

            # Execute RT searches concurrently
            rt_results = await asyncio.gather(*rt_tasks, return_exceptions=True)

            # Track which indexed titles have been matched to avoid duplicates
            matched_titles: set[str] = set()

            # Match RT results to indexed titles
            for i, rt_result in enumerate(rt_results):
                if isinstance(rt_result, BaseException) or not isinstance(rt_result, tuple):
                    continue

                _, rt_response, _ = rt_result
                if (
                    not rt_response
                    or isinstance(rt_response, BaseException)
                    or rt_response.status_code != 200
                    or not rt_response.results
                ):
                    continue

                # Get the corresponding indexed title for this RT search
                if i >= len(titles_to_enrich):
                    continue
                idx_title, idx_year, idx_type = titles_to_enrich[i]
                idx_title_lower = idx_title.lower().strip()

                # Find best match from RT results
                best_match: dict | None = None
                best_score = 0

                for rt_item in rt_response.results:
                    rt_title = (rt_item.title or "").lower().strip()
                    rt_year = rt_item.release_year

                    # Skip if already matched
                    if rt_title in matched_titles:
                        continue

                    # Calculate match score
                    score = 0

                    # Title match (exact or substring)
                    if idx_title_lower == rt_title:
                        score += 100  # Exact match
                    elif idx_title_lower in rt_title or rt_title in idx_title_lower:
                        score += 50  # Partial match

                    # Year match (bonus)
                    if idx_year and rt_year and idx_year == rt_year:
                        score += 20

                    if score > best_score:
                        best_score = score
                        best_match = rt_item.model_dump()

                # Add best match if found
                if best_match and best_score > 0:
                    matched_titles.add((best_match.get("title") or "").lower().strip())
                    ratings_results.append(best_match)

    # Sort ratings results if ratings are requested along with tv/movie
    if ratings_results and "ratings" in requested_sources and ("tv" in requested_sources or "movie" in requested_sources):
        if ratings_sort == "audience_score":
            ratings_results.sort(
                key=lambda x: int(x.get("audience_score") or -1),
                reverse=True,
            )
        elif ratings_sort == "critics_score":
            ratings_results.sort(
                key=lambda x: int(x.get("critics_score") or -1),
                reverse=True,
            )
        else:  # popularity (default)
            ratings_results.sort(
                key=lambda x: int(x.get("popularity") or -1),
                reverse=True,
            )

    final_results["ratings"] = ratings_results[:limit]

    # Process artist results (API)
    if "artist" in results_map:
        artist_res = results_map["artist"]
        artist_results: list[dict] = []
        if (
            artist_res
            and not isinstance(artist_res, BaseException)
            and artist_res.status_code == 200
            and artist_res.results
        ):
            artist_results = [artist.model_dump() for artist in artist_res.results[:limit]]
        final_results["artist"] = artist_results

    # Process album results (API)
    if "album" in results_map:
        album_res = results_map["album"]
        album_results: list[dict] = []
        if (
            album_res
            and not isinstance(album_res, BaseException)
            and album_res.status_code == 200
            and album_res.results
        ):
            album_results = [album.model_dump() for album in album_res.results[:limit]]
        final_results["album"] = album_results

    # Ensure all requested sources have a key (even if empty)
    for src in requested_sources:
        if src not in final_results:
            final_results[src] = []

    return final_results


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
    # Split on both spaces and colons, then flatten
    parts = q.replace(":", " : ").split()
    words = [w.lower() for w in parts if w and w != ":"]
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
        # Escape special characters in search terms
        escaped_words = [escape_redis_search_term(w) for w in words]
        fuzzy_terms = " ".join(f"%{w}%" for w in escaped_words)
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

    For podcast:
        - Gets podcast metadata from Redis index

    Args:
        request: DetailsRequest with mc_id, source_id, mc_type, mc_subtype

    Returns:
        Dict with detailed metadata including:
        - All indexed fields
        - For tv/movie: watch_providers, main_cast with images
        - For person: movie_credits, tv_credits
        - For podcast: podcast metadata
    """
    redis = get_redis()
    mc_type = request.mc_type.lower()
    mc_subtype = (request.mc_subtype or "").lower()

    # Check if this is an author (OpenLibrary) - they have mc_type=person, mc_subtype=author
    is_author = mc_subtype == "author"
    is_book = mc_type == "book"

    # Determine the Redis key prefix and index based on type
    if is_author:
        key_prefix = "author:"
    elif is_book:
        key_prefix = "book:"
    elif mc_type == "person":
        key_prefix = "person:"
    elif mc_type == "podcast":
        key_prefix = "podcast:"
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
    if is_author:
        return await _get_author_details(request, index_data)
    elif is_book:
        return await _get_book_details(request, index_data)
    elif mc_type == "person":
        return await _get_person_details(request, index_data)
    elif mc_type == "podcast":
        return await _get_podcast_details(request, index_data)
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


async def _get_podcast_details(request: DetailsRequest, index_data: dict | None) -> dict[str, Any]:
    """
    Get detailed metadata for a podcast using the PodcastIndex API.

    Uses the same get_podcast_by_id wrapper that the mobile app uses.
    Falls back to Redis index data if the API call fails.
    """
    from api.podcast.wrappers import podcast_wrapper

    # source_id should be the numeric PodcastIndex feed ID
    try:
        feed_id = int(request.source_id)
    except ValueError:
        return {
            "error": f"Invalid source_id: {request.source_id}. Expected numeric feed ID.",
            "status_code": 400,
        }

    # Use the existing podcast wrapper (same as mobile app)
    try:
        result = await podcast_wrapper.get_podcast_by_id(feed_id)

        if result.status_code != 200 or result.error:
            # Fall back to index data if API fails
            if index_data:
                logger.warning(
                    f"API failed for podcast {feed_id}, using index data: {result.error}"
                )
                return {**index_data, "id": request.mc_id, "mc_type": "podcast"}
            return {
                "error": result.error or "Podcast not found",
                "status_code": result.status_code or 404,
            }

        # Convert to dict and ensure proper identifiers
        podcast_data: dict[str, Any] = result.model_dump()
        podcast_data["mc_id"] = request.mc_id
        podcast_data["mc_type"] = "podcast"

        return podcast_data

    except Exception as e:
        logger.error(f"Error fetching podcast details: {e}")
        # Fall back to index data if available
        if index_data:
            return {**index_data, "id": request.mc_id, "mc_type": "podcast"}
        return {"error": str(e), "status_code": 500}


async def _get_author_details(request: DetailsRequest, index_data: dict | None) -> dict[str, Any]:
    """
    Get detailed metadata for an author from OpenLibrary.

    Authors are stored in the idx:author index with author: prefix.
    Unlike TMDB persons, we don't need to fetch external API data -
    all the data is already in Redis from the OpenLibrary dump.
    """
    if not index_data:
        return {
            "error": f"Author not found: {request.mc_id}",
            "status_code": 404,
        }

    # Build result from index data
    result: dict[str, Any] = {**index_data}

    # Ensure proper identifiers
    result["id"] = request.mc_id
    result["mc_type"] = "person"
    result["mc_subtype"] = "author"

    # Use author_links from index data (already built during ETL)
    result["author_links"] = index_data.get("author_links", [])

    # Format display fields
    result["search_title"] = index_data.get("name", "Unknown Author")
    result["name"] = index_data.get("name", "Unknown Author")

    # Full overview is the bio
    if index_data.get("bio"):
        result["full_overview"] = index_data["bio"]
        result["overview"] = index_data["bio"]

    return result


async def _get_book_details(request: DetailsRequest, index_data: dict | None) -> dict[str, Any]:
    """
    Get detailed metadata for a book from OpenLibrary.

    Books are stored in the idx:book index with book: prefix.
    All the data is already in Redis from the OpenLibrary dump.
    """
    if not index_data:
        return {
            "error": f"Book not found: {request.mc_id}",
            "status_code": 404,
        }

    # Build result from index data
    result: dict[str, Any] = {**index_data}

    # Ensure proper identifiers
    result["id"] = request.mc_id
    result["mc_type"] = "book"

    # Format display fields
    result["search_title"] = index_data.get("title", "Unknown Title")
    result["name"] = index_data.get("title", "Unknown Title")

    # Use description as full_overview
    if index_data.get("description"):
        result["full_overview"] = index_data["description"]
        result["overview"] = index_data["description"]

    # Build cover image URLs if cover_i is available
    cover_i = index_data.get("cover_i")
    if cover_i:
        result["cover_urls"] = {
            "small": f"https://covers.openlibrary.org/b/id/{cover_i}-S.jpg",
            "medium": f"https://covers.openlibrary.org/b/id/{cover_i}-M.jpg",
            "large": f"https://covers.openlibrary.org/b/id/{cover_i}-L.jpg",
        }
        result["image"] = f"https://covers.openlibrary.org/b/id/{cover_i}-M.jpg"

    # Build OpenLibrary URL if openlibrary_key is available
    ol_key = index_data.get("openlibrary_key") or index_data.get("key")
    if ol_key:
        # Key format is /works/OL123W
        key_id = ol_key.replace("/works/", "")
        result["openlibrary_url"] = f"https://openlibrary.org/works/{key_id}"

    # Format subjects for display
    subjects = index_data.get("subjects", [])
    if subjects:
        result["subjects_display"] = subjects[:10]  # Limit to 10 for display

    return result


# ============================================================
# Cast Name Search Models and Functions
# ============================================================


class CastNameItem(BaseModel):
    """Model for a cast member's name parts."""

    first: str | None = None
    last: str | None = None
    character_first: str | None = None
    character_last: str | None = None


class CastNameSearchResponse(BaseModel):
    """Response model for cast name search endpoint."""

    title: str
    description: str | None
    cast_names: list[CastNameItem]


def _filter_name_by_length(name: str | None) -> str | None:
    """
    Return the name only if its length is between 3 and 7 characters (inclusive).

    Args:
        name: The name string to check

    Returns:
        The name if length is 3-7, otherwise None
    """
    if name is None:
        return None
    name = name.strip()
    if 3 <= len(name) <= 7:
        return name
    return None


def _split_name(full_name: str | None) -> tuple[str | None, str | None]:
    """
    Split a full name into first and last name parts.

    Handles common name patterns:
    - "John Smith" -> ("John", "Smith")
    - "John" -> ("John", None)
    - "John Paul Smith" -> ("John", "Smith") - first word as first, last word as last

    Args:
        full_name: The full name to split

    Returns:
        Tuple of (first_name, last_name), with length filtering applied
    """
    if not full_name:
        return (None, None)

    parts = full_name.strip().split()
    if len(parts) == 0:
        return (None, None)
    elif len(parts) == 1:
        return (_filter_name_by_length(parts[0]), None)
    else:
        # First word as first name, last word as last name
        first = _filter_name_by_length(parts[0])
        last = _filter_name_by_length(parts[-1])
        return (first, last)


def _process_cast_names(cast_list: list[dict[str, Any]]) -> list[CastNameItem]:
    """
    Process cast list and extract name parts with length filtering.

    Args:
        cast_list: List of cast member dictionaries with 'name' and 'character' fields

    Returns:
        List of CastNameItem with filtered name parts
    """
    result: list[CastNameItem] = []

    for cast_member in cast_list:
        actor_name = cast_member.get("name", "")
        character_name = cast_member.get("character", "")

        # Split actor name
        first, last = _split_name(actor_name)

        # Split character name
        char_first, char_last = _split_name(character_name)

        result.append(
            CastNameItem(
                first=first,
                last=last,
                character_first=char_first,
                character_last=char_last,
            )
        )

    return result


class CastNameSearchRequest(BaseModel):
    """Request model for cast name search."""

    query: str | None = None  # Text title to search for
    tmdb_id: int | None = None  # Optional direct TMDB ID
    media_type: str | None = None  # Optional: "movie" or "tv" (None = search both)


async def get_cast_names(request: CastNameSearchRequest) -> CastNameSearchResponse:
    """
    Get movie/TV show details with cast names split into first/last parts.

    Names are only included if they are between 3-7 characters in length.

    Flow:
    1. Search Redis index by title (both movie and TV unless media_type specified)
    2. Pick best match: exact title match first, then most popular
    3. Fetch full cast with character names from TMDB API

    Args:
        request: CastNameSearchRequest with query (text) or tmdb_id, and optional media_type

    Returns:
        CastNameSearchResponse with title, description, and cast_names
    """
    from core.search_queries import build_autocomplete_query

    tmdb_service = TMDBService()
    repo = get_repo()

    # Get TMDB ID - either from request or by searching Redis
    tmdb_id: int | None = request.tmdb_id
    mc_type_enum: MCType = MCType.MOVIE  # Default, will be set from search result
    title_from_index: str | None = None
    overview_from_index: str | None = None

    if tmdb_id is None and request.query:
        # Search Redis index for the title
        search_query = build_autocomplete_query(request.query)

        # Add media type filter if specified, otherwise search both
        if request.media_type:
            mc_type_filter = request.media_type.lower()
            search_query = f"({search_query}) @mc_type:{{{mc_type_filter}}}"
        else:
            # Search both movie and tv
            search_query = f"({search_query}) @mc_type:{{movie|tv}}"

        # Get multiple results to find best match
        search_results = await repo.search(search_query, limit=10)

        if not search_results.docs:
            return CastNameSearchResponse(
                title="Not Found",
                description=f"No results found for '{request.query}'",
                cast_names=[],
            )

        # Find best match: exact title match first, then most popular
        query_lower = request.query.lower().strip()
        best_match: dict[str, Any] | None = None
        exact_match: dict[str, Any] | None = None

        for doc in search_results.docs:
            parsed = parse_doc(doc)
            title = (parsed.get("search_title") or "").lower().strip()

            # Check for exact match
            if title == query_lower:
                exact_match = parsed
                break

        # Use exact match if found, otherwise first result (sorted by popularity)
        best_match = exact_match or parse_doc(search_results.docs[0])

        source_id = best_match.get("source_id")
        mc_type_str = best_match.get("mc_type", "movie")

        if not source_id:
            return CastNameSearchResponse(
                title="Error",
                description="Could not find source_id in search result",
                cast_names=[],
            )

        try:
            tmdb_id = int(source_id)
        except ValueError:
            return CastNameSearchResponse(
                title="Error",
                description=f"Invalid source_id: {source_id}",
                cast_names=[],
            )

        # Determine media type from the matched result
        if mc_type_str == "tv":
            mc_type_enum = MCType.TV_SERIES
        else:
            mc_type_enum = MCType.MOVIE

        # Get title and overview from Redis result
        title_from_index = best_match.get("search_title")
        overview_from_index = best_match.get("overview")

    elif tmdb_id is not None:
        # Direct TMDB ID lookup - need media_type
        if request.media_type and request.media_type.lower() == "tv":
            mc_type_enum = MCType.TV_SERIES
        else:
            mc_type_enum = MCType.MOVIE

    else:
        return CastNameSearchResponse(
            title="Error",
            description="Either 'query' or 'tmdb_id' must be provided",
            cast_names=[],
        )

    # Fetch full cast with character names from TMDB
    # (Redis only stores first 2 actor names without character info)
    detailed = await tmdb_service.get_media_details(
        tmdb_id=tmdb_id,
        media_type=mc_type_enum,
        include_cast=True,
        include_videos=False,
        include_watch_providers=False,
        include_keywords=False,
        cast_limit=50,
    )

    if detailed.error:
        return CastNameSearchResponse(
            title="Error",
            description=detailed.error,
            cast_names=[],
        )

    # Use title/overview from Redis if available, fallback to TMDB
    title = title_from_index or detailed.title or detailed.name or "Unknown"
    description = overview_from_index or detailed.overview

    # Get cast list from tmdb_cast
    cast_list = detailed.tmdb_cast.get("cast", []) if detailed.tmdb_cast else []

    # Process cast names
    cast_names = _process_cast_names(cast_list)

    return CastNameSearchResponse(
        title=title,
        description=description,
        cast_names=cast_names,
    )
