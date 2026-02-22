import asyncio
import json
import re
import time
from collections.abc import AsyncIterator
from typing import Any, Literal, Union

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
from core.iptc import expand_query_string, get_search_aliases
from core.ranking import (
    EXACT_MATCH_SOURCE_PRIORITY,
    is_exact_match,
    score_book_result,
    score_media_result,
    score_person_result,
    score_podcast_result,
)
from core.search_queries import (
    build_autocomplete_query,
    build_books_autocomplete_query,
    build_filter_query,
    build_fuzzy_fulltext_query,
    build_media_query_from_user_input,
    escape_redis_search_term,
    normalize_for_tag,
    strip_query_apostrophes,
)
from utils.get_logger import get_logger
from utils.soft_comparison import is_author_name_match, is_person_autocomplete_match

# Stream event: either (source, results, latency_ms) or ("exact_match", item)
StreamEvent = Union[
    tuple[str, list[Any], float],
    tuple[Literal["exact_match"], dict[str, Any]],
]

logger = get_logger(__name__)


def _rank_person_result(person: dict, query: str) -> tuple[int, int, float]:
    """
    Generate a sort key for ranking person results.
    Delegates to score_person_result from core.ranking.
    """
    result: tuple[int, int, float] = score_person_result(query, person)
    return result


def _rank_media_result(media: dict, query: str) -> tuple[int, int, float]:
    """
    Generate a sort key for ranking movie/TV results.
    Delegates to score_media_result from core.ranking.
    """
    result: tuple[int, int, float] = score_media_result(query, media)
    return result


def _rank_podcast_result(podcast: dict, query: str) -> tuple[int, float, int]:
    """
    Generate a sort key for ranking podcast results.
    Delegates to score_podcast_result from core.ranking.

    Uses tiered scoring based on match quality across title, author, and categories.
    Within each tier, sorted by popularity (desc), then episode_count (desc).
    """
    result: tuple[int, float, int] = score_podcast_result(query, podcast)
    return result


def _rank_book_result(book: dict, query: str) -> tuple[int, float, int]:
    """
    Generate a sort key for ranking book results.
    Delegates to score_book_result from core.ranking.

    Uses tiered scoring based on match quality across title, author, subjects, and description.
    Within each tier, sorted by popularity_score (desc), then work_id (asc, lower = older/more established).
    """
    result: tuple[int, float, int] = score_book_result(query, book)
    return result


def _iter_exact_matches(
    source: str, results: list[dict], query: str | None
) -> list[dict[str, Any]]:
    """Return items from results that are exact matches. Indexed sources only."""
    if not query or len(query.strip()) < 2:
        return []
    if source not in EXACT_MATCH_SOURCE_PRIORITY:
        return []
    exact_items: list[dict[str, Any]] = []
    for item in results:
        if is_exact_match(query.strip(), item, source):
            exact_items.append(_normalize_exact_match_cast(item))
    return exact_items


def _normalize_exact_match_cast(item: dict[str, Any]) -> dict[str, Any]:
    """
    Convert exact-match cast payload to a structured `{name, id}` format.

    This preserves all existing fields and only rewrites `cast` for exact-match items.
    """
    cast_names = item.get("cast")
    cast_ids = item.get("cast_ids")

    if not isinstance(cast_names, list):
        item["cast"] = []
        return item

    if not cast_ids:
        cast_ids = []
    elif not isinstance(cast_ids, list):
        cast_ids = [cast_ids]

    normalized_cast: list[dict[str, Any]] = []
    for index, cast_name in enumerate(cast_names):
        if not isinstance(cast_name, str) or not cast_name:
            continue

        cast_id = cast_ids[index] if index < len(cast_ids) else None
        normalized_cast.append({"name": cast_name, "id": str(cast_id) if cast_id else None})

    item["cast"] = normalized_cast
    return item


def _pick_exact_match(
    results: dict[str, list[dict[str, Any]]], query: str | None
) -> dict[str, Any] | None:
    """
    Pick the single best exact match from search results by cross-source priority.

    Returns the first exact match found when scanning sources in priority order
    (movie, tv, person, podcast, book, author). Returns None if no exact match.
    """
    if not query or len(query.strip()) < 2:
        return None
    q = query.strip()
    for source in EXACT_MATCH_SOURCE_PRIORITY:
        items = results.get(source) or []
        for item in items:
            if is_exact_match(q, item, source):
                return _normalize_exact_match_cast(item)
    return None


def _remove_exact_from_results(
    results: dict[str, Any], exact: dict[str, Any]
) -> None:
    """Remove the exact-match item from its source list (in-place).

    Uses object identity so only the specific dict returned by
    ``_pick_exact_match`` is removed.
    """
    for source in EXACT_MATCH_SOURCE_PRIORITY:
        items = results.get(source)
        if not items:
            continue
        filtered = [item for item in items if item is not exact]
        if len(filtered) < len(items):
            results[source] = filtered
            return


def _filter_exact_items(
    results: list[dict[str, Any]], exact_items: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Return *results* without any items present in *exact_items* (identity check)."""
    exact_ids = {id(item) for item in exact_items}
    return [item for item in results if id(item) not in exact_ids]


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

    # Use original title (with apostrophes) for display when available.
    # search_title is normalized (apostrophes stripped) for search indexing,
    # but the 'title' field preserves the original for display purposes.
    if "title" in result and result["title"]:
        result["search_title"] = result["title"]

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


def _extract_docs(result: object) -> list[object]:
    """Safely extract RediSearch docs from an arbitrary result object."""
    docs = getattr(result, "docs", None)
    if isinstance(docs, list):
        return docs
    return []


def build_people_autocomplete_query(q: str) -> str:
    """
    Build a prefix search query for people autocomplete.
    Searches both search_title (name) and also_known_as fields.

    Note: RediSearch requires minimum 2 characters for prefix search.
    Single-character last words are handled by searching complete words only
    and relying on post-query filtering.
    """
    # Strip apostrophes to match indexed names (e.g. "O'Brien" -> "OBrien")
    q = strip_query_apostrophes(q)

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

    # For multi-word names, match documents containing all words.
    # Previously this required the final two words to be contiguous in title text,
    # which breaks names with middle words (e.g. "Megan Therese Rippey").
    if len(escaped_words) == 1:
        # Single word - use as prefix (must be 2+ chars for RediSearch)
        if len(words[0]) >= 2:
            return f"(@search_title:{escaped_words[0]}*) | (@also_known_as:{escaped_words[0]}*)"
        else:
            # Single character - too short for prefix, return broad match
            return "*"
    # Multi-word query: each token must match in either field (in any position).
    # Build one clause per token and combine with AND semantics via spaces.
    query_terms: list[str] = []
    for word in escaped_words:
        if len(word) <= 3:
            token = f"{word}"
        else:
            token = f"{word}*"
        title_term = f"@search_title:({token})"
        aka_term = f"@also_known_as:({token})"
        query_terms.append(f"({title_term} | {aka_term})")

    return " ".join(query_terms)


def _build_text_query_for_variation(
    variation: str, stopwords: set[str]
) -> tuple[str | None, str | None, bool]:
    """
    Build title and author queries for a single query variation.

    Returns:
        Tuple of (title_query, author_query, skip_author)
    """
    parts = variation.replace(":", " : ").split()
    words = [w.lower() for w in parts if w and w != ":"]
    words = [w for w in words if w and w not in stopwords]

    if not words:
        return None, None, True

    escaped_words = [escape_redis_search_term(w) for w in words]
    query_len = len(escaped_words[0]) if len(escaped_words) == 1 else len(escaped_words[-1])
    skip_author = query_len < 3

    if len(escaped_words) == 1:
        word = escaped_words[0]
        if len(word) <= 3:
            title_query = f"@search_title:{word}"
        else:
            title_query = f"@search_title:{word}*"
        author_query = f"@author:{word}*" if not skip_author else None
    else:
        exact_words = " ".join(escaped_words[:-1])
        last_word = escaped_words[-1]
        if len(last_word) <= 3:
            title_query = f"@search_title:({exact_words} {last_word})"
        else:
            title_query = f"@search_title:({exact_words} {last_word}*)"
        author_query = f"@author:({exact_words} {last_word}*)" if not skip_author else None

    return title_query, author_query, skip_author


def build_podcasts_autocomplete_query(q: str, include_tag_fields: bool = True) -> str:
    """
    Build a prefix search query for podcasts autocomplete.

    Searches:
    - search_title (TEXT field with prefix matching)
    - author (TEXT field with prefix matching)
    - author_normalized (TAG field - matches podcast creator)
    - categories (TAG field - matches podcast categories)

    Supports query expansion for abbreviations (e.g., "NY Jets" -> also searches "New York Jets").
    """
    # Strip apostrophes to match indexed titles
    q = strip_query_apostrophes(q)

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

    # Get query variations (original + expanded)
    # e.g., "NY Jets" -> ["NY Jets", "new york Jets"]
    variations = expand_query_string(q)

    query_parts: list[str] = []
    any_skip_author = True  # Track if any variation allows author search

    # Build TEXT field queries for each variation
    for variation in variations:
        title_query, author_query, skip_author = _build_text_query_for_variation(
            variation, stopwords
        )
        if title_query:
            query_parts.append(f"({title_query})")
        if author_query:
            query_parts.append(f"({author_query})")
            any_skip_author = False
        elif not skip_author:
            any_skip_author = False

    if not query_parts:
        return "*"

    # Add TAG field queries for union search (use original query for normalization)
    if include_tag_fields:
        normalized_full = normalize_for_tag(q)
        use_prefix = len(normalized_full) > 3
        if normalized_full and len(normalized_full) >= 2:
            # Author normalized - skip for very short queries
            if not any_skip_author:
                tag_pattern = f"{normalized_full}*" if use_prefix else normalized_full
                query_parts.append(f"(@author_normalized:{{{tag_pattern}}})")
            # Categories - use IPTC alias expansion with EXACT matches
            category_aliases = get_search_aliases(normalized_full)
            category_union = "|".join(category_aliases)
            query_parts.append(f"(@categories:{{{category_union}}})")

    return " | ".join(query_parts)


def build_authors_autocomplete_query(q: str) -> str:
    """
    Build a search query for authors (OpenLibrary) autocomplete.
    Searches both search_title (name) and name fields.

    Uses exact word matching (no prefix wildcards) so that partial word
    matches are excluded.  For example, "tennis" matches "Jeni Tennis"
    but NOT "Jeni Tennison".
    """
    # Strip apostrophes to match indexed names
    q = strip_query_apostrophes(q)

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

    # Exact word matching — no wildcard suffix so "tennis" won't match "tennison"
    if len(escaped_words) == 1:
        return f"(@search_title:{escaped_words[0]}) | (@name:{escaped_words[0]})"
    else:
        all_words = " ".join(escaped_words)
        title_query = f"@search_title:({all_words})"
        name_query = f"@name:({all_words})"
        return f"({title_query}) | ({name_query})"


async def autocomplete(
    q: str,
    sources: set[str] | None = None,
    raw: bool = False,
    no_duplicate: bool = False,
) -> dict[str, Any]:
    """
    Autocomplete search that returns categorized results.

    Args:
        q: Search query string
        sources: Optional set of sources to search. If None, searches all sources.
                 Valid sources: tv, movie, person, podcast, author, book, news, video, ratings, artist, album
        raw: If True, treat q as raw RediSearch syntax for indexed sources (validated, raises on error)

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
            "exact_match": None,
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

    # Build queries - raw mode passes query through to all indexed sources
    media_query = build_media_query_from_user_input(q, raw=raw)
    if raw:
        raw_query = q.strip()
        people_query = raw_query
        podcasts_query = raw_query
        authors_query = raw_query
        books_query = raw_query
    else:
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
        # Build task list based on requested sources
        timed_tasks = []

        # RediSearch (local) - no timeout needed
        # "media" covers both tv and movie
        if "tv" in sources or "movie" in sources:
            timed_tasks.append(timed_task("media", repo.search(media_query, limit=50)))
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

        # External/brokered APIs (news, video, ratings, artist, album) are excluded
        # from autocomplete to avoid excessive API calls during debounce/typing.
        # They are only fetched on the /api/search endpoint (Enter key).

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
            media_res = await repo.search(media_query, limit=50)
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

        # Brokered APIs excluded from autocomplete fallback
        news_res = None
        video_res = None
        ratings_res = None
        artist_res = None
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

    # Re-rank movie and TV results: exact title matches first, then by popularity
    movie_results = sorted(movie_results, key=lambda m: _rank_media_result(m, q))
    tv_results = sorted(tv_results, key=lambda t: _rank_media_result(t, q))

    # Parse person results and filter using autocomplete prefix matching
    # This ensures "Rhea S" matches "Rhea Seehorn" even when RediSearch can't handle 1-char prefix
    person_results_raw = [parse_doc(doc) for doc in people_res.docs]  # type: ignore[union-attr]
    person_results_filtered = [
        p
        for p in person_results_raw
        if is_person_autocomplete_match(q, p.get("search_title", "") or p.get("name", ""))
    ]
    # Re-rank to prioritize exact matches and shorter names over pure popularity
    person_results = sorted(person_results_filtered, key=lambda p: _rank_person_result(p, q))

    # Parse podcast results and re-rank: exact title matches first, then by recency/episodes/popularity
    podcast_results_raw = [parse_doc(doc) for doc in podcasts_res.docs]  # type: ignore[union-attr]
    podcast_results = sorted(podcast_results_raw, key=lambda p: _rank_podcast_result(p, q))

    # Parse author results and filter to exact word matches only
    # (prevents "tennis" from matching "Jeni Tennison")
    author_results_raw = [parse_doc(doc) for doc in authors_res.docs]  # type: ignore[union-attr]
    author_results = [
        a
        for a in author_results_raw
        if is_author_name_match(q, a.get("search_title", "") or a.get("name", ""))
    ]

    # Parse book results and re-rank: exact matches first, then by popularity
    book_results_raw = [parse_doc(doc) for doc in books_res.docs]  # type: ignore[union-attr]
    book_results = sorted(book_results_raw, key=lambda b: _rank_book_result(b, q))

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

    result = {
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
    exact = _pick_exact_match(result, q)
    result["exact_match"] = exact
    if no_duplicate and exact is not None:
        _remove_exact_from_results(result, exact)
    return result


async def autocomplete_stream(
    q: str,
    sources: set[str] | None = None,
    raw: bool = False,
    no_duplicate: bool = False,
) -> AsyncIterator[StreamEvent]:
    """
    Streaming autocomplete that yields results as they become available.

    Uses asyncio.as_completed() to return fast sources first.
    Each yield contains: (source_name, results_list, latency_ms)

    Args:
        q: Search query string
        sources: Optional set of sources to search. If None, searches all sources.
                 Valid sources: tv, movie, person, podcast, author, book, news, video, ratings, artist, album
        raw: If True, treat q as raw RediSearch syntax for indexed sources (validated, raises on error)

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

    # Build queries - raw mode passes query through to all indexed sources
    media_query = build_media_query_from_user_input(q, raw=raw)
    if raw:
        raw_query = q.strip()
        people_query = raw_query
        podcasts_query = raw_query
        authors_query = raw_query
        books_query = raw_query
    else:
        people_query = build_people_autocomplete_query(q)
        podcasts_query = build_podcasts_autocomplete_query(q)
        authors_query = build_authors_autocomplete_query(q)
        books_query = build_books_autocomplete_query(q)

    # Create named tasks based on requested sources
    tasks_dict: dict[asyncio.Task, str] = {}  # type: ignore[type-arg]

    # RediSearch tasks (local) - no timeout
    # "media" covers both tv and movie
    if "tv" in sources or "movie" in sources:
        tasks_dict[asyncio.create_task(timed_task("media", repo.search(media_query, limit=50)))] = (
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

    # External/brokered APIs (news, video, ratings, artist, album) are excluded
    # from autocomplete stream to avoid excessive API calls during typing.
    # They are only fetched on the /api/search endpoint (Enter key).

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
                    # Re-rank: exact title matches first, then by popularity
                    tv_results = sorted(tv_results, key=lambda t: _rank_media_result(t, q))
                    movie_results = sorted(movie_results, key=lambda m: _rank_media_result(m, q))
                    # Yield TV and movie separately (only if requested)
                    if tv_results and "tv" in sources:
                        tv_top = tv_results[:10]
                        tv_exact = _iter_exact_matches("tv", tv_top, q)
                        if no_duplicate and tv_exact:
                            tv_top = _filter_exact_items(tv_top, tv_exact)
                        if tv_top:
                            yield ("tv", tv_top, elapsed)
                        for item in tv_exact:
                            yield ("exact_match", item)
                    if movie_results and "movie" in sources:
                        movie_top = movie_results[:10]
                        movie_exact = _iter_exact_matches("movie", movie_top, q)
                        if no_duplicate and movie_exact:
                            movie_top = _filter_exact_items(movie_top, movie_exact)
                        if movie_top:
                            yield ("movie", movie_top, elapsed)
                        for item in movie_exact:
                            yield ("exact_match", item)
                continue  # Already yielded tv/movie

            elif name == "person":
                # Person results need autocomplete prefix filtering and re-ranking
                if data and not isinstance(data, BaseException) and hasattr(data, "docs"):
                    parsed_all = [parse_doc(doc) for doc in data.docs]
                    filtered = [
                        p
                        for p in parsed_all
                        if is_person_autocomplete_match(q, p.get("search_title", "") or p.get("name", ""))
                    ]
                    # Re-rank to prioritize exact matches and shorter names
                    parsed_results = sorted(filtered, key=lambda p: _rank_person_result(p, q))[:10]

            elif name == "podcast":
                if data and not isinstance(data, BaseException) and hasattr(data, "docs"):
                    parsed_all = [parse_doc(doc) for doc in data.docs]
                    # Re-rank podcasts: title starts with query > contains query
                    parsed_results = sorted(parsed_all, key=lambda p: _rank_podcast_result(p, q))[
                        :10
                    ]

            elif name == "author":
                if data and not isinstance(data, BaseException) and hasattr(data, "docs"):
                    parsed_all = [parse_doc(doc) for doc in data.docs]
                    parsed_results = [
                        a
                        for a in parsed_all
                        if is_author_name_match(q, a.get("search_title", "") or a.get("name", ""))
                    ][:10]

            elif name == "book":
                if data and not isinstance(data, BaseException) and hasattr(data, "docs"):
                    parsed_all = [parse_doc(doc) for doc in data.docs]
                    # Re-rank books: exact matches first, then by popularity
                    parsed_results = sorted(parsed_all, key=lambda b: _rank_book_result(b, q))[:10]

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
                exact_items = _iter_exact_matches(name, parsed_results, q)
                if no_duplicate and exact_items:
                    parsed_results = _filter_exact_items(parsed_results, exact_items)
                if parsed_results:
                    yield (name, parsed_results, elapsed)
                for item in exact_items:
                    yield ("exact_match", item)

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
    raw: bool = False,
    no_duplicate: bool = False,
) -> dict[str, Any]:
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
    query_text = q if q is not None and len(q) >= 2 else None
    has_query = query_text is not None

    if not has_query and not has_filters:
        # Return empty dict with all requested source keys
        requested = sources if sources else VALID_SOURCES
        out: dict[str, Any] = {src: [] for src in requested}
        out["exact_match"] = None
        return out

    # Default to all sources if none specified
    requested_sources = sources if sources else VALID_SOURCES
    # Filter to only valid sources
    requested_sources = requested_sources & VALID_SOURCES

    if not requested_sources:
        empty: dict[str, Any] = {"exact_match": None}
        return empty

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
            raw=raw,
        )
    elif query_text is not None:
        media_query = build_media_query_from_user_input(query_text, raw=raw)
    else:
        media_query = "*"

    # For other indices, build query from text or pass raw through
    if query_text is not None:
        if raw:
            raw_query = query_text.strip()
            people_query = raw_query
            podcasts_query = raw_query
            authors_query = raw_query
            books_query = raw_query
        else:
            people_query = build_people_autocomplete_query(query_text)
            podcasts_query = build_podcasts_autocomplete_query(query_text)
            authors_query = build_authors_autocomplete_query(query_text)
            books_query = build_books_autocomplete_query(query_text)
    else:
        # Filter-only mode: use match-all for indices without specific filters
        people_query = "*"
        podcasts_query = "*"
        authors_query = "*"
        books_query = "*"

    # Create empty result placeholder for indexed searches
    empty_result = type("obj", (object,), {"docs": []})()

    # Build task list based on requested sources (wrapped with timing)
    # External API timeout - YouTube/Spotify/RT are single-call APIs (~1-2s)
    api_timeout = 2.5
    # News needs more time: concept URI resolution (~1-2s on cache miss) + article search (~2s)
    news_api_timeout = 6.0
    timed_tasks: list[Any] = []

    # Indexed sources (RediSearch) - no timeout needed
    # Fetch limit * 5 for media to ensure exact title matches aren't pushed out
    # by higher-popularity keyword/cast matches in the union query.
    # Python re-ranking (score_media_result) handles final ordering.
    media_fetch_limit = max(limit * 5, 50) if has_query else limit * 2
    if "tv" in requested_sources or "movie" in requested_sources:
        timed_tasks.append(timed_task("media", repo.search(media_query, limit=media_fetch_limit)))
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
    # Other sources require a text query - skip if no query provided or raw mode
    if query_text is not None and not raw:
        if "news" in requested_sources:
            timed_tasks.append(
                timed_task(
                    "news",
                    newsai_wrapper.search_news(query=query_text, page_size=limit),
                    news_api_timeout,
                )
            )
        if "video" in requested_sources:
            timed_tasks.append(
                timed_task(
                    "video",
                    youtube_wrapper.search_videos(query=query_text, max_results=limit),
                    api_timeout,
                )
            )
        if "ratings" in requested_sources:
            timed_tasks.append(
                timed_task(
                    "ratings",
                    rottentomatoes_wrapper.search_content(query=query_text, limit=limit),
                    api_timeout,
                )
            )
        if "artist" in requested_sources:
            timed_tasks.append(
                timed_task(
                    "artist",
                    spotify_wrapper.search_artists(query=query_text, limit=limit),
                    api_timeout,
                )
            )
        if "album" in requested_sources:
            timed_tasks.append(
                timed_task(
                    "album",
                    spotify_wrapper.search_albums(query=query_text, limit=limit),
                    api_timeout,
                )
            )

    # Execute all tasks concurrently
    total_start = time.perf_counter()
    try:
        timed_results = await asyncio.gather(*timed_tasks, return_exceptions=True)
    except Exception:
        # If gather itself fails, return empty results
        fail_out: dict[str, Any] = {src: [] for src in requested_sources}
        fail_out["exact_match"] = None
        return fail_out
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
    logger.info(
        f"Search {query_desc} latency: total={total_elapsed:.0f}ms | {' | '.join(timing_parts)}"
    )

    # Handle exceptions and parse results
    final_results: dict[str, Any] = {}

    # Process media (tv/movie) results
    if "media" in results_map:
        media_res = results_map["media"]
        if isinstance(media_res, BaseException):
            media_res = empty_result

        tv_results: list[dict] = []
        movie_results: list[dict] = []

        for doc in _extract_docs(media_res):
            parsed = parse_doc(doc)
            mc_type = parsed.get("mc_type", "")
            if mc_type == "tv" and "tv" in requested_sources:
                tv_results.append(parsed)
            elif mc_type == "movie" and "movie" in requested_sources:
                movie_results.append(parsed)

        # Re-rank: exact title matches first, then by popularity
        if q:
            tv_results = sorted(tv_results, key=lambda t: _rank_media_result(t, q))
            movie_results = sorted(movie_results, key=lambda m: _rank_media_result(m, q))

        if "tv" in requested_sources:
            final_results["tv"] = tv_results[:limit]
        if "movie" in requested_sources:
            final_results["movie"] = movie_results[:limit]

    # Process person results with autocomplete prefix filtering and re-ranking
    if "person" in results_map:
        person_res = results_map["person"]
        if isinstance(person_res, BaseException):
            person_res = empty_result
        parsed_people = [parse_doc(doc) for doc in _extract_docs(person_res)]

        if has_query and q:
            # Filter using autocomplete prefix matching (handles 1-char prefix case)
            filtered_people = [
                p
                for p in parsed_people
                if is_person_autocomplete_match(q, p.get("search_title", "") or p.get("name", ""))
            ]
            # Re-rank to prioritize exact matches and shorter names over pure popularity
            final_results["person"] = sorted(
                filtered_people, key=lambda p: _rank_person_result(p, q)
            )[:limit]
        else:
            # Filter-only mode: return results as-is (sorted by popularity from Redis)
            final_results["person"] = parsed_people[:limit]

    # Process podcast results with re-ranking when query is present
    if "podcast" in results_map:
        podcast_res = results_map["podcast"]
        if isinstance(podcast_res, BaseException):
            podcast_res = empty_result
        parsed_podcasts = [parse_doc(doc) for doc in _extract_docs(podcast_res)]
        # Re-rank: exact title matches first, then by recency/episodes/popularity
        if q:
            parsed_podcasts = sorted(parsed_podcasts, key=lambda p: _rank_podcast_result(p, q))
        final_results["podcast"] = parsed_podcasts[:limit]

    # Process author results with exact word matching filter
    if "author" in results_map:
        author_res = results_map["author"]
        if isinstance(author_res, BaseException):
            author_res = empty_result
        parsed_authors = [parse_doc(doc) for doc in _extract_docs(author_res)]
        if has_query and q:
            parsed_authors = [
                a
                for a in parsed_authors
                if is_author_name_match(q, a.get("search_title", "") or a.get("name", ""))
            ]
        final_results["author"] = parsed_authors[:limit]

    # Process book results
    if "book" in results_map:
        book_res = results_map["book"]
        if isinstance(book_res, BaseException):
            book_res = empty_result
        parsed_books = [parse_doc(doc) for doc in _extract_docs(book_res)]
        # Re-rank: exact matches first, then by popularity
        if q:
            parsed_books = sorted(parsed_books, key=lambda b: _rank_book_result(b, q))
        final_results["book"] = parsed_books[:limit]

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
    elif (
        not has_query
        and "ratings" in requested_sources
        and ("tv" in requested_sources or "movie" in requested_sources)
    ):
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
            titles_to_enrich = indexed_titles[: limit * 2]
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
    if (
        ratings_results
        and "ratings" in requested_sources
        and ("tv" in requested_sources or "movie" in requested_sources)
    ):
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

    # Add exact_match when query is present (single best match by source priority)
    exact = _pick_exact_match(final_results, q if has_query else None)
    final_results["exact_match"] = exact
    if no_duplicate and exact is not None:
        _remove_exact_from_results(final_results, exact)

    return final_results


async def search_stream(
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
    raw: bool = False,
    no_duplicate: bool = False,
) -> AsyncIterator[StreamEvent]:
    """
    Streaming search that yields categorized results as each source completes.

    Uses asyncio.as_completed() so fast sources (local RediSearch) return
    immediately while slower brokered APIs (news, video, etc.) stream in later.

    Args:
        Same as search().

    Yields:
        tuple of (source_name, results_list, latency_ms) as each source completes.
        source_name may be "tv", "movie", "person", etc.
        Media index yields "tv" and "movie" separately from a single "media" task.
    """
    has_filters = any([genre_ids, cast_ids, year_min, year_max, rating_min, rating_max, mc_type])
    query_text = q if q is not None and len(q) >= 2 else None
    has_query = query_text is not None

    if not has_query and not has_filters:
        return

    requested_sources = sources if sources else VALID_SOURCES
    requested_sources = requested_sources & VALID_SOURCES
    if not requested_sources:
        return

    repo = get_repo()

    # Build queries (same logic as search())
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
            raw=raw,
        )
    elif query_text is not None:
        media_query = build_media_query_from_user_input(query_text, raw=raw)
    else:
        media_query = "*"

    if query_text is not None:
        if raw:
            raw_query = query_text.strip()
            people_query = raw_query
            podcasts_query = raw_query
            authors_query = raw_query
            books_query = raw_query
        else:
            people_query = build_people_autocomplete_query(query_text)
            podcasts_query = build_podcasts_autocomplete_query(query_text)
            authors_query = build_authors_autocomplete_query(query_text)
            books_query = build_books_autocomplete_query(query_text)
    else:
        people_query = "*"
        podcasts_query = "*"
        authors_query = "*"
        books_query = "*"

    api_timeout = 2.5
    news_api_timeout = 6.0  # News: concept resolution + article search

    # Create named tasks
    tasks_dict: dict[asyncio.Task, str] = {}  # type: ignore[type-arg]

    # Indexed sources (RediSearch) - no timeout
    # Fetch more media results for text queries to ensure exact title matches
    # aren't pushed out by popularity-sorted keyword/cast/genre matches.
    media_fetch_limit = max(limit * 5, 50) if has_query else limit * 2
    if "tv" in requested_sources or "movie" in requested_sources:
        tasks_dict[
            asyncio.create_task(
                timed_task("media", repo.search(media_query, limit=media_fetch_limit))
            )
        ] = "media"
    if "person" in requested_sources:
        tasks_dict[
            asyncio.create_task(
                timed_task("person", repo.search_people(people_query, limit=limit * 2))
            )
        ] = "person"
    if "podcast" in requested_sources:
        tasks_dict[
            asyncio.create_task(
                timed_task("podcast", repo.search_podcasts(podcasts_query, limit=limit))
            )
        ] = "podcast"
    if "author" in requested_sources:
        tasks_dict[
            asyncio.create_task(
                timed_task("author", repo.search_authors(authors_query, limit=limit))
            )
        ] = "author"
    if "book" in requested_sources:
        tasks_dict[
            asyncio.create_task(timed_task("book", repo.search_books(books_query, limit=limit)))
        ] = "book"

    # Brokered sources - apply timeout
    if query_text is not None and not raw:
        if "news" in requested_sources:
            tasks_dict[
                asyncio.create_task(
                    timed_task(
                        "news",
                        newsai_wrapper.search_news(query=query_text, page_size=limit),
                        news_api_timeout,
                    )
                )
            ] = "news"
        if "video" in requested_sources:
            tasks_dict[
                asyncio.create_task(
                    timed_task(
                        "video",
                        youtube_wrapper.search_videos(query=query_text, max_results=limit),
                        api_timeout,
                    )
                )
            ] = "video"
        if "ratings" in requested_sources:
            tasks_dict[
                asyncio.create_task(
                    timed_task(
                        "ratings",
                        rottentomatoes_wrapper.search_content(query=query_text, limit=limit),
                        api_timeout,
                    )
                )
            ] = "ratings"
        if "artist" in requested_sources:
            tasks_dict[
                asyncio.create_task(
                    timed_task(
                        "artist",
                        spotify_wrapper.search_artists(query=query_text, limit=limit),
                        api_timeout,
                    )
                )
            ] = "artist"
        if "album" in requested_sources:
            tasks_dict[
                asyncio.create_task(
                    timed_task(
                        "album",
                        spotify_wrapper.search_albums(query=query_text, limit=limit),
                        api_timeout,
                    )
                )
            ] = "album"

    if not tasks_dict:
        return

    total_start = time.perf_counter()
    timing_parts: list[str] = []

    for completed_task in asyncio.as_completed(tasks_dict.keys()):
        try:
            result = await completed_task
            if not isinstance(result, tuple) or len(result) != 3:
                continue

            name, data, elapsed = result
            timing_parts.append(f"{name}={elapsed:.0f}ms")

            if name == "media":
                if data and not isinstance(data, BaseException) and hasattr(data, "docs"):
                    tv_results: list[dict] = []
                    movie_results: list[dict] = []
                    for doc in data.docs:
                        parsed = parse_doc(doc)
                        parsed_mc_type = parsed.get("mc_type", "")
                        if parsed_mc_type == "tv" and "tv" in requested_sources:
                            tv_results.append(parsed)
                        elif parsed_mc_type == "movie" and "movie" in requested_sources:
                            movie_results.append(parsed)
                    if q:
                        tv_results = sorted(tv_results, key=lambda t: _rank_media_result(t, q))
                        movie_results = sorted(
                            movie_results, key=lambda m: _rank_media_result(m, q)
                        )
                    if tv_results and "tv" in requested_sources:
                        tv_top = tv_results[:limit]
                        tv_exact = _iter_exact_matches("tv", tv_top, q)
                        if no_duplicate and tv_exact:
                            tv_top = _filter_exact_items(tv_top, tv_exact)
                        if tv_top:
                            yield ("tv", tv_top, elapsed)
                        for item in tv_exact:
                            yield ("exact_match", item)
                    if movie_results and "movie" in requested_sources:
                        movie_top = movie_results[:limit]
                        movie_exact = _iter_exact_matches("movie", movie_top, q)
                        if no_duplicate and movie_exact:
                            movie_top = _filter_exact_items(movie_top, movie_exact)
                        if movie_top:
                            yield ("movie", movie_top, elapsed)
                        for item in movie_exact:
                            yield ("exact_match", item)
                continue

            elif name == "person":
                if data and not isinstance(data, BaseException) and hasattr(data, "docs"):
                    parsed_all = [parse_doc(doc) for doc in data.docs]
                    if has_query and q:
                        filtered = [
                            p
                            for p in parsed_all
                            if is_person_autocomplete_match(
                                q, p.get("search_title", "") or p.get("name", "")
                            )
                        ]
                        parsed_results = sorted(filtered, key=lambda p: _rank_person_result(p, q))[
                            :limit
                        ]
                    else:
                        parsed_results = parsed_all[:limit]

            elif name == "podcast":
                if data and not isinstance(data, BaseException) and hasattr(data, "docs"):
                    parsed_all = [parse_doc(doc) for doc in data.docs]
                    if q:
                        parsed_results = sorted(
                            parsed_all, key=lambda p: _rank_podcast_result(p, q)
                        )[:limit]
                    else:
                        parsed_results = parsed_all[:limit]
                else:
                    parsed_results = []

            elif name == "author":
                if data and not isinstance(data, BaseException) and hasattr(data, "docs"):
                    parsed_all = [parse_doc(doc) for doc in data.docs]
                    if has_query and q:
                        parsed_results = [
                            a
                            for a in parsed_all
                            if is_author_name_match(
                                q, a.get("search_title", "") or a.get("name", "")
                            )
                        ][:limit]
                    else:
                        parsed_results = parsed_all[:limit]
                else:
                    parsed_results = []

            elif name == "book":
                if data and not isinstance(data, BaseException) and hasattr(data, "docs"):
                    parsed_all = [parse_doc(doc) for doc in data.docs]
                    if q:
                        parsed_results = sorted(parsed_all, key=lambda b: _rank_book_result(b, q))[
                            :limit
                        ]
                    else:
                        parsed_results = parsed_all[:limit]
                else:
                    parsed_results = []

            elif name in ("news", "video", "artist", "album"):
                parsed_results = []
                if (
                    data
                    and not isinstance(data, BaseException)
                    and hasattr(data, "status_code")
                    and data.status_code == 200
                    and hasattr(data, "results")
                    and data.results
                ):
                    parsed_results = [item.model_dump() for item in data.results[:limit]]

            elif name == "ratings":
                parsed_results = []
                if (
                    data
                    and not isinstance(data, BaseException)
                    and hasattr(data, "status_code")
                    and data.status_code == 200
                    and hasattr(data, "results")
                    and data.results
                ):
                    parsed_results = [item.model_dump() for item in data.results[:limit]]
                    if ratings_sort == "audience_score":
                        parsed_results.sort(
                            key=lambda x: int(x.get("audience_score") or -1), reverse=True
                        )
                    elif ratings_sort == "critics_score":
                        parsed_results.sort(
                            key=lambda x: int(x.get("critics_score") or -1), reverse=True
                        )
                    else:
                        parsed_results.sort(
                            key=lambda x: int(x.get("popularity") or -1), reverse=True
                        )

            else:
                parsed_results = []

            if parsed_results:
                exact_items = _iter_exact_matches(name, parsed_results, q)
                if no_duplicate and exact_items:
                    parsed_results = _filter_exact_items(parsed_results, exact_items)
                if parsed_results:
                    yield (name, parsed_results, elapsed)
                for item in exact_items:
                    yield ("exact_match", item)

        except Exception as e:
            logger.warning(f"Error processing streaming search result: {e}")
            continue

    total_elapsed = (time.perf_counter() - total_start) * 1000
    query_desc = f"'{q}'" if q else "[filters only]"
    logger.info(
        f"Search stream {query_desc} latency: total={total_elapsed:.0f}ms | "
        f"{' | '.join(timing_parts)}"
    )


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
        media_res_obj, people_res_obj = await asyncio.gather(
            repo.search(media_query, limit=50),
            repo.search_people(people_query, limit=10),
            return_exceptions=True,
        )
    except Exception:
        media_res_obj = None
        people_res_obj = None

    # Parse and categorize media results
    tv_results = []
    movie_results = []

    for doc in _extract_docs(media_res_obj):
        parsed = parse_doc(doc)
        mc_type = parsed.get("mc_type", "")
        if mc_type == "tv":
            tv_results.append(parsed)
        elif mc_type == "movie":
            movie_results.append(parsed)

    # Re-rank: exact title matches first, then by popularity
    tv_results = sorted(tv_results, key=lambda t: _rank_media_result(t, q))
    movie_results = sorted(movie_results, key=lambda m: _rank_media_result(m, q))

    # Parse person results
    person_results = [parse_doc(doc) for doc in _extract_docs(people_res_obj)]

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
    rss_details: bool = False  # For podcasts: fetch and parse RSS feed episodes


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

    If request.rss_details is True, also fetches and parses the RSS feed
    to include episode data in the response.
    """
    from api.podcast.rss_parser import parse_rss_feed
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

        # If rss_details requested, fetch and parse the RSS feed
        if request.rss_details and podcast_data.get("url"):
            feed_url = podcast_data["url"]
            logger.info(f"Fetching RSS feed for podcast {feed_id}: {feed_url}")
            try:
                rss_result = await parse_rss_feed(feed_url, max_episodes=25)
                # Add RSS data to response
                podcast_data["rss_episodes"] = [ep.model_dump() for ep in rss_result.episodes]
                podcast_data["rss_total_episodes"] = rss_result.total_episodes
                podcast_data["rss_feed_title"] = rss_result.feed_title
                podcast_data["rss_feed_description"] = rss_result.feed_description
                if rss_result.error:
                    podcast_data["rss_error"] = rss_result.error
            except Exception as rss_error:
                logger.warning(f"Failed to parse RSS feed for podcast {feed_id}: {rss_error}")
                podcast_data["rss_episodes"] = []
                podcast_data["rss_error"] = str(rss_error)

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

        search_docs = _extract_docs(search_results)
        if not search_docs:
            return CastNameSearchResponse(
                title="Not Found",
                description=f"No results found for '{request.query}'",
                cast_names=[],
            )

        # Find best match: exact title match first, then most popular
        query_lower = request.query.lower().strip()
        best_match: dict[str, Any] | None = None
        exact_match: dict[str, Any] | None = None

        for doc in search_docs:
            parsed = parse_doc(doc)
            title = (parsed.get("search_title") or "").lower().strip()

            # Check for exact match
            if title == query_lower:
                exact_match = parsed
                break

        # Use exact match if found, otherwise first result (sorted by popularity)
        best_match = exact_match or parse_doc(search_docs[0])

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
