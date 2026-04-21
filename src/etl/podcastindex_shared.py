from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from typing import cast

from core.iptc import expand_keywords
from core.search_queries import build_media_query_from_user_input

AFTER_SHOWS_CATEGORY = "after-shows"
AFTER_SHOWS_TAG = "after_shows"
DEFAULT_MEDIA_FETCH_LIMIT = 50

_PODCAST_COLUMNS = """
    id, title, url, link, description,
    itunesAuthor, itunesOwnerName, imageUrl, language,
    category1, category2, category3, category4, category5,
    category6, category7, category8, category9, category10,
    episodeCount, popularityScore, itunesId, podcastGuid, lastUpdate
"""
_AFTER_SHOWS_CATEGORY_CLAUSE = " OR ".join(
    [f"LOWER(TRIM(category{i})) = '{AFTER_SHOWS_CATEGORY}'" for i in range(1, 11)]
)


def build_default_query(
    *, since_timestamp: int | None, min_popularity: int, limit: int | None = None
) -> tuple[str, list[int]]:
    """Build the default PodcastIndex query."""
    where_clauses = [
        "popularityScore >= ?",
        "language LIKE 'en%'",
        "episodeCount > 0",
        "imageUrl != ''",
        "imageUrl IS NOT NULL",
        "dead = 0",
    ]
    params: list[int] = [min_popularity]

    if since_timestamp is not None:
        where_clauses.insert(0, "lastUpdate >= ?")
        params.insert(0, since_timestamp)

    query = f"""
        SELECT
            {_PODCAST_COLUMNS}
        FROM podcasts
        WHERE
            {' AND '.join(where_clauses)}
        ORDER BY popularityScore DESC, episodeCount DESC
    """
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    return query, params


def build_after_shows_query(
    *, since_timestamp: int | None, limit: int | None = None
) -> tuple[str, list[int]]:
    """Build the After-Shows override query."""
    where_clauses = [
        f"({_AFTER_SHOWS_CATEGORY_CLAUSE})",
        "episodeCount > 0",
        "imageUrl != ''",
        "imageUrl IS NOT NULL",
        "dead = 0",
    ]
    params: list[int] = []

    if since_timestamp is not None:
        where_clauses.insert(0, "lastUpdate >= ?")
        params.append(since_timestamp)

    query = f"""
        SELECT
            {_PODCAST_COLUMNS}
        FROM podcasts
        WHERE
            {' AND '.join(where_clauses)}
        ORDER BY popularityScore DESC, episodeCount DESC
    """
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    return query, params


def build_categories_array(row: sqlite3.Row) -> list[str]:
    """Build a normalized and IPTC-expanded categories array."""
    raw_categories: list[str] = []
    row_keys = row.keys()
    for i in range(1, 11):
        category_key = f"category{i}"
        category_value = row[category_key] if category_key in row_keys else ""
        if category_value and str(category_value).strip():
            raw_categories.append(str(category_value).strip())

    if not raw_categories:
        return []

    keyword_dicts: list[dict[str, str]] = [{"name": category} for category in raw_categories]
    return cast(list[str], expand_keywords(keyword_dicts))


def has_after_shows_tag(categories: Sequence[str]) -> bool:
    """Return True when the normalized category list includes after_shows."""
    normalized = {str(category).strip().lower().replace("-", "_").replace(" ", "_") for category in categories}
    return AFTER_SHOWS_TAG in normalized


def has_after_shows_source_category(categories: Sequence[str]) -> bool:
    """Return True when raw source categories include after-shows."""
    normalized = {str(category).strip().lower() for category in categories}
    return AFTER_SHOWS_CATEGORY in normalized


def merge_rows_by_feed_id(*row_sets: Sequence[sqlite3.Row]) -> list[sqlite3.Row]:
    """Merge SQLite row sets preserving first-seen order by podcast feed id."""
    merged: list[sqlite3.Row] = []
    seen_feed_ids: set[int] = set()

    for rows in row_sets:
        for row in rows:
            feed_id = row["id"]
            if not isinstance(feed_id, int) or feed_id in seen_feed_ids:
                continue
            seen_feed_ids.add(feed_id)
            merged.append(row)

    return merged


def build_parent_media_candidate_query(title: str) -> str:
    """Build the media search query used for companion-podcast resolution."""
    return cast(str, build_media_query_from_user_input(title, raw=False))
