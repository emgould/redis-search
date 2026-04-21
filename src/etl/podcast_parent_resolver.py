from __future__ import annotations

import asyncio
import re

from adapters.redis_repository import RedisRepository
from core.exact_matches import (
    build_media_source_query,
    collect_exact_matches,
    extract_redis_search_docs,
    parse_redis_search_doc,
)
from core.ranking import score_media_result
from etl.podcastindex_shared import DEFAULT_MEDIA_FETCH_LIMIT, build_parent_media_candidate_query

_TRAILING_BRACKETS_PATTERN = re.compile(r"\s*[\(\[].*?[\)\]]\s*$")
_LEADING_PROVIDER_PATTERN = re.compile(
    r"^(?:hbo|max|netflix|disney|hulu|prime video|amazon prime video|apple tv)\W+s\s+",
    re.IGNORECASE,
)
_LEADING_ARTICLE_PATTERN = re.compile(r"^(?:the)\s+", re.IGNORECASE)
_TRAILING_SUFFIX_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\s+official podcast$", re.IGNORECASE),
    re.compile(r"\s+podcast$", re.IGNORECASE),
    re.compile(r"\s+after[\s-]?show$", re.IGNORECASE),
    re.compile(r"\s+recap$", re.IGNORECASE),
    re.compile(r"\s+companion podcast$", re.IGNORECASE),
    re.compile(r"\s+rewatch$", re.IGNORECASE),
)
_WHITESPACE_PATTERN = re.compile(r"\s+")


def _normalize_candidate_title(value: str) -> str:
    trimmed = _WHITESPACE_PATTERN.sub(" ", value).strip(" -:|")
    return trimmed.strip()


def _append_candidate(candidates: list[str], seen: set[str], value: str) -> None:
    normalized = _normalize_candidate_title(value)
    normalized_key = normalized.casefold()
    if len(normalized) < 2 or normalized_key in seen:
        return
    seen.add(normalized_key)
    candidates.append(normalized)


def clean_podcast_title(raw_title: str) -> list[str]:
    """Generate deterministic media-title candidates from a podcast title."""
    base_title = _normalize_candidate_title(raw_title)
    if not base_title:
        return []

    candidates: list[str] = []
    seen: set[str] = set()
    _append_candidate(candidates, seen, base_title)

    without_brackets = _normalize_candidate_title(_TRAILING_BRACKETS_PATTERN.sub("", base_title))
    _append_candidate(candidates, seen, without_brackets)

    queue = [without_brackets or base_title]
    processed: set[str] = set()
    while queue:
        current = queue.pop(0)
        current_key = current.casefold()
        if current_key in processed:
            continue
        processed.add(current_key)

        stripped_provider = _normalize_candidate_title(_LEADING_PROVIDER_PATTERN.sub("", current))
        if stripped_provider and stripped_provider.casefold() != current_key:
            _append_candidate(candidates, seen, stripped_provider)
            queue.append(stripped_provider)

        stripped_article = _normalize_candidate_title(_LEADING_ARTICLE_PATTERN.sub("", current))
        if stripped_article and stripped_article.casefold() != current_key:
            _append_candidate(candidates, seen, stripped_article)
            queue.append(stripped_article)

        for pattern in _TRAILING_SUFFIX_PATTERNS:
            stripped_suffix = _normalize_candidate_title(pattern.sub("", current))
            if stripped_suffix and stripped_suffix.casefold() != current_key:
                _append_candidate(candidates, seen, stripped_suffix)
                queue.append(stripped_suffix)

    return candidates


def _parse_media_results(result: object, query: str) -> list[dict[str, object]]:
    parsed_results = [parse_redis_search_doc(doc) for doc in extract_redis_search_docs(result)]
    return sorted(parsed_results, key=lambda item: score_media_result(query, item))


async def resolve_parent_mc_ids(repo: RedisRepository, raw_title: str) -> list[str]:
    """Resolve canonical media mc_ids for a companion podcast title."""
    resolved_ids: list[str] = []
    seen_ids: set[str] = set()

    for candidate in clean_podcast_title(raw_title):
        base_query = build_parent_media_candidate_query(candidate)
        movie_query = build_media_source_query(base_query, "movie")
        tv_query = build_media_source_query(base_query, "tv")
        movie_result, tv_result = await asyncio.gather(
            repo.search(movie_query, limit=DEFAULT_MEDIA_FETCH_LIMIT, sort_by="popularity"),
            repo.search(tv_query, limit=DEFAULT_MEDIA_FETCH_LIMIT, sort_by="popularity"),
        )

        exact_matches = collect_exact_matches(
            {
                "movie": _parse_media_results(movie_result, candidate),
                "tv": _parse_media_results(tv_result, candidate),
            },
            candidate,
        )

        for match in exact_matches:
            mc_id = match.get("mc_id")
            if not isinstance(mc_id, str) or mc_id in seen_ids:
                continue
            seen_ids.add(mc_id)
            resolved_ids.append(mc_id)

    return resolved_ids
