from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta

from core.ranking import EXACT_MATCH_SOURCE_PRIORITY, is_exact_match

_THEATRICAL_WINDOW_DAYS = 183
_SOURCE_PRIORITY_INDEX: dict[str, int] = {
    source: idx for idx, source in enumerate(EXACT_MATCH_SOURCE_PRIORITY)
}


def parse_redis_search_doc(doc: object) -> dict[str, object]:
    """Parse a Redis Search document into a plain dict."""
    result: dict[str, object] = {}

    doc_id = getattr(doc, "id", None)
    if isinstance(doc_id, str):
        result["id"] = doc_id

    doc_json = getattr(doc, "json", None)
    if isinstance(doc_json, str) and doc_json:
        try:
            parsed = json.loads(doc_json)
        except (json.JSONDecodeError, TypeError):
            parsed = None
        if isinstance(parsed, dict):
            for key, value in parsed.items():
                if isinstance(key, str):
                    result[key] = value

    if "mc_id" not in result:
        fallback_id = result.get("id")
        if isinstance(fallback_id, str):
            result["mc_id"] = fallback_id

    doc_dict = getattr(doc, "__dict__", None)
    if isinstance(doc_dict, dict):
        for key, value in doc_dict.items():
            if key not in {"id", "payload", "json"} and value is not None:
                result[key] = value

    title = result.get("title")
    if isinstance(title, str) and title:
        result["search_title"] = title

    raw_id = result.get("id")
    mc_type = result.get("mc_type")
    mc_subtype = result.get("mc_subtype")
    if (
        isinstance(raw_id, str)
        and mc_type == "person"
        and mc_subtype != "author"
        and raw_id.startswith("person_")
        and not raw_id.startswith("tmdb_")
    ):
        fixed_id = f"tmdb_{raw_id}"
        result["id"] = fixed_id
        raw_id = fixed_id

    if (
        isinstance(raw_id, str)
        and mc_type == "person"
        and mc_subtype != "author"
        and not result.get("source_id")
    ):
        match = re.search(r"_(\d+)$", raw_id)
        if match:
            result["source_id"] = match.group(1)

    return result


def extract_redis_search_docs(result: object) -> list[object]:
    """Safely extract RediSearch docs from an arbitrary result object."""
    docs = getattr(result, "docs", None)
    if isinstance(docs, list):
        return docs
    return []


def build_media_source_query(query_str: str, media_source: str) -> str:
    """Restrict a media query to a specific source bucket."""
    type_filter = f"@mc_type:{{{media_source}}}"
    if query_str == "*":
        return type_filter
    return f"({query_str}) {type_filter}"


def normalize_exact_match_cast(item: dict[str, object]) -> dict[str, object]:
    """Convert exact-match cast payload to structured {name, id} dicts."""
    cast_names = item.get("cast")
    cast_ids = item.get("cast_ids")

    if not isinstance(cast_names, list):
        item["cast"] = []
        return item

    if not isinstance(cast_ids, list):
        cast_ids = []

    normalized_cast: list[dict[str, str | None]] = []
    for index, cast_name in enumerate(cast_names):
        if isinstance(cast_name, dict):
            name = cast_name.get("name")
            cast_id = cast_name.get("id")
            if isinstance(name, str) and name:
                normalized_cast.append(
                    {
                        "name": name,
                        "id": str(cast_id) if cast_id is not None else None,
                    }
                )
            continue

        if not isinstance(cast_name, str) or not cast_name:
            continue

        cast_id = cast_ids[index] if index < len(cast_ids) else None
        normalized_cast.append(
            {
                "name": cast_name,
                "id": str(cast_id) if cast_id is not None else None,
            }
        )

    item["cast"] = normalized_cast
    return item


def iter_exact_matches(
    source: str, results: list[dict[str, object]], query: str | None
) -> list[dict[str, object]]:
    """Return items from results that are exact matches."""
    if not query or len(query.strip()) < 2:
        return []
    if source not in EXACT_MATCH_SOURCE_PRIORITY:
        return []

    exact_items: list[dict[str, object]] = []
    normalized_query = query.strip()
    for item in results:
        if is_exact_match(normalized_query, item, source):
            exact_items.append(normalize_exact_match_cast(item))
    return exact_items


def has_watch_providers(item: dict[str, object]) -> bool:
    """Return True if the item has a real streaming or on-demand provider."""
    watch_providers = item.get("watch_providers")
    if not isinstance(watch_providers, dict):
        return False
    if watch_providers.get("streaming_platform_ids"):
        return True
    if watch_providers.get("on_demand_platform_ids"):
        return True
    return bool(watch_providers.get("primary_provider"))


def in_theatrical_window(item: dict[str, object]) -> bool:
    """Return True for movies released within the last ~6 months."""
    if item.get("mc_type") != "movie":
        return False

    raw_release_date = item.get("release_date")
    if not isinstance(raw_release_date, str) or not raw_release_date:
        return False

    try:
        release_date = datetime.fromisoformat(raw_release_date.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return False

    if release_date.tzinfo is None:
        release_date = release_date.replace(tzinfo=UTC)

    cutoff = datetime.now(tz=UTC) - timedelta(days=_THEATRICAL_WINDOW_DAYS)
    return release_date >= cutoff


def effective_date_yyyymm(source: str, item: dict[str, object]) -> int:
    """Return YYYYMM int from the effective date for exact-match sorting."""
    raw_date = item.get("release_date") if source == "movie" else item.get("last_air_date")
    if not isinstance(raw_date, str) or not raw_date:
        return 0

    try:
        parsed_date = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return 0

    return parsed_date.year * 100 + parsed_date.month


def exact_match_media_sort_key(candidate: tuple[str, dict[str, object]]) -> tuple[int, int, float, int]:
    """Sort key for media exact-match candidates."""
    source, item = candidate
    viable = 0 if (has_watch_providers(item) or in_theatrical_window(item)) else 1
    yyyymm = effective_date_yyyymm(source, item)
    popularity_value = item.get("popularity")
    popularity = float(popularity_value) if isinstance(popularity_value, int | float) else 0.0
    return (
        viable,
        -yyyymm,
        -popularity,
        _SOURCE_PRIORITY_INDEX.get(source, len(EXACT_MATCH_SOURCE_PRIORITY)),
    )


def pick_exact_match(
    results: dict[str, list[dict[str, object]]], query: str | None
) -> dict[str, object] | None:
    """Pick the single best exact match from search results."""
    if not query or len(query.strip()) < 2:
        return None

    normalized_query = query.strip()
    media_exact_candidates: list[tuple[str, dict[str, object]]] = []
    for source in ("movie", "tv"):
        items = results.get(source) or []
        for item in items:
            if is_exact_match(normalized_query, item, source):
                media_exact_candidates.append((source, item))

    if media_exact_candidates:
        if len(media_exact_candidates) > 1:
            viable = [
                (source, item)
                for source, item in media_exact_candidates
                if has_watch_providers(item) or in_theatrical_window(item)
            ]
            if viable:
                media_exact_candidates = viable

        _, best_media_item = min(media_exact_candidates, key=exact_match_media_sort_key)
        return normalize_exact_match_cast(best_media_item)

    for source in EXACT_MATCH_SOURCE_PRIORITY:
        if source in {"movie", "tv"}:
            continue
        items = results.get(source) or []
        for item in items:
            if is_exact_match(normalized_query, item, source):
                return normalize_exact_match_cast(item)

    return None


def collect_exact_matches(
    results: dict[str, list[dict[str, object]]],
    query: str | None,
    hero: dict[str, object] | None = None,
) -> list[dict[str, object]]:
    """Return all exact-match items across sources, ranked."""
    if not query or len(query.strip()) < 2:
        return []

    normalized_query = query.strip()
    collected: list[dict[str, object]] = []

    media_candidates: list[tuple[str, dict[str, object]]] = []
    for source in ("movie", "tv"):
        for item in results.get(source) or []:
            if not is_exact_match(normalized_query, item, source):
                continue
            watch_providers = item.get("watch_providers")
            if not isinstance(watch_providers, dict) or not watch_providers.get("streaming_platform_ids"):
                continue
            media_candidates.append((source, item))

    if media_candidates:
        media_candidates.sort(key=exact_match_media_sort_key)
        collected.extend(normalize_exact_match_cast(item) for _, item in media_candidates)

    for source in EXACT_MATCH_SOURCE_PRIORITY:
        if source in {"movie", "tv"}:
            continue
        for item in results.get(source) or []:
            if is_exact_match(normalized_query, item, source):
                collected.append(normalize_exact_match_cast(item))

    if hero is not None:
        hero_id = hero.get("mc_id")
        if isinstance(hero_id, str) and hero_id:
            collected = [item for item in collected if item.get("mc_id") != hero_id]
            collected.insert(0, hero)

    return collected
