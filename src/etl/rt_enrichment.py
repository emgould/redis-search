"""Rotten Tomatoes enrichment for media documents.

Provides a 3-tier matching strategy:
  1. Direct vanity lookup via ``rt_id`` from ``external_ids`` (O(1) dict hit).
  2. Title + year fuzzy match against local RT content index.
  3. (Optional, backfill-only) Live Algolia search fallback.

When a match is found, the following fields are stamped onto the document:
  rt_audience_score, rt_critics_score, rt_vanity, rt_release_year, rt_runtime

On a successful title+year or Algolia match the ``external_ids.rt_id`` field
is also back-populated with the matched vanity (prefixed ``m/`` or ``tv/``).
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any

from api.rottentomatoes.core import RottenTomatoesService
from api.rottentomatoes.local_store import (
    LookupRecord,
    RTContentLookupStore,
    add_hits_to_cache,
    get_store,
)
from api.rottentomatoes.models import MCRottenTomatoesItem
from contracts.models import MCType
from utils.get_logger import get_logger

logger = get_logger(__name__)

_RT_TYPE_PREFIX: dict[str, str] = {
    "movie": "m/",
    "tv": "tv/",
}


def _strip_rt_prefix(rt_id: str) -> str:
    """Remove the 'm/' or 'tv/' prefix from an rt_id to get the bare vanity."""
    for prefix in ("m/", "tv/"):
        if rt_id.startswith(prefix):
            return rt_id[len(prefix):]
    return rt_id


def normalize_rt_match_title(value: str | None) -> str:
    """Normalize titles for strict RT match acceptance."""
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKC", value)
    normalized = normalized.casefold()
    normalized = re.sub(r"[^\w\s]", " ", normalized, flags=re.UNICODE)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _doc_mc_type(doc: dict[str, Any]) -> str:
    mc_type = doc.get("mc_type")
    if isinstance(mc_type, str) and mc_type:
        return mc_type
    return "movie"


def _doc_year(doc: dict[str, Any]) -> int | None:
    year = doc.get("year")
    if isinstance(year, int):
        return year
    if isinstance(year, str):
        try:
            return int(year)
        except ValueError:
            return None

    for field_name in ("release_date", "first_air_date"):
        value = doc.get(field_name)
        if isinstance(value, str) and len(value) >= 4:
            try:
                return int(value[:4])
            except ValueError:
                continue
    return None


def _record_type_matches(mc_type: str, record: LookupRecord) -> bool:
    record_type = record.get("type")
    if not isinstance(record_type, str) or not record_type:
        return False
    normalized_type = record_type.lower()
    if normalized_type in {"series", "tv_series", "tv_show"}:
        normalized_type = "tv"
    return normalized_type == mc_type


def _record_year(record: LookupRecord) -> int | None:
    value = record.get("release_year")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def rt_record_matches_doc(doc: dict[str, Any], record: LookupRecord) -> bool:
    """Return True only when an RT record strictly matches title, type, and year."""
    mc_type = _doc_mc_type(doc)
    if not _record_type_matches(mc_type, record):
        return False

    doc_year = _doc_year(doc)
    record_year = _record_year(record)
    if doc_year is None or record_year is None or doc_year != record_year:
        return False

    record_title = record.get("title")
    if not isinstance(record_title, str):
        return False
    normalized_record_title = normalize_rt_match_title(record_title)
    candidate_titles = (
        doc.get("title"),
        doc.get("search_title"),
        doc.get("original_title"),
    )
    return any(
        normalize_rt_match_title(title if isinstance(title, str) else None)
        == normalized_record_title
        for title in candidate_titles
    )


def _apply_record(
    doc: dict[str, Any],
    record: LookupRecord,
    mc_type: str,
    via: str,
) -> bool:
    """Stamp RT fields from *record* onto *doc*. Returns True when any field changed."""
    changed = False
    field_map: list[tuple[str, str]] = [
        ("audience_score", "rt_audience_score"),
        ("critics_score", "rt_critics_score"),
        ("vanity", "rt_vanity"),
        ("release_year", "rt_release_year"),
        ("runtime", "rt_runtime"),
    ]
    for src_key, dst_key in field_map:
        value = record.get(src_key)
        if value is not None and doc.get(dst_key) != value:
            doc[dst_key] = value
            changed = True

    vanity = record.get("vanity")
    if vanity:
        prefix = _RT_TYPE_PREFIX.get(mc_type, "m/")
        rt_id_value = f"{prefix}{vanity}"
        external_ids: dict[str, Any] = doc.get("external_ids") or {}
        if external_ids.get("rt_id") != rt_id_value:
            external_ids["rt_id"] = rt_id_value
            doc["external_ids"] = external_ids
            changed = True

        tms_id = record.get("tms_id")
        if tms_id and not external_ids.get("tms_id"):
            external_ids["tms_id"] = tms_id
            doc["external_ids"] = external_ids
            changed = True

    if changed:
        logger.debug(
            "RT enriched doc=%s via=%s vanity=%s audience=%s critics=%s",
            doc.get("id", "?"),
            via,
            record.get("vanity"),
            record.get("audience_score"),
            record.get("critics_score"),
        )
    return changed


def enrich_from_local(
    doc: dict[str, Any],
    store: RTContentLookupStore | None = None,
) -> bool:
    """Attempt local RT enrichment (tier 1 vanity, then tier 2 title+year).

    Returns True when the document was modified.
    """
    if store is None:
        store = get_store()

    mc_type = _doc_mc_type(doc)
    external_ids: dict[str, Any] | None = doc.get("external_ids")

    # Tier 1: vanity lookup via rt_id
    if isinstance(external_ids, dict):
        rt_id = external_ids.get("rt_id")
        if isinstance(rt_id, str) and rt_id:
            vanity = _strip_rt_prefix(rt_id)
            record = store.lookup_by_vanity(vanity)
            if record is not None and rt_record_matches_doc(doc, record):
                return _apply_record(doc, record, mc_type, via="vanity")

    # Tier 2: title + year
    title = doc.get("title") or doc.get("search_title")
    if not title:
        return False

    year = _doc_year(doc)
    candidates = store.lookup(title=title, year=year)
    for record in candidates:
        if rt_record_matches_doc(doc, record):
            return _apply_record(doc, record, mc_type, via="title+year")

    return False


def enrich_from_local_title_year(
    doc: dict[str, Any],
    store: RTContentLookupStore | None = None,
) -> bool:
    """Attempt local RT enrichment without trusting existing ``external_ids.rt_id``."""
    if store is None:
        store = get_store()

    title = doc.get("title") or doc.get("search_title")
    if not isinstance(title, str) or not title.strip():
        return False

    mc_type = _doc_mc_type(doc)
    candidates = store.lookup(title=title, year=_doc_year(doc))
    for record in candidates:
        if rt_record_matches_doc(doc, record):
            return _apply_record(doc, record, mc_type, via="title+year")

    return False


async def enrich_from_algolia(
    doc: dict[str, Any],
    store: RTContentLookupStore | None = None,
) -> bool:
    """Live Algolia fallback (tier 3). Used during backfill only.

    Searches RT Algolia for the document title, selects the best match,
    enriches the doc, and inserts the result into the local cache.

    Returns True when the document was modified.
    """
    if store is None:
        store = get_store()

    raw_title = doc.get("title") or doc.get("search_title") or ""
    title = raw_title.strip() if isinstance(raw_title, str) else ""
    if not title:
        return False

    mc_type = _doc_mc_type(doc)
    media_type = MCType.TV_SERIES if mc_type == "tv" else MCType.MOVIE

    service = RottenTomatoesService()
    try:
        response = await service.search_content(query=title, limit=5, media_type=media_type)
    except Exception:
        logger.warning("Algolia search failed for title=%s", title)
        return False

    if not response.results:
        return False

    best: MCRottenTomatoesItem | None = None
    for item in response.results:
        record = _record_from_algolia_item(item, mc_type)
        if not rt_record_matches_doc(doc, record):
            continue
        best = item
        break

    if best is None:
        logger.debug("No strict RT Algolia match for title=%s type=%s", title, mc_type)
        return False

    raw_hit = best.model_dump(exclude_none=True)
    add_hits_to_cache([raw_hit])

    record = _record_from_algolia_item(best, mc_type)

    return _apply_record(doc, record, mc_type, via="algolia")


def _record_from_algolia_item(item: MCRottenTomatoesItem, fallback_type: str) -> LookupRecord:
    rotten = item.metrics or {}
    item_type = item.mc_type.value if item.mc_type is not None else fallback_type
    return {
        "objectID": item.source_id or "",
        "title": item.title,
        "type": item_type,
        "release_year": item.release_year,
        "vanity": item.vanity,
        "critics_score": rotten.get("critics_score") or item.critics_score,
        "audience_score": rotten.get("audience_score") or item.audience_score,
        "runtime": item.runtime,
        "tms_id": item.tms_id,
    }


def enrich_redis_doc(
    redis_doc: dict[str, Any],
    store: RTContentLookupStore | None = None,
) -> bool:
    """Synchronous local-only enrichment entry point for the ETL pipeline."""
    return enrich_from_local(redis_doc, store=store)


async def enrich_redis_doc_with_fallback(
    redis_doc: dict[str, Any],
    store: RTContentLookupStore | None = None,
) -> bool:
    """Async enrichment with Algolia fallback (for backfill scripts)."""
    if enrich_from_local(redis_doc, store=store):
        return True
    return await enrich_from_algolia(redis_doc, store=store)
