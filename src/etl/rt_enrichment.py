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

from typing import Any

from api.rottentomatoes.local_store import (
    LookupRecord,
    RTContentLookupStore,
    add_hits_to_cache,
    get_store,
)
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

    mc_type = doc.get("mc_type", "movie")
    external_ids: dict[str, Any] | None = doc.get("external_ids")

    # Tier 1: vanity lookup via rt_id
    if isinstance(external_ids, dict):
        rt_id = external_ids.get("rt_id")
        if isinstance(rt_id, str) and rt_id:
            vanity = _strip_rt_prefix(rt_id)
            record = store.lookup_by_vanity(vanity)
            if record is not None:
                return _apply_record(doc, record, mc_type, via="vanity")

    # Tier 2: title + year
    title = doc.get("title") or doc.get("search_title")
    if not title:
        return False

    year: int | None = doc.get("year")
    candidates = store.lookup(title=title, year=year)
    if candidates:
        return _apply_record(doc, candidates[0], mc_type, via="title+year")

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
    from api.rottentomatoes.core import RottenTomatoesService
    from api.rottentomatoes.models import MCRottenTomatoesItem

    if store is None:
        store = get_store()

    title = doc.get("title") or doc.get("search_title")
    if not title:
        return False

    mc_type = doc.get("mc_type", "movie")
    year: int | None = doc.get("year")

    service = RottenTomatoesService()
    try:
        response = await service.search_content(query=title, limit=5)
    except Exception:
        logger.warning("Algolia search failed for title=%s", title)
        return False

    if not response.results:
        return False

    best: MCRottenTomatoesItem | None = None
    for item in response.results:
        if year is not None and item.release_year != year:
            continue
        rt_type = (item.mc_type.value if item.mc_type else "").lower()
        if rt_type and rt_type != mc_type:
            continue
        best = item
        break

    if best is None and response.results:
        best = response.results[0]

    if best is None:
        return False

    raw_hit = best.model_dump(exclude_none=True)
    add_hits_to_cache([raw_hit])

    rotten = best.metrics or {}
    record: LookupRecord = {
        "objectID": best.source_id or "",
        "title": best.title,
        "type": mc_type,
        "release_year": best.release_year,
        "vanity": best.vanity,
        "critics_score": rotten.get("critics_score") or best.critics_score,
        "audience_score": rotten.get("audience_score") or best.audience_score,
        "runtime": best.runtime,
        "tms_id": best.tms_id,
    }

    return _apply_record(doc, record, mc_type, via="algolia")


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
