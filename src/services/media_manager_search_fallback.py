"""Media Manager semantic fallback when indexed movie/tv buckets are empty."""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Any, Literal

from adapters.media_manager_client import MediaManagerClient
from utils.get_logger import get_logger

logger = get_logger(__name__)

FALLBACK_HTTP_TIMEOUT_SECONDS = 8.0
# Media Manager LLM rerank (`re_rank: true`) can take noticeably longer than FAISS-only.
FALLBACK_LLM_RERANK_TIMEOUT_SECONDS = 60.0
FALLBACK_MM_RE_RANK = True
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/"
# Same size as MC medium poster (`w185`) used in indexed Redis docs / TMDB models.
TMDB_POSTER_SIZE = "w185"
MEDIA_MANAGER_FALLBACK_BUCKETS: frozenset[str] = frozenset({"movie", "tv"})
INDEX_BY_BUCKET: dict[str, Literal["movie-index", "tv-index"]] = {
    "movie": "movie-index",
    "tv": "tv-index",
}


@dataclass(frozen=True)
class SemanticFallbackQuerySpec:
    """One or more query strings for Media Manager (extensible for dual-path)."""

    primary: str
    extras: tuple[str, ...] = ()

    @property
    def all_queries(self) -> tuple[str, ...]:
        if not self.extras:
            return (self.primary,)
        return (self.primary, *self.extras)


def resolve_media_manager_search_base_url() -> str | None:
    """Serving URL first, then API URL (ETL may also expose search in dev)."""
    for key in ("MEDIA_MANAGER_SERVING_URL", "MEDIA_MANAGER_API_URL"):
        raw = (os.getenv(key) or "").strip()
        if raw:
            return raw.rstrip("/")
    return None


def semantic_fallback_eligible(
    *,
    query_text: str | None,
    skip: int,
    raw: bool,
    has_filters: bool,
    media_sort: str,
    mc_type: str | None,
    requested_sources: set[str],
    full_results: dict[str, list[dict[str, Any]]],
) -> bool:
    if skip != 0 or raw or has_filters:
        return False
    if media_sort != "popularity":
        return False
    if query_text is None or len(query_text.strip()) < 2:
        return False
    if resolve_media_manager_search_base_url() is None:
        return False

    buckets = MEDIA_MANAGER_FALLBACK_BUCKETS & requested_sources
    if mc_type in MEDIA_MANAGER_FALLBACK_BUCKETS:
        buckets = buckets & {mc_type}
    if not buckets:
        return False

    return any(not full_results.get(bucket) for bucket in buckets)


def _string_from_taxonomy_entry(entry: object) -> str | None:
    if isinstance(entry, str):
        stripped = entry.strip()
        return stripped if stripped else None
    if isinstance(entry, dict):
        for key in ("name", "search_title", "title"):
            val = entry.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
        entry_id = entry.get("id")
        if entry_id is not None:
            return str(entry_id)
    return None


def _coerce_string_list(value: object) -> list[str]:
    """Coerce Redis/MM keyword or genre lists to str lists for ranking."""
    if not value:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        text = _string_from_taxonomy_entry(item)
        if text is not None:
            out.append(text)
    return out


def _normalize_ranking_fields(doc: dict[str, Any]) -> None:
    """In-place: match redis-search ranking expectations (string lists)."""
    doc["keywords"] = _coerce_string_list(doc.get("keywords"))
    doc["genres"] = _coerce_string_list(doc.get("genres"))
    if not doc.get("cast_names"):
        doc["cast_names"] = _coerce_string_list(doc.get("cast"))
    else:
        doc["cast_names"] = _coerce_string_list(doc.get("cast_names"))

    director = doc.get("director_name") or doc.get("director")
    if isinstance(director, dict):
        doc["director_name"] = _string_from_taxonomy_entry(director) or ""
    elif isinstance(director, str):
        doc["director_name"] = director


def _poster_url_from_path(poster_path: object) -> str | None:
    if not isinstance(poster_path, str) or not poster_path.strip():
        return None
    path = poster_path.strip()
    if path.startswith(("http://", "https://")):
        return path
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{TMDB_IMAGE_BASE}{TMDB_POSTER_SIZE}{path}"


def _apply_media_display_contract(doc: dict[str, Any], mm_row: dict[str, Any]) -> None:
    """Fill UI-facing fields only when the redis-shaped payload is missing them."""
    poster_url = mm_row.get("poster_image_url")
    if isinstance(poster_url, str) and poster_url.strip() and not doc.get("image"):
        doc["image"] = poster_url.strip()

    if not doc.get("image"):
        for candidate in (doc.get("poster_path"), doc.get("image")):
            built = _poster_url_from_path(candidate)
            if built:
                doc["image"] = built
                if isinstance(candidate, str) and candidate.startswith("/"):
                    doc.setdefault("poster_path", candidate)
                break

    doc.setdefault("source", "tmdb")
    if not doc.get("mc_subtype"):
        mc_type = doc.get("mc_type")
        if mc_type in ("movie", "tv"):
            doc["mc_subtype"] = mc_type

    if doc.get("rating") is None:
        vote_avg = doc.get("vote_average")
        if vote_avg is not None:
            doc["rating"] = vote_avg

    if not doc.get("year"):
        for date_field in ("release_date", "first_air_date"):
            val = doc.get(date_field)
            if isinstance(val, str) and len(val) >= 4 and val[:4].isdigit():
                doc["year"] = int(val[:4])
                break
        if not doc.get("year"):
            mm_date = mm_row.get("release_date")
            if isinstance(mm_date, str) and len(mm_date) >= 4 and mm_date[:4].isdigit():
                doc["year"] = int(mm_date[:4])

    if not doc.get("source_id"):
        mc_id = str(doc.get("mc_id") or "")
        tail = mc_id.rsplit("_", 1)
        if len(tail) == 2 and tail[1].isdigit():
            doc["source_id"] = tail[1]


def _redis_doc_from_mm_row(item: dict[str, Any]) -> dict[str, Any] | None:
    """FAISS ``index_metadata`` is the redis-search JSON doc MM indexed from Redis."""
    metadata = item.get("metadata")
    if isinstance(metadata, dict):
        index_meta = metadata.get("index_metadata")
        if isinstance(index_meta, dict) and index_meta.get("mc_id"):
            return dict(index_meta)
        nested = metadata.get("metadata")
        if isinstance(nested, dict) and nested.get("mc_id"):
            return dict(nested)

    normalized = item.get("normalized_metadata")
    if isinstance(normalized, dict) and normalized.get("mc_id"):
        return dict(normalized)
    return None


def _redact_query_for_log(query: str, max_len: int = 80) -> str:
    trimmed = query.strip()
    if len(trimmed) <= max_len:
        return trimmed
    return f"{trimmed[: max_len - 3]}..."


def map_media_manager_hit_to_redis_doc(
    item: dict[str, Any],
    bucket: str,
) -> dict[str, Any] | None:
    """Return the redis-search media JSON embedded in the MM FAISS hit (no live MGET)."""
    raw_doc = _redis_doc_from_mm_row(item)
    if raw_doc is None:
        return None

    doc = raw_doc
    mc_id = str(doc.get("mc_id") or "").strip()
    if not mc_id:
        return None

    doc.setdefault("mc_type", bucket)
    doc.setdefault("id", mc_id)
    # Same display rule as ``parse_doc``: keep ``title``, mirror to ``search_title``.
    if doc.get("title"):
        doc["search_title"] = doc["title"]
    elif not doc.get("search_title"):
        title = item.get("title")
        if isinstance(title, str) and title.strip():
            doc["search_title"] = title.strip()
            doc.setdefault("title", title.strip())

    _normalize_ranking_fields(doc)
    _apply_media_display_contract(doc, item)
    doc["_semantic_fallback"] = True
    return doc


async def _search_bucket(
    client: MediaManagerClient,
    *,
    query: str,
    bucket: str,
    limit: int,
) -> tuple[list[dict[str, Any]], float, str | None, dict[str, Any]]:
    index_name = INDEX_BY_BUCKET[bucket]
    request_timeout = (
        FALLBACK_LLM_RERANK_TIMEOUT_SECONDS
        if FALLBACK_MM_RE_RANK
        else FALLBACK_HTTP_TIMEOUT_SECONDS
    )
    started = time.perf_counter()
    try:
        payload = await client.search_index(
            query=query,
            index_name=index_name,
            limit=limit,
            re_rank=FALLBACK_MM_RE_RANK,
            timeout=request_timeout,
        )
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - started) * 1000
        logger.warning(
            "Media Manager fallback %s failed query=%r latency=%.0fms err=%s",
            bucket,
            _redact_query_for_log(query),
            elapsed_ms,
            exc,
        )
        return [], elapsed_ms, str(exc), {}

    elapsed_ms = (time.perf_counter() - started) * 1000
    raw_results = payload.get("results")
    if not isinstance(raw_results, list):
        return [], elapsed_ms, "malformed_results", {}

    mm_response_meta = _mm_search_response_meta(payload)

    mapped: list[dict[str, Any]] = []
    for row in raw_results:
        if not isinstance(row, dict):
            continue
        doc = map_media_manager_hit_to_redis_doc(row, bucket)
        if doc is not None:
            mapped.append(doc)
    return mapped[:limit], elapsed_ms, None, mm_response_meta


def _mm_search_response_meta(payload: dict[str, Any]) -> dict[str, Any]:
    """Surface MM search extras (LLM rerank) on ``semantic_fallback`` metadata."""
    meta: dict[str, Any] = {"re_rank_requested": FALLBACK_MM_RE_RANK}
    if isinstance(payload.get("re_rank_applied"), bool):
        meta["re_rank_applied"] = payload["re_rank_applied"]
    commentary = payload.get("commentary")
    if isinstance(commentary, str) and commentary.strip():
        meta["commentary"] = commentary
    followups = payload.get("followup_prompts")
    if isinstance(followups, list) and followups:
        meta["followup_prompts_count"] = len(followups)
    query_used = payload.get("query_used")
    if isinstance(query_used, str) and query_used.strip():
        meta["query_used"] = query_used
    return meta


async def apply_media_manager_semantic_fallback(
    *,
    full_results: dict[str, list[dict[str, Any]]],
    query_spec: SemanticFallbackQuerySpec,
    requested_sources: set[str],
    limit: int,
    skip: int,
    raw: bool,
    has_filters: bool,
    media_sort: str,
    mc_type: str | None,
) -> dict[str, Any] | None:
    """Best-effort fill for empty movie/tv buckets. Returns metadata or None."""
    query_text = query_spec.primary
    if not semantic_fallback_eligible(
        query_text=query_text,
        skip=skip,
        raw=raw,
        has_filters=has_filters,
        media_sort=media_sort,
        mc_type=mc_type,
        requested_sources=requested_sources,
        full_results=full_results,
    ):
        return None

    base_url = resolve_media_manager_search_base_url()
    if base_url is None:
        return None

    buckets = MEDIA_MANAGER_FALLBACK_BUCKETS & requested_sources
    if mc_type in MEDIA_MANAGER_FALLBACK_BUCKETS:
        buckets = buckets & {mc_type}
    empty_buckets = [b for b in sorted(buckets) if not full_results.get(b)]
    if not empty_buckets:
        return None

    # v1 uses primary query only; extras reserved for dual-path MM calls.
    query = query_spec.primary.strip()
    meta: dict[str, Any] = {}
    client_timeout = (
        FALLBACK_LLM_RERANK_TIMEOUT_SECONDS
        if FALLBACK_MM_RE_RANK
        else FALLBACK_HTTP_TIMEOUT_SECONDS
    )
    client = MediaManagerClient(
        base_url=base_url,
        timeout=client_timeout,
    )
    try:
        tasks = {
            bucket: asyncio.create_task(
                _search_bucket(client, query=query, bucket=bucket, limit=limit)
            )
            for bucket in empty_buckets
        }
        for bucket, task in tasks.items():
            docs, latency_ms, error, mm_extras = await task
            bucket_meta: dict[str, Any] = {
                "applied": bool(docs),
                "count": len(docs),
                "latency_ms": round(latency_ms, 1),
                "query": query,
                "index_name": INDEX_BY_BUCKET[bucket],
            }
            if error:
                bucket_meta["error"] = error
            if mm_extras:
                bucket_meta.update(mm_extras)
            meta[bucket] = bucket_meta
            if docs:
                full_results[bucket] = docs
                logger.info(
                    "Media Manager fallback %s count=%d latency=%.0fms query=%r",
                    bucket,
                    len(docs),
                    latency_ms,
                    _redact_query_for_log(query),
                )
    finally:
        await client.close()

    return {k: v for k, v in meta.items() if v is not None}


def extra_queries_for_source_hint(
    *,
    stripped_q: str | None,
    primary: str,
) -> tuple[str, ...]:
    """Extra MM queries for dual source-hint paths (not used until wired)."""
    if stripped_q is None:
        return ()
    stripped = stripped_q.strip()
    if len(stripped) < 2 or stripped == primary.strip():
        return ()
    return (stripped,)


def build_fallback_query_spec(
    query_text: str | None,
    *,
    extra_queries: tuple[str, ...] = (),
) -> SemanticFallbackQuerySpec | None:
    if query_text is None or len(query_text.strip()) < 2:
        return None
    return SemanticFallbackQuerySpec(primary=query_text.strip(), extras=extra_queries)
