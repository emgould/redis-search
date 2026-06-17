#!/usr/bin/env python3
"""Backfill media titles from 2026 that are on a top-12 provider but were missed by the ETL.

Phase A: Scan cached TMDB files (data/us/movie, data/us/tv) for 2026 months that
         already have local JSON. Titles that pass the major-provider filter and are
         missing from Redis are enriched and loaded.

Phase B: For 2026 months without cached files, hit the TMDB Discover API to pull
         candidates, enrich, filter, and load.

Usage:
    python scripts/backfill_major_provider_media.py --dry-run
    python scripts/backfill_major_provider_media.py --apply
"""

import argparse
import asyncio
import json
import os
import re
import time
from calendar import monthrange
from collections.abc import Awaitable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal, cast

from redis.asyncio import Redis

from src.adapters.config import load_env
from src.ai.microgenre_batch import build_microgenre_input_from_document
from src.ai.microgenre_document import microgenre_result_to_redis, valid_microgenres_value
from src.ai.prompts.microgenre_classifier import score_microgenres
from src.api.tmdb.core import TMDBService
from src.api.tmdb.models import MCMovieItem, MCTvItem
from src.contracts.models import MCType
from src.core.normalize import document_to_redis, normalize_document, resolve_timestamps
from src.core.streaming_providers import MAJOR_PROVIDER_IDS
from src.etl.documentary_filter import is_documentary, is_eligible_documentary
from src.etl.rt_enrichment import enrich_from_algolia, enrich_from_local
from src.utils.genre_mapping import get_genre_mapping_with_fallback
from src.utils.get_logger import get_logger

logger = get_logger(__name__)

BATCH_SIZE = 15
LOAD_BATCH_SIZE = 100
CACHE_FILE_RE = re.compile(r"^tmdb_(movie|tv)_(\d{4})_(\d{2})\.json$")
DOCUMENTARY_LOOKBACK_YEARS = 10
TARGET_YEAR = 2026
SOURCE_TAG = "backfill_20260513"


def _parse_cache_file(path: Path) -> tuple[str, int, int] | None:
    """Return (media_type, year, month) from a cache filename, or None."""
    m = CACHE_FILE_RE.match(path.name)
    if not m:
        return None
    return m.group(1), int(m.group(2)), int(m.group(3))


def _month_range(year: int, month: int) -> tuple[str, str]:
    """First and last day of a given month as YYYY-MM-DD strings."""
    first = f"{year:04d}-{month:02d}-01"
    _, last_day = monthrange(year, month)
    last = f"{year:04d}-{month:02d}-{last_day:02d}"
    return first, last


def _extract_provider_ids(item: dict[str, Any]) -> set[int]:
    """Extract provider IDs from raw TMDB + custom provider structures."""
    ids: set[int] = set()
    wp = item.get("watch_providers") or {}

    for key in ("flatrate", "buy", "rent"):
        for p in wp.get(key, []):
            if isinstance(p, dict) and isinstance(p.get("provider_id"), int):
                ids.add(p["provider_id"])

    for key in ("streaming_platform_ids", "on_demand_platform_ids"):
        id_list = wp.get(key)
        if isinstance(id_list, list):
            for pid in id_list:
                if isinstance(pid, int):
                    ids.add(pid)

    for key in ("streaming_platforms", "on_demand_platforms"):
        for p in wp.get(key, []):
            if isinstance(p, dict) and isinstance(p.get("provider_id"), int):
                ids.add(p["provider_id"])

    primary = wp.get("primary_provider")
    if isinstance(primary, dict) and isinstance(primary.get("provider_id"), int):
        ids.add(primary["provider_id"])

    primary_id = wp.get("primary_provider_id")
    if isinstance(primary_id, int):
        ids.add(primary_id)

    return ids


def _has_major_provider(item: dict[str, Any]) -> bool:
    """True when item has at least one top-12 provider ID."""
    return bool(_extract_provider_ids(item) & MAJOR_PROVIDER_IDS)


def _passes_backfill_filter(item: dict[str, Any], media_type: str) -> bool:
    """Lightweight filter for backfill candidates.

    Requires poster + major provider. Documentaries route through existing
    documentary eligibility checks. Movies require runtime >= 40.
    """
    if not item.get("poster_path"):
        return False

    if is_documentary(item):
        return is_eligible_documentary(
            item,
            years_back=DOCUMENTARY_LOOKBACK_YEARS,
            as_of=date.today(),
            require_major_provider=True,
        )

    if not _has_major_provider(item):
        return False

    if media_type == "movie":
        runtime = item.get("runtime") or 0
        if runtime < 40:
            return False

    return True


class BackfillDiscoverService(TMDBService):
    """Thin wrapper around TMDBService for Discover API calls."""

    async def discover_ids(
        self,
        media_type: str,
        date_gte: str,
        date_lte: str,
        *,
        max_pages: int = 500,
    ) -> list[int]:
        """Return TMDB IDs from the Discover endpoint for a date window."""
        endpoint = f"discover/{media_type}"
        date_field = "release_date" if media_type == "movie" else "first_air_date"
        params: dict[str, Any] = {
            f"{date_field}.gte": date_gte,
            f"{date_field}.lte": date_lte,
            "include_adult": "false",
            "include_video": "false",
            "language": "en-US",
            "region": "US",
            "sort_by": "popularity.desc",
            "page": 1,
        }

        first_page = await self._make_request(endpoint, params)
        if not first_page:
            return []

        total_pages = min(first_page.get("total_pages", 1), max_pages)
        ids: list[int] = []
        seen: set[int] = set()

        for item in first_page.get("results", []):
            tid = item.get("id")
            if tid and tid not in seen:
                seen.add(tid)
                ids.append(tid)

        for page_num in range(2, total_pages + 1):
            page_params = {**params, "page": page_num}
            result = await self._make_request(endpoint, page_params)
            if result and isinstance(result, dict):
                for item in result.get("results", []):
                    tid = item.get("id")
                    if tid and tid not in seen:
                        seen.add(tid)
                        ids.append(tid)
            if page_num % 10 == 0:
                logger.info("  Discover page %d/%d", page_num, total_pages)
                await asyncio.sleep(0.2)

        logger.info("Discovered %d %s titles for %s → %s", len(ids), media_type, date_gte, date_lte)
        return ids


async def _enrich_ids(
    service: BackfillDiscoverService,
    tmdb_ids: list[int],
    mc_type: MCType,
) -> list[dict[str, Any]]:
    """Enrich a list of TMDB IDs, returning enriched payloads."""
    enriched: list[dict[str, Any]] = []
    total = len(tmdb_ids)
    for i in range(0, total, BATCH_SIZE):
        batch_ids = tmdb_ids[i : i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

        logger.info("Enriching batch %d/%d (%d ids)", batch_num, total_batches, len(batch_ids))

        tasks = [
            service.get_media_details(
                tid,
                mc_type,
                include_cast=True,
                include_videos=True,
                include_watch_providers=True,
                include_keywords=True,
                cast_limit=10,
            )
            for tid in batch_ids
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for idx, result in enumerate(results):
            tid = batch_ids[idx]
            if isinstance(result, Exception):
                logger.warning("Error enriching %s %d: %s", mc_type.value, tid, result)
                continue
            if result is None:
                continue
            if isinstance(result, (MCMovieItem, MCTvItem)):
                enriched.append(result.model_dump(mode="json"))
            elif isinstance(result, dict):
                enriched.append(result)

        if i + BATCH_SIZE < total:
            await asyncio.sleep(0.3)

    logger.info("Enriched %d / %d titles", len(enriched), total)
    return enriched


async def _flush_batch(
    redis: Redis,
    prepared: list[tuple[str, dict[str, Any]]],
    enrich_semaphore: asyncio.Semaphore,
    microgenre_semaphore: asyncio.Semaphore,
    stats: dict[str, int],
) -> None:
    """RT-enrich, attach microgenres, resolve timestamps, and pipeline-write a batch."""

    async def _enrich_with_backoff(redis_doc: dict[str, Any]) -> None:
        if enrich_from_local(redis_doc):
            return
        max_retries = 4
        delay = 1.0
        async with enrich_semaphore:
            for attempt in range(max_retries):
                try:
                    await enrich_from_algolia(redis_doc)
                    return
                except Exception as exc:
                    is_rate_limit = "429" in str(exc)
                    if is_rate_limit and attempt < max_retries - 1:
                        logger.debug("Algolia 429, backing off %.1fs", delay)
                        await asyncio.sleep(delay)
                        delay = min(delay * 2, 16.0)
                    else:
                        if not is_rate_limit:
                            logger.debug("Algolia enrichment failed for doc: %s", exc)
                        return

    async def _attach_microgenres(
        redis_doc: dict[str, Any],
        existing_doc: dict[str, Any] | None,
    ) -> None:
        existing_microgenres = valid_microgenres_value(
            existing_doc.get("microgenres") if existing_doc else None
        )
        if existing_microgenres is not None:
            redis_doc["microgenres"] = existing_microgenres
            stats["microgenres_preserved"] += 1
            return

        if existing_doc is not None:
            stats["microgenres_skipped_existing"] += 1
            return

        current_microgenres = valid_microgenres_value(redis_doc.get("microgenres"))
        if current_microgenres is not None:
            redis_doc["microgenres"] = current_microgenres
            stats["microgenres_preserved"] += 1
            return

        doc_media_type = redis_doc.get("mc_type")
        if doc_media_type not in ("movie", "tv"):
            return

        async with microgenre_semaphore:
            try:
                classifier_input = build_microgenre_input_from_document(
                    redis_doc,
                    cast(Literal["movie", "tv"], doc_media_type),
                    score_threshold=0.1,
                )
                response = await score_microgenres(classifier_input)
            except Exception as exc:
                stats["microgenres_failed"] += 1
                logger.warning(
                    "Microgenre classification raised for %s: %s",
                    redis_doc.get("mc_id") or redis_doc.get("id"),
                    exc,
                )
                return

            if response.error is not None or response.result is None:
                stats["microgenres_failed"] += 1
                logger.warning(
                    "Microgenre classification failed for %s: %s",
                    redis_doc.get("mc_id") or redis_doc.get("id"),
                    response.error or "no result",
                )
                return

            redis_doc["microgenres"] = microgenre_result_to_redis(response.result)
            stats["microgenres_generated"] += 1

    # 1. RT enrichment
    await asyncio.gather(*[_enrich_with_backoff(doc) for _, doc in prepared])

    # 2. Read existing docs for timestamp resolution + microgenre preservation
    now_ts = int(datetime.now(UTC).timestamp())
    read_pipe = redis.pipeline()
    for key, _ in prepared:
        read_pipe.json().get(key)
    existing_docs: list[object] = await read_pipe.execute()

    # 3. Microgenres
    await asyncio.gather(
        *[
            _attach_microgenres(
                redis_doc,
                existing if isinstance(existing, dict) else None,
            )
            for (_key, redis_doc), existing in zip(prepared, existing_docs, strict=True)
        ]
    )

    # 4. Timestamp resolution + pipeline write
    write_pipe = redis.pipeline()
    for (key, redis_doc), existing in zip(prepared, existing_docs, strict=True):
        existing_dict = existing if isinstance(existing, dict) else None
        ca, ma, src = resolve_timestamps(existing_dict, now_ts, source_tag=SOURCE_TAG)
        redis_doc["created_at"] = ca
        redis_doc["modified_at"] = ma
        redis_doc["_source"] = src
        write_pipe.json().set(key, "$", redis_doc)
    await write_pipe.execute()


async def run_backfill(
    data_root: Path,
    redis_host: str,
    redis_port: int,
    redis_password: str | None,
    apply_updates: bool,
) -> None:
    """Run the 2026 major-provider backfill (Phase A + Phase B)."""
    load_env()

    genre_mapping = await get_genre_mapping_with_fallback(allow_fallback=True)
    logger.info("Loaded %d genres", len(genre_mapping))

    redis: Redis | None = None
    if apply_updates:
        redis = Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=True,
        )
        await cast(Awaitable[bool], redis.ping())
        logger.info("Connected to Redis at %s:%d", redis_host, redis_port)

    cached_months: dict[str, set[int]] = {"movie": set(), "tv": set()}
    total_candidates = 0
    total_missing = 0
    total_loaded = 0
    total_skipped = 0
    seen_keys: set[str] = set()

    enrich_semaphore = asyncio.Semaphore(20)
    microgenre_concurrency = max(1, int(os.getenv("MICROGENRE_ETL_CONCURRENCY", "3")))
    microgenre_semaphore = asyncio.Semaphore(microgenre_concurrency)
    enrichment_stats: dict[str, int] = {
        "microgenres_preserved": 0,
        "microgenres_skipped_existing": 0,
        "microgenres_generated": 0,
        "microgenres_failed": 0,
    }

    async def _collect_and_load(
        items: list[dict[str, Any]],
        media_type: str,
        genre_mapping: dict[int, str],
    ) -> None:
        """Normalize items, skip already-indexed, batch-load with full enrichment."""
        nonlocal total_candidates, total_missing, total_loaded, total_skipped

        mc_type_enum = MCType.TV_SERIES if media_type == "tv" else MCType.MOVIE
        pending: list[tuple[str, dict[str, Any]]] = []

        for item in items:
            if not _passes_backfill_filter(item, media_type):
                continue

            total_candidates += 1
            normalized = normalize_document(
                dict(item), source=None, mc_type=mc_type_enum, genre_mapping=genre_mapping
            )
            if normalized is None:
                total_skipped += 1
                continue

            redis_key = f"media:{normalized.id}"
            if redis_key in seen_keys:
                continue
            seen_keys.add(redis_key)

            if redis is not None:
                exists = await redis.exists(redis_key)
                if int(cast(int, exists)) == 1:
                    continue

            total_missing += 1

            if apply_updates and redis is not None:
                redis_doc = document_to_redis(normalized)
                pending.append((redis_key, redis_doc))

                if len(pending) >= LOAD_BATCH_SIZE:
                    batch_start = time.time()
                    await _flush_batch(
                        redis, pending, enrich_semaphore,
                        microgenre_semaphore, enrichment_stats,
                    )
                    total_loaded += len(pending)
                    elapsed = time.time() - batch_start
                    logger.info(
                        "  Loaded batch of %d (%.1fs, %.0f/s)",
                        len(pending), elapsed,
                        len(pending) / elapsed if elapsed > 0 else 0,
                    )
                    pending = []

        if pending and apply_updates and redis is not None:
            batch_start = time.time()
            await _flush_batch(
                redis, pending, enrich_semaphore,
                microgenre_semaphore, enrichment_stats,
            )
            total_loaded += len(pending)
            elapsed = time.time() - batch_start
            logger.info(
                "  Loaded final batch of %d (%.1fs)",
                len(pending), elapsed,
            )

    try:
        # ── Phase A: scan cached files ────────────────────────────────
        logger.info("═" * 60)
        logger.info("Phase A: Scanning cached 2026 files")
        logger.info("═" * 60)

        for media_type in ("movie", "tv"):
            cache_dir = data_root / media_type
            if not cache_dir.exists():
                continue

            for path in sorted(cache_dir.glob(f"tmdb_{media_type}_2026_*.json")):
                parsed = _parse_cache_file(path)
                if parsed is None:
                    continue
                _, year, month = parsed
                if year != TARGET_YEAR:
                    continue

                cached_months[media_type].add(month)

                try:
                    raw = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    logger.warning("Skipping unreadable file: %s", path)
                    continue

                records = raw.get("results", [])
                if not records or not isinstance(records, list):
                    continue

                logger.info("Scanning %s: %d records", path.name, len(records))
                await _collect_and_load(records, media_type, genre_mapping)

        logger.info(
            "Phase A done — candidates=%d missing=%d loaded=%d",
            total_candidates, total_missing, total_loaded,
        )

        # ── Phase B: Discover API for uncached months ─────────────────
        today = date.today()
        current_month = today.month if today.year == TARGET_YEAR else 12

        uncached_movie = sorted(set(range(1, current_month + 1)) - cached_months["movie"])
        uncached_tv = sorted(set(range(1, current_month + 1)) - cached_months["tv"])

        needs_discover = bool(uncached_movie or uncached_tv)
        if not needs_discover:
            logger.info("All 2026 months covered by cache — skipping Phase B")
        else:
            logger.info("═" * 60)
            logger.info("Phase B: Discover API for uncached months")
            logger.info("  movie months to discover: %s", uncached_movie or "(none)")
            logger.info("  tv months to discover: %s", uncached_tv or "(none)")
            logger.info("═" * 60)

            service = BackfillDiscoverService()

            for media_type, months in [("movie", uncached_movie), ("tv", uncached_tv)]:
                mc_type_enum = MCType.TV_SERIES if media_type == "tv" else MCType.MOVIE

                for month in months:
                    date_gte, date_lte = _month_range(TARGET_YEAR, month)
                    logger.info("Discovering %s for %04d-%02d", media_type, TARGET_YEAR, month)

                    tmdb_ids = await service.discover_ids(media_type, date_gte, date_lte)
                    if not tmdb_ids:
                        continue

                    if not apply_updates:
                        logger.info(
                            "[DRY RUN] Would enrich+filter %d %s titles for %04d-%02d",
                            len(tmdb_ids), media_type, TARGET_YEAR, month,
                        )
                        continue

                    enriched = await _enrich_ids(service, tmdb_ids, mc_type_enum)
                    await _collect_and_load(enriched, media_type, genre_mapping)

    finally:
        if redis is not None:
            await redis.aclose()

    logger.info("═" * 60)
    logger.info("Backfill Summary")
    logger.info("═" * 60)
    logger.info("  Cached months movie: %s", sorted(cached_months["movie"]) or "(none)")
    logger.info("  Cached months tv:    %s", sorted(cached_months["tv"]) or "(none)")
    logger.info("  Total candidates:    %d", total_candidates)
    logger.info("  Missing from index:  %d", total_missing)
    logger.info("  Loaded:              %d", total_loaded)
    logger.info("  Skipped (normalize): %d", total_skipped)
    logger.info("  Microgenres generated:  %d", enrichment_stats["microgenres_generated"])
    logger.info("  Microgenres preserved:  %d", enrichment_stats["microgenres_preserved"])
    logger.info("  Microgenres failed:     %d", enrichment_stats["microgenres_failed"])
    logger.info("  Mode:                %s", "apply" if apply_updates else "dry-run")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill 2026 major-provider media missing from Redis",
    )
    parser.add_argument(
        "--data-root",
        type=str,
        default=str(Path(__file__).resolve().parents[1] / "data" / "us"),
        help="Path to cache root (default: data/us)",
    )
    parser.add_argument(
        "--redis-host",
        type=str,
        default=os.getenv("REDIS_HOST", "localhost"),
        help="Redis host",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=int(os.getenv("REDIS_PORT", "6380")),
        help="Redis port",
    )
    parser.add_argument(
        "--redis-password",
        type=str,
        default=os.getenv("REDIS_PASSWORD"),
        help="Redis password",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write missing titles to Redis (default: dry run)",
    )
    args = parser.parse_args()

    data_root = Path(args.data_root)
    if not data_root.exists():
        raise FileNotFoundError(f"Data root not found: {data_root}")

    asyncio.run(
        run_backfill(
            data_root=data_root,
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            redis_password=args.redis_password,
            apply_updates=args.apply,
        )
    )


if __name__ == "__main__":
    main()
