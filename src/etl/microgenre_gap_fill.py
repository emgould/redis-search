"""Scan Redis for missing microgenres, classify, write Redis, push Media Manager."""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal

from redis.asyncio import Redis

from adapters.media_manager_client import MediaManagerClient
from ai.microgenre_batch import build_microgenre_input_from_document
from ai.microgenre_batch_models import (
    DEFAULT_BATCH_CONCURRENCY,
    MAX_BATCH_CONCURRENCY,
    MicroGenreBatchSidecarRecord,
)
from ai.microgenre_document import (
    microgenre_result_to_redis,
    microgenre_sidecar_to_redis,
    valid_microgenres_value,
)
from ai.prompts.microgenre_classifier import (
    MICROGENRE_REASONING_EFFORT,
    MicroGenreProvider,
    score_microgenres,
)
from etl.media_manager_filter import passes_media_manager_filter
from utils.get_logger import get_logger

logger = get_logger(__name__)

DEFAULT_DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "microgenre-classifications" / "april_25_2026"
DEFAULT_PATHS = {
    "tv": DEFAULT_DATA_DIR / "microgenre-results-tv.jsonl",
    "movie": DEFAULT_DATA_DIR / "microgenre-results-movie.jsonl",
}
WRITE_BATCH_SIZE = 500
SCAN_COUNT = 1000
MGET_BATCH_SIZE = 500
MM_BATCH_SIZE = 100
DEFAULT_CONCURRENCY = DEFAULT_BATCH_CONCURRENCY
# Cerebras pay-as-you-go is capped at 1.5M tokens/minute. This prompt is about
# 11k tokens per call, so ~4 in flight saturates that cap at a 1.5s call.
# 150 in flight submits the whole minute budget at once and returns 429s.
DEFAULT_CEREBRAS_CONCURRENCY = 4
RECENT_RELEASE_YEARS = 3


@dataclass
class BackfillStats:
    scanned_rows: int = 0
    successful_rows: int = 0
    skipped_error_rows: int = 0
    skipped_media_type: int = 0
    malformed_rows: int = 0
    missing_docs: int = 0
    already_has_microgenres: int = 0
    updated: int = 0
    dry_run_updates: int = 0
    mm_submitted: int = 0
    mm_queued: int = 0
    mm_skipped: int = 0
    mm_filtered: int = 0
    mm_errors: int = 0


@dataclass
class MissingStats:
    keys_seen: int = 0
    scanned_docs: int = 0
    already_has_microgenres: int = 0
    skipped_ineligible: int = 0
    candidates: int = 0
    classified: int = 0
    failed: int = 0
    unresolved: int = 0
    updated: int = 0
    dry_run_updates: int = 0
    mm_submitted: int = 0
    mm_queued: int = 0
    mm_skipped: int = 0
    mm_filtered: int = 0
    mm_errors: int = 0
    sample_mc_ids: list[str] = field(default_factory=list)


def _selected_types(mc_type: str) -> set[str]:
    return {"tv", "movie"} if mc_type == "both" else {mc_type}


def _resolve_concurrency(requested: int | None, llm_provider: MicroGenreProvider) -> int:
    if requested is None:
        requested = (
            DEFAULT_CEREBRAS_CONCURRENCY
            if llm_provider == "cerebras"
            else DEFAULT_CONCURRENCY
        )
    return int(max(1, min(requested, MAX_BATCH_CONCURRENCY)))


def _json_doc(raw: object) -> dict[str, Any] | None:
    """Parse RedisJSON MGET payloads (string / list-wrapped / dict)."""
    if isinstance(raw, str):
        try:
            return _json_doc(json.loads(raw))
        except json.JSONDecodeError:
            return None
    if isinstance(raw, list):
        if not raw:
            return None
        return _json_doc(raw[0])
    if isinstance(raw, dict):
        return raw
    return None


def _doc_media_type(doc: dict[str, Any]) -> Literal["movie", "tv"] | None:
    value = doc.get("mc_type")
    if value == "tv":
        return "tv"
    if value == "movie":
        return "movie"
    return None


def _has_valid_microgenres(value: object) -> bool:
    return valid_microgenres_value(value) is not None


def _has_populated_microgenres(value: object) -> bool:
    """True when microgenres exist and at least one score is present (gap-fill complete)."""
    valid = valid_microgenres_value(value)
    if valid is None:
        return False
    return len(valid["microgenre_scores"]) > 0


def microgenres_ready_for_redis(microgenres: dict[str, object]) -> bool:
    """True when a classifier result should be persisted (at least one genre score)."""
    scores = microgenres.get("microgenre_scores")
    if not isinstance(scores, dict):
        return False
    return len(scores) > 0


def _score_map_is_empty(raw: object) -> bool:
    """True only for an empty score object. Missing and populated maps are not empty."""
    if isinstance(raw, str):
        cleaned = raw.strip()
        if not cleaned or cleaned == "null":
            return False
        try:
            raw = json.loads(cleaned)
        except json.JSONDecodeError:
            return False
    return isinstance(raw, dict) and len(raw) == 0


def _aggregate_row(row: object) -> dict[str, str]:
    if not isinstance(row, list):
        return {}
    parsed: dict[str, str] = {}
    for index in range(0, len(row) - 1, 2):
        key = row[index]
        value = row[index + 1]
        if isinstance(key, str) and isinstance(value, str):
            parsed[key] = value
    return parsed


def _parse_iso_date(value: object) -> date | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if len(cleaned) < 10:
        return None
    try:
        return date.fromisoformat(cleaned[:10])
    except ValueError:
        return None


def _numeric_rt_score(value: object) -> float | None:
    """Return a parseable RT score, or None when absent/invalid."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().removesuffix("%")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _has_rt_rating(doc: dict[str, Any]) -> bool:
    return (
        _numeric_rt_score(doc.get("rt_audience_score")) is not None
        or _numeric_rt_score(doc.get("rt_critics_score")) is not None
    )


def _cutoff_date(years_back: int, *, today: date | None = None) -> date:
    today = today or date.today()
    try:
        return today.replace(year=today.year - years_back)
    except ValueError:
        # Feb 29 on non-leap target year
        return today.replace(month=2, day=28, year=today.year - years_back)


def _primary_release_date(
    doc: dict[str, Any],
    media_type: Literal["movie", "tv"],
) -> date | None:
    if media_type == "tv":
        return _parse_iso_date(doc.get("first_air_date")) or _parse_iso_date(
            doc.get("release_date")
        )
    return _parse_iso_date(doc.get("release_date")) or _parse_iso_date(doc.get("first_air_date"))


def _is_recent_release(
    doc: dict[str, Any],
    media_type: Literal["movie", "tv"],
    *,
    years_back: int = RECENT_RELEASE_YEARS,
    today: date | None = None,
) -> bool:
    parsed = _primary_release_date(doc, media_type)
    if parsed is None:
        return False
    return parsed >= _cutoff_date(years_back, today=today)


def eligible_for_microgenre_backfill(
    doc: dict[str, Any],
    media_type: Literal["movie", "tv"],
    *,
    years_back: int = RECENT_RELEASE_YEARS,
    today: date | None = None,
) -> bool:
    """Gap-fill eligibility: RT score present, or release/first-air within lookback."""
    return _has_rt_rating(doc) or _is_recent_release(
        doc, media_type, years_back=years_back, today=today
    )


def _unwrap_path_value(value: object) -> object:
    """Normalize JSON.MGET path results that may be wrapped in a list."""
    if isinstance(value, str):
        try:
            return _unwrap_path_value(json.loads(value))
        except json.JSONDecodeError:
            return value
    if isinstance(value, list):
        if not value:
            return None
        return _unwrap_path_value(value[0])
    return value


def load_sidecar_rows(
    paths: list[Path],
    mc_type: str,
    limit: int | None,
) -> tuple[list[MicroGenreBatchSidecarRecord], BackfillStats]:
    stats = BackfillStats()
    rows: list[MicroGenreBatchSidecarRecord] = []
    selected_types = _selected_types(mc_type)

    for path in paths:
        if not path.exists():
            raise FileNotFoundError(f"Microgenre sidecar does not exist: {path}")
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                stats.scanned_rows += 1
                try:
                    row = MicroGenreBatchSidecarRecord(**json.loads(line))
                except (json.JSONDecodeError, ValueError) as exc:
                    stats.malformed_rows += 1
                    logger.warning("Skipping malformed row %s:%d: %s", path, line_number, exc)
                    continue

                if row.media_type not in selected_types:
                    stats.skipped_media_type += 1
                    continue
                if row.error is not None or row.classification is None:
                    stats.skipped_error_rows += 1
                    continue
                stats.successful_rows += 1
                rows.append(row)
                if limit is not None and len(rows) >= limit:
                    return rows, stats
    return rows, stats


async def _flush_mm_batch(
    mm_client: MediaManagerClient | None,
    docs: list[dict[str, Any]],
    stats: BackfillStats | MissingStats,
    dry_run: bool,
) -> None:
    if mm_client is None or not docs:
        return

    eligible: list[dict[str, Any]] = []
    for doc in docs:
        passed, _reason = passes_media_manager_filter(doc)
        if passed:
            eligible.append(doc)
        else:
            stats.mm_filtered += 1

    for idx in range(0, len(eligible), MM_BATCH_SIZE):
        batch = eligible[idx : idx + MM_BATCH_SIZE]
        stats.mm_submitted += len(batch)
        try:
            response = await mm_client.insert_docs(
                batch,
                dry_run=dry_run,
                metadata_only=True,
            )
            stats.mm_queued += response["queued"]
            stats.mm_skipped += response["skipped"]
            errors = response.get("errors", [])
            if errors:
                stats.mm_errors += len(errors)
                for err in errors:
                    logger.warning("Media Manager batch error: %s", err)
        except Exception as exc:
            stats.mm_errors += len(batch)
            logger.warning("Media Manager batch failed: %s", exc)


async def backfill_rows(
    redis: Redis,  # type: ignore[type-arg]
    rows: list[MicroGenreBatchSidecarRecord],
    stats: BackfillStats,
    dry_run: bool,
    force: bool,
    push_to_mm: bool = False,
) -> BackfillStats:
    now_ts = int(datetime.now(UTC).timestamp())
    mm_client: MediaManagerClient | None = None
    if push_to_mm and os.getenv("MEDIA_MANAGER_API_URL"):
        mm_client = MediaManagerClient()
        await mm_client.health_check()
    elif push_to_mm:
        logger.warning("MEDIA_MANAGER_API_URL unset; skipping Media Manager push")

    mm_buffer: list[dict[str, Any]] = []
    try:
        for batch_start in range(0, len(rows), WRITE_BATCH_SIZE):
            batch = rows[batch_start : batch_start + WRITE_BATCH_SIZE]
            keys = [f"media:{row.mc_id}" for row in batch]
            existing_micro = await redis.execute_command("JSON.MGET", *keys, "$.microgenres")
            if not isinstance(existing_micro, list):
                existing_micro = [None] * len(batch)
            if len(existing_micro) != len(batch):
                existing_micro = [
                    *existing_micro[: len(batch)],
                    *([None] * (len(batch) - len(existing_micro))),
                ]

            full_docs_raw = await redis.execute_command("JSON.MGET", *keys, "$")
            if not isinstance(full_docs_raw, list):
                full_docs_raw = [None] * len(batch)

            pipe = redis.pipeline()
            write_count = 0
            for key, row, existing, full_raw in zip(
                keys, batch, existing_micro, full_docs_raw, strict=False
            ):
                microgenres = microgenre_sidecar_to_redis(row)
                if microgenres is None:
                    stats.skipped_error_rows += 1
                    continue

                full_doc = _json_doc(full_raw)
                if full_doc is None:
                    stats.missing_docs += 1
                    continue

                if not force and _has_valid_microgenres(_unwrap_path_value(existing)):
                    stats.already_has_microgenres += 1
                    continue

                if dry_run:
                    stats.dry_run_updates += 1
                    updated_doc = dict(full_doc)
                    updated_doc["microgenres"] = microgenres
                    mm_buffer.append(updated_doc)
                    continue

                pipe.json().set(key, "$.microgenres", microgenres)
                pipe.json().set(key, "$.modified_at", now_ts)
                write_count += 1
                updated_doc = dict(full_doc)
                updated_doc["microgenres"] = microgenres
                updated_doc["modified_at"] = now_ts
                mm_buffer.append(updated_doc)

            if write_count:
                await pipe.execute()
                stats.updated += write_count

            while len(mm_buffer) >= MM_BATCH_SIZE:
                chunk = mm_buffer[:MM_BATCH_SIZE]
                del mm_buffer[:MM_BATCH_SIZE]
                await _flush_mm_batch(mm_client, chunk, stats, dry_run)

            logger.info(
                "Sidecar progress: %d/%d (updated=%d dry_run=%d missing=%d existing=%d)",
                min(batch_start + len(batch), len(rows)),
                len(rows),
                stats.updated,
                stats.dry_run_updates,
                stats.missing_docs,
                stats.already_has_microgenres,
            )

        if mm_buffer:
            await _flush_mm_batch(mm_client, mm_buffer, stats, dry_run)
    finally:
        if mm_client is not None:
            await mm_client.close()

    return stats


async def collect_missing_docs(
    redis: Redis,  # type: ignore[type-arg]
    mc_type: str,
    force: bool,
    limit: int | None,
    *,
    all_missing: bool = False,
) -> tuple[list[tuple[str, dict[str, Any]]], MissingStats]:
    """Stream SCAN + MGET; stop as soon as ``limit`` missing candidates are found."""
    stats = MissingStats()
    selected = _selected_types(mc_type)
    candidates: list[tuple[str, dict[str, Any]]] = []
    batch_keys: list[str] = []
    last_log_scanned = 0
    keys_seen = 0
    parse_failures = 0
    eligibility = "all-missing" if all_missing else f"RT-or-last-{RECENT_RELEASE_YEARS}y"

    logger.info(
        "Scanning media:* for missing microgenres (mc_type=%s limit=%s eligibility=%s)...",
        mc_type,
        "none" if limit is None else str(limit),
        eligibility,
    )

    async def _consume_batch(keys: list[str]) -> bool:
        """Process one MGET batch. Returns True when candidate limit is reached."""
        nonlocal last_log_scanned, keys_seen, parse_failures
        if not keys:
            return False
        keys_seen += len(keys)
        stats.keys_seen = keys_seen
        raw_docs = await redis.execute_command("JSON.MGET", *keys, "$")
        if not isinstance(raw_docs, list):
            parse_failures += len(keys)
            return False
        for key, raw_doc in zip(keys, raw_docs, strict=False):
            doc = _json_doc(raw_doc)
            if doc is None:
                parse_failures += 1
                continue
            stats.scanned_docs += 1
            media_type = _doc_media_type(doc)
            if media_type is None or media_type not in selected:
                continue
            if not force and _has_populated_microgenres(doc.get("microgenres")):
                stats.already_has_microgenres += 1
                continue
            if not all_missing and not eligible_for_microgenre_backfill(doc, media_type):
                stats.skipped_ineligible += 1
                continue
            candidates.append((key, doc))
            if len(stats.sample_mc_ids) < 20:
                mc_id = doc.get("mc_id") or doc.get("id")
                if isinstance(mc_id, str):
                    stats.sample_mc_ids.append(mc_id)
            if limit is not None and len(candidates) >= limit:
                stats.candidates = len(candidates)
                logger.info(
                    "Scan stopping early: %d gap candidates (limit=%d) after keys=%d "
                    "movie/tv_docs=%d",
                    len(candidates),
                    limit,
                    keys_seen,
                    stats.scanned_docs,
                )
                return True

        if stats.scanned_docs - last_log_scanned >= 5000:
            logger.info(
                "Scan progress: keys=%d scanned=%d already_populated=%d "
                "ineligible=%d missing=%d parse_fail=%d",
                keys_seen,
                stats.scanned_docs,
                stats.already_has_microgenres,
                stats.skipped_ineligible,
                len(candidates),
                parse_failures,
            )
            last_log_scanned = stats.scanned_docs
        return False

    async for raw_key in redis.scan_iter(match="media:*", count=SCAN_COUNT):
        batch_keys.append(str(raw_key))
        if len(batch_keys) < MGET_BATCH_SIZE:
            continue
        done = await _consume_batch(batch_keys)
        batch_keys = []
        if done:
            return candidates, stats

    if await _consume_batch(batch_keys):
        return candidates, stats

    stats.candidates = len(candidates)
    return candidates, stats


def _index_type_query(mc_type: str) -> str:
    if mc_type == "movie":
        return "@mc_type:{movie}"
    if mc_type == "tv":
        return "@mc_type:{tv}"
    return "@mc_type:{movie|tv}"


async def collect_empty_score_docs(
    redis: Redis,  # type: ignore[type-arg]
    mc_type: str,
    limit: int | None,
) -> tuple[list[tuple[str, dict[str, Any]]], MissingStats]:
    """Read idx:media and keep documents whose microgenre_scores object is empty."""
    stats = MissingStats()
    empty_ids: list[str] = []
    cursor: object = None
    page: object = None

    logger.info(
        "Querying idx:media for empty microgenre_scores (mc_type=%s limit=%s)...",
        mc_type,
        "none" if limit is None else str(limit),
    )

    while True:
        if cursor is None:
            response = await redis.execute_command(
                "FT.AGGREGATE",
                "idx:media",
                _index_type_query(mc_type),
                "LOAD",
                "2",
                "$.mc_id",
                "$.microgenres.microgenre_scores",
                "WITHCURSOR",
                "COUNT",
                "2000",
            )
        else:
            response = await redis.execute_command(
                "FT.CURSOR",
                "READ",
                "idx:media",
                str(cursor),
                "COUNT",
                "2000",
            )
        if not isinstance(response, list) or len(response) != 2:
            break
        page, cursor = response
        rows = page[1:] if isinstance(page, list) else []
        for row in rows:
            item = _aggregate_row(row)
            mc_id = item.get("$.mc_id")
            if mc_id is None:
                continue
            stats.scanned_docs += 1
            if not _score_map_is_empty(item.get("$.microgenres.microgenre_scores")):
                stats.already_has_microgenres += 1
                continue
            empty_ids.append(mc_id)
            if len(stats.sample_mc_ids) < 20:
                stats.sample_mc_ids.append(mc_id)
            if limit is not None and len(empty_ids) >= limit:
                cursor = 0
                break
        if stats.scanned_docs and stats.scanned_docs % 20000 < 2000:
            logger.info(
                "Index progress: scanned=%d populated=%d empty=%d",
                stats.scanned_docs,
                stats.already_has_microgenres,
                len(empty_ids),
            )
        if not cursor or cursor == "0" or cursor == 0:
            break

    candidates: list[tuple[str, dict[str, Any]]] = []
    for offset in range(0, len(empty_ids), MGET_BATCH_SIZE):
        chunk = empty_ids[offset : offset + MGET_BATCH_SIZE]
        keys = [f"media:{mc_id}" for mc_id in chunk]
        raw_docs = await redis.execute_command("JSON.MGET", *keys, "$")
        if not isinstance(raw_docs, list):
            continue
        for key, raw_doc in zip(keys, raw_docs, strict=False):
            doc = _json_doc(raw_doc)
            if doc is None:
                continue
            microgenres = doc.get("microgenres")
            if not isinstance(microgenres, dict) or not _score_map_is_empty(
                microgenres.get("microgenre_scores")
            ):
                continue
            candidates.append((key, doc))

    stats.candidates = len(candidates)
    return candidates, stats


async def fill_missing(
    redis: Redis,  # type: ignore[type-arg]
    candidates: list[tuple[str, dict[str, Any]]],
    stats: MissingStats,
    dry_run: bool,
    concurrency: int,
    push_to_mm: bool,
    enable_web_search: bool,
    llm_provider: MicroGenreProvider,
    media_manager_client: MediaManagerClient | None = None,
) -> MissingStats:
    if dry_run:
        stats.dry_run_updates = len(candidates)
        return stats

    now_ts = int(datetime.now(UTC).timestamp())
    concurrency = max(1, min(concurrency, MAX_BATCH_CONCURRENCY))
    # Classify in small waves so progress logs appear frequently.
    classify_wave = max(concurrency * 2, 10)
    semaphore = asyncio.Semaphore(concurrency)
    owns_mm_client = False
    mm_client = media_manager_client
    if mm_client is None and push_to_mm and os.getenv("MEDIA_MANAGER_API_URL"):
        mm_client = MediaManagerClient()
        await mm_client.health_check()
        owns_mm_client = True
    elif push_to_mm and mm_client is None:
        logger.warning("MEDIA_MANAGER_API_URL unset; skipping Media Manager push")

    mm_buffer: list[dict[str, Any]] = []
    classify_started = time.time()
    attempted = 0

    logger.info(
        "Starting classification of %d candidates "
        "(llm=%s concurrency=%d wave=%d web_search=%s%s). "
        "First progress log after ~%d completions.",
        len(candidates),
        llm_provider,
        concurrency,
        classify_wave,
        enable_web_search,
        (
            f" reasoning_effort={MICROGENRE_REASONING_EFFORT}"
            if llm_provider == "openai"
            else ""
        ),
        classify_wave,
    )

    async def _classify_one(
        key: str, doc: dict[str, Any]
    ) -> tuple[str, dict[str, Any], dict[str, Any]] | None:
        media_type = _doc_media_type(doc)
        if media_type is None:
            return None
        async with semaphore:
            try:
                classifier_input = build_microgenre_input_from_document(
                    doc,
                    media_type,
                    score_threshold=0.1,
                    # Backfill defaults web search off; --web-search re-enables date gate.
                    enable_web_search=None if enable_web_search else False,
                )
                response = await score_microgenres(
                    classifier_input,
                    provider=llm_provider,
                )
            except Exception as exc:
                stats.failed += 1
                logger.warning(
                    "Microgenre classification raised for %s: %s",
                    doc.get("mc_id") or doc.get("id"),
                    exc,
                )
                return None

        if response.error is not None or response.result is None:
            stats.failed += 1
            logger.warning(
                "Microgenre classification failed for %s: %s",
                doc.get("mc_id") or doc.get("id"),
                response.error or "no result",
            )
            return None

        result = response.result
        microgenres = microgenre_result_to_redis(result)
        if not microgenres_ready_for_redis(microgenres):
            stats.unresolved += 1
            mc = doc.get("mc_id") or doc.get("id")
            if result.unknown:
                logger.info(
                    "Microgenre unresolved for %s: classifier unknown (%s); microgenre_scores={}",
                    mc,
                    result.unknown_reason or "no reason",
                )
            elif not enable_web_search and llm_provider == "openai":
                logger.info(
                    "Microgenre unresolved for %s: microgenre_scores={} "
                    "(enable web search: ARGS='--web-search')",
                    mc,
                )
            else:
                logger.info(
                    "Microgenre unresolved for %s: microgenre_scores={} (not a classifier error)",
                    mc,
                )
            return None
        stats.classified += 1
        return key, doc, microgenres

    try:
        for wave_start in range(0, len(candidates), classify_wave):
            wave = candidates[wave_start : wave_start + classify_wave]
            results = await asyncio.gather(*[_classify_one(key, doc) for key, doc in wave])
            attempted += len(wave)

            pipe = redis.pipeline()
            write_count = 0
            for result in results:
                if result is None:
                    continue
                key, doc, microgenres = result
                pipe.json().set(key, "$.microgenres", microgenres)
                pipe.json().set(key, "$.modified_at", now_ts)
                write_count += 1
                updated_doc = dict(doc)
                updated_doc["microgenres"] = microgenres
                updated_doc["modified_at"] = now_ts
                mm_buffer.append(updated_doc)

            if write_count:
                await pipe.execute()
                stats.updated += write_count

            while len(mm_buffer) >= MM_BATCH_SIZE:
                chunk = mm_buffer[:MM_BATCH_SIZE]
                del mm_buffer[:MM_BATCH_SIZE]
                await _flush_mm_batch(mm_client, chunk, stats, dry_run=False)

            elapsed = max(time.time() - classify_started, 0.001)
            rate = attempted / elapsed
            remaining = len(candidates) - attempted
            eta_s = remaining / rate if rate > 0 else 0.0
            logger.info(
                "Missing-fill progress: %d/%d (updated=%d unresolved=%d failed=%d "
                "classified=%d mm_queued=%d rate=%.2f/s eta=%.1fmin)",
                attempted,
                len(candidates),
                stats.updated,
                stats.unresolved,
                stats.failed,
                stats.classified,
                stats.mm_queued,
                rate,
                eta_s / 60.0,
            )

        if mm_buffer:
            await _flush_mm_batch(mm_client, mm_buffer, stats, dry_run=False)
    finally:
        if owns_mm_client and mm_client is not None:
            await mm_client.close()

    return stats


async def maybe_finalize_mm_publish(push_to_mm: bool, dry_run: bool, finalize: bool) -> None:
    if dry_run or not push_to_mm or not finalize:
        return
    if not os.getenv("MEDIA_MANAGER_API_URL"):
        return
    mm_client = MediaManagerClient()
    try:
        await mm_client.health_check()
        logger.info("Polling Media Manager queue before finalize-publish...")
        await mm_client.poll_until_drained()
        resp = await mm_client.finalize_publish()
        logger.info(
            "Finalize-publish complete: status=%s metadata_only_updated=%s",
            resp["status"],
            resp["metadata_only_updated"],
        )
    finally:
        await mm_client.close()


async def run_missing_gap_fill(
    redis_host: str,
    redis_port: int,
    redis_password: str | None,
    *,
    mc_type: str = "both",
    all_missing: bool = True,
    limit: int | None = None,
    concurrency: int | None = None,
    llm_provider: MicroGenreProvider = "openai",
    enable_web_search: bool = True,
    push_to_mm: bool = True,
    media_manager_client: MediaManagerClient | None = None,
) -> MissingStats:
    """Scan Redis for missing microgenres, classify, write Redis, push MM metadata."""
    redis = Redis(
        host=redis_host,
        port=redis_port,
        password=redis_password,
        decode_responses=True,
    )
    try:
        await redis.ping()  # type: ignore[misc]
        candidates, stats = await collect_missing_docs(
            redis,
            mc_type,
            force=False,
            limit=limit,
            all_missing=all_missing,
        )
        logger.info(
            "Microgenre gap scan: scanned=%d already_populated=%d ineligible=%d candidates=%d",
            stats.scanned_docs,
            stats.already_has_microgenres,
            stats.skipped_ineligible,
            stats.candidates,
        )
        if stats.sample_mc_ids:
            logger.info("Sample missing mc_ids: %s", ", ".join(stats.sample_mc_ids))
        if not candidates:
            return stats
        resolved_concurrency = _resolve_concurrency(concurrency, llm_provider)
        return await fill_missing(
            redis,
            candidates,
            stats,
            dry_run=False,
            concurrency=resolved_concurrency,
            push_to_mm=push_to_mm,
            enable_web_search=enable_web_search,
            llm_provider=llm_provider,
            media_manager_client=media_manager_client,
        )
    finally:
        await redis.aclose()
