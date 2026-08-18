#!/usr/bin/env python3
"""Backfill compact microgenre metadata onto existing Redis media documents.

Two modes:

1. ``missing`` (default) — SCAN ``media:*`` for movie/tv docs with null/missing
   ``microgenres`` that are eligible (have an RT score, or release/first-air
   within the last 3 years), classify via the live LLM, write Redis, and push
   updated docs to Media Manager with ``metadata_only=True``.

2. ``sidecar`` — apply precomputed JSONL classifications onto Redis (no LLM).
   Optionally push updated docs to Media Manager (``metadata_only=True``).

Usage:
    make backfill-microgenres REDIS=dev ARGS='--dry-run --limit 100'
    make backfill-microgenres REDIS=dev ARGS='--limit 50 --concurrency 3'
    make backfill-microgenres REDIS=dev ARGS='--llm openai --limit 50'
    make backfill-microgenres REDIS=dev ARGS='--mode sidecar --dry-run'
    make backfill-microgenres REDIS=dev ARGS='--no-push-to-mm --limit 20'
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal, cast

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from adapters.config import load_env  # noqa: E402

load_env()

from redis.asyncio import Redis  # noqa: E402

from adapters.media_manager_client import MediaManagerClient  # noqa: E402
from ai.microgenre_batch import build_microgenre_input_from_document  # noqa: E402
from ai.microgenre_batch_models import (  # noqa: E402
    DEFAULT_BATCH_CONCURRENCY,
    MAX_BATCH_CONCURRENCY,
    MicroGenreBatchSidecarRecord,
)
from ai.microgenre_document import (  # noqa: E402
    microgenre_result_to_redis,
    microgenre_sidecar_to_redis,
    valid_microgenres_value,
)
from ai.prompts.microgenre_classifier import (  # noqa: E402
    MicroGenreProvider,
    score_microgenres,
)
from etl.media_manager_filter import passes_media_manager_filter  # noqa: E402
from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)

DEFAULT_DATA_DIR = _project_root / "data" / "microgenre-classifications" / "april_25_2026"
DEFAULT_PATHS = {
    "tv": DEFAULT_DATA_DIR / "microgenre-results-tv.jsonl",
    "movie": DEFAULT_DATA_DIR / "microgenre-results-movie.jsonl",
}
WRITE_BATCH_SIZE = 500
SCAN_COUNT = 1000
MGET_BATCH_SIZE = 500
MM_BATCH_SIZE = 100
DEFAULT_CONCURRENCY = DEFAULT_BATCH_CONCURRENCY
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
    scanned_docs: int = 0
    already_has_microgenres: int = 0
    skipped_ineligible: int = 0
    candidates: int = 0
    classified: int = 0
    failed: int = 0
    updated: int = 0
    dry_run_updates: int = 0
    mm_submitted: int = 0
    mm_queued: int = 0
    mm_skipped: int = 0
    mm_filtered: int = 0
    mm_errors: int = 0
    sample_mc_ids: list[str] = field(default_factory=list)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill media:* microgenres. Default mode scans Redis for missing "
            "microgenres on titles with an RT score or release/first-air within "
            "the last 3 years, classifies them, writes Redis, and pushes Media "
            "Manager metadata updates."
        )
    )
    parser.add_argument(
        "--mode",
        choices=["missing", "sidecar"],
        default="missing",
        help="missing=scan Redis gaps + classify (default); sidecar=apply JSONL only",
    )
    parser.add_argument("--mc-type", choices=["tv", "movie", "both"], default="both")
    parser.add_argument("--tv-path", type=Path, default=DEFAULT_PATHS["tv"])
    parser.add_argument("--movie-path", type=Path, default=DEFAULT_PATHS["movie"])
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reclassify/overwrite docs that already have valid microgenres.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=(
            f"Max concurrent LLM classifications in missing mode "
            f"(default {DEFAULT_CONCURRENCY}, max {MAX_BATCH_CONCURRENCY}; "
            "same scale as microgenre-batch)."
        ),
    )
    parser.add_argument(
        "--push-to-mm",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Push updated docs to Media Manager (metadata_only). Default: on.",
    )
    parser.add_argument(
        "--web-search",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Enable OpenAI web_search for titles on/after 2025-06-01. "
            "Default: off (Redis overview/genres/keywords are enough; web search is expensive). "
            "Incompatible with --llm cerebras."
        ),
    )
    parser.add_argument(
        "--llm",
        choices=["openai", "cerebras"],
        default="cerebras",
        help=(
            "Classifier backend for missing mode: cerebras (gpt-oss-120b chat "
            "completions, default) or openai (gpt-5.6-terra Responses API)."
        ),
    )
    parser.add_argument(
        "--finalize",
        action="store_true",
        help="After MM pushes, drain the queue and call finalize-publish.",
    )
    parser.add_argument("--redis-host", default=os.getenv("REDIS_HOST", "localhost"))
    parser.add_argument("--redis-port", type=int, default=int(os.getenv("REDIS_PORT", "6380")))
    parser.add_argument("--redis-password", default=os.getenv("REDIS_PASSWORD") or None)
    return parser.parse_args()


def _selected_types(mc_type: str) -> set[str]:
    return {"tv", "movie"} if mc_type == "both" else {mc_type}


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
) -> tuple[list[tuple[str, dict[str, Any]]], MissingStats]:
    """Stream SCAN + MGET; stop as soon as ``limit`` missing candidates are found."""
    stats = MissingStats()
    selected = _selected_types(mc_type)
    candidates: list[tuple[str, dict[str, Any]]] = []
    batch_keys: list[str] = []
    last_log_scanned = 0
    keys_seen = 0
    parse_failures = 0

    logger.info(
        "Scanning media:* for missing microgenres "
        "(mc_type=%s limit=%s eligibility=RT-or-last-%dy)...",
        mc_type,
        "none" if limit is None else str(limit),
        RECENT_RELEASE_YEARS,
    )

    async def _consume_batch(keys: list[str]) -> bool:
        """Process one MGET batch. Returns True when candidate limit is reached."""
        nonlocal last_log_scanned, keys_seen, parse_failures
        if not keys:
            return False
        keys_seen += len(keys)
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
            if not force and _has_valid_microgenres(doc.get("microgenres")):
                stats.already_has_microgenres += 1
                continue
            if not eligible_for_microgenre_backfill(doc, media_type):
                stats.skipped_ineligible += 1
                continue
            candidates.append((key, doc))
            if len(stats.sample_mc_ids) < 20:
                mc_id = doc.get("mc_id") or doc.get("id")
                if isinstance(mc_id, str):
                    stats.sample_mc_ids.append(mc_id)
            if limit is not None and len(candidates) >= limit:
                stats.candidates = len(candidates)
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


async def fill_missing(
    redis: Redis,  # type: ignore[type-arg]
    candidates: list[tuple[str, dict[str, Any]]],
    stats: MissingStats,
    dry_run: bool,
    concurrency: int,
    push_to_mm: bool,
    enable_web_search: bool,
    llm_provider: MicroGenreProvider,
) -> MissingStats:
    if dry_run:
        stats.dry_run_updates = len(candidates)
        return stats

    now_ts = int(datetime.now(UTC).timestamp())
    concurrency = max(1, min(concurrency, MAX_BATCH_CONCURRENCY))
    # Classify in small waves so progress logs appear frequently.
    classify_wave = max(concurrency * 2, 10)
    semaphore = asyncio.Semaphore(concurrency)
    mm_client: MediaManagerClient | None = None
    if push_to_mm and os.getenv("MEDIA_MANAGER_API_URL"):
        mm_client = MediaManagerClient()
        await mm_client.health_check()
    elif push_to_mm:
        logger.warning("MEDIA_MANAGER_API_URL unset; skipping Media Manager push")

    mm_buffer: list[dict[str, Any]] = []
    classify_started = time.time()
    attempted = 0

    logger.info(
        "Starting classification of %d candidates "
        "(llm=%s concurrency=%d wave=%d web_search=%s). "
        "First progress log after ~%d completions.",
        len(candidates),
        llm_provider,
        concurrency,
        classify_wave,
        enable_web_search,
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
                    cast(Literal["movie", "tv"], media_type),
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

        microgenres = microgenre_result_to_redis(response.result)
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
                "Missing-fill progress: %d/%d (classified=%d updated=%d failed=%d "
                "mm_queued=%d rate=%.2f/s eta=%.1fmin)",
                attempted,
                len(candidates),
                stats.classified,
                stats.updated,
                stats.failed,
                stats.mm_queued,
                rate,
                eta_s / 60.0,
            )

        if mm_buffer:
            await _flush_mm_batch(mm_client, mm_buffer, stats, dry_run=False)
    finally:
        if mm_client is not None:
            await mm_client.close()

    return stats


async def _maybe_finalize(push_to_mm: bool, dry_run: bool, finalize: bool) -> None:
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


async def run(args: argparse.Namespace) -> int:
    llm_provider = cast(MicroGenreProvider, args.llm)
    if args.mode == "missing" and args.web_search and llm_provider == "cerebras":
        raise SystemExit("--web-search is only supported with --llm openai")
    if args.mode == "missing" and llm_provider == "cerebras" and not os.getenv("CEREBRAS_API_KEY"):
        raise SystemExit("CEREBRAS_API_KEY is required when --llm cerebras")
    if args.mode == "missing" and llm_provider == "openai" and not os.getenv("OPENAI_API_KEY"):
        raise SystemExit("OPENAI_API_KEY is required when --llm openai")
    if (
        not args.dry_run
        and args.push_to_mm
        and not os.getenv("MEDIA_MANAGER_API_URL")
    ):
        raise SystemExit(
            "MEDIA_MANAGER_API_URL is required unless --no-push-to-mm is specified"
        )

    redis = Redis(
        host=args.redis_host,
        port=args.redis_port,
        password=args.redis_password,
        decode_responses=True,
    )
    start = time.time()
    try:
        await redis.ping()  # type: ignore[misc]

        if args.mode == "sidecar":
            paths: list[Path] = []
            if args.mc_type in ("tv", "both"):
                paths.append(args.tv_path)
            if args.mc_type in ("movie", "both"):
                paths.append(args.movie_path)

            rows, stats = load_sidecar_rows(paths, args.mc_type, args.limit)
            logger.info(
                "Loaded %d successful sidecar rows from %d path(s) "
                "(scanned=%d malformed=%d errors=%d)",
                len(rows),
                len(paths),
                stats.scanned_rows,
                stats.malformed_rows,
                stats.skipped_error_rows,
            )
            if rows:
                stats = await backfill_rows(
                    redis,
                    rows,
                    stats,
                    args.dry_run,
                    args.force,
                    args.push_to_mm,
                )
            if stats.mm_errors == 0:
                await _maybe_finalize(args.push_to_mm, args.dry_run, args.finalize)
            elif args.finalize:
                logger.error(
                    "Skipping finalize-publish because %d Media Manager errors occurred",
                    stats.mm_errors,
                )
            elapsed = time.time() - start
            logger.info("=" * 60)
            logger.info("Microgenre Sidecar Backfill Summary")
            logger.info("=" * 60)
            logger.info("  Scanned rows:       %d", stats.scanned_rows)
            logger.info("  Successful rows:    %d", stats.successful_rows)
            logger.info("  Updated:            %d", stats.updated)
            logger.info("  Dry-run updates:    %d", stats.dry_run_updates)
            logger.info("  Already populated:  %d", stats.already_has_microgenres)
            logger.info("  Missing docs:       %d", stats.missing_docs)
            logger.info("  MM submitted:       %d", stats.mm_submitted)
            logger.info("  MM queued:          %d", stats.mm_queued)
            logger.info("  MM filtered:        %d", stats.mm_filtered)
            logger.info("  MM errors:          %d", stats.mm_errors)
            logger.info("  Duration:           %.2fs", elapsed)
            return 0 if stats.malformed_rows == 0 and stats.mm_errors == 0 else 1

        candidates, stats = await collect_missing_docs(redis, args.mc_type, args.force, args.limit)
        logger.info(
            "Redis scan complete: scanned=%d already_populated=%d ineligible=%d missing_candidates=%d",
            stats.scanned_docs,
            stats.already_has_microgenres,
            stats.skipped_ineligible,
            stats.candidates,
        )
        if stats.scanned_docs == 0:
            logger.warning(
                "No media documents parsed. Check RedisJSON MGET decoding / tunnel target."
            )
        if stats.sample_mc_ids:
            logger.info("Sample missing mc_ids: %s", ", ".join(stats.sample_mc_ids))

        stats = await fill_missing(
            redis,
            candidates,
            stats,
            args.dry_run,
            args.concurrency,
            args.push_to_mm,
            args.web_search,
            llm_provider,
        )
        if stats.mm_errors == 0:
            await _maybe_finalize(args.push_to_mm, args.dry_run, args.finalize)
        elif args.finalize:
            logger.error(
                "Skipping finalize-publish because %d Media Manager errors occurred",
                stats.mm_errors,
            )
        elapsed = time.time() - start

        logger.info("=" * 60)
        logger.info("Microgenre Missing-Fill Summary")
        logger.info("=" * 60)
        logger.info("  LLM provider:       %s", llm_provider)
        logger.info("  Scanned docs:       %d", stats.scanned_docs)
        logger.info("  Already populated:  %d", stats.already_has_microgenres)
        logger.info("  Ineligible skipped: %d", stats.skipped_ineligible)
        logger.info("  Candidates:         %d", stats.candidates)
        logger.info("  Classified:         %d", stats.classified)
        logger.info("  Updated:            %d", stats.updated)
        logger.info("  Dry-run updates:    %d", stats.dry_run_updates)
        logger.info("  Failed:             %d", stats.failed)
        logger.info("  MM submitted:       %d", stats.mm_submitted)
        logger.info("  MM queued:          %d", stats.mm_queued)
        logger.info("  MM filtered:        %d", stats.mm_filtered)
        logger.info("  MM errors:          %d", stats.mm_errors)
        logger.info("  Duration:           %.2fs", elapsed)
        return 0 if stats.failed == 0 and stats.mm_errors == 0 else 1
    finally:
        await redis.aclose()


def main() -> None:
    raise SystemExit(asyncio.run(run(parse_args())))


if __name__ == "__main__":
    main()
