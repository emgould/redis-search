#!/usr/bin/env python3
"""Backfill compact microgenre metadata onto existing Redis media documents.

See ``etl.microgenre_gap_fill`` for scan/classify/write helpers.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path
from typing import cast

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from adapters.config import load_env  # noqa: E402

load_env()

from redis.asyncio import Redis  # noqa: E402

from ai.prompts.microgenre_classifier import MicroGenreProvider  # noqa: E402
from etl.microgenre_gap_fill import (  # noqa: E402
    DEFAULT_CEREBRAS_CONCURRENCY,
    DEFAULT_CONCURRENCY,
    DEFAULT_PATHS,
    BackfillStats,  # noqa: F401
    _resolve_concurrency,
    backfill_rows,
    collect_empty_score_docs,
    collect_missing_docs,
    fill_missing,
    load_sidecar_rows,
    maybe_finalize_mm_publish,
)
from etl.microgenre_scan_cache import (  # noqa: E402
    _cache_request,
    cache_request_matches,
    default_scan_cache_path,
    load_candidates_from_cache_keys,
    read_scan_cache,
    resolve_scan_source,
    stats_from_cache_dict,
    write_scan_cache,
)
from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill media:* microgenres. Default mode scans Redis for missing "
            "microgenres on titles with an RT score or release/first-air within "
            "the last 3 years, classifies them, writes Redis, and pushes Media "
            "Manager metadata updates. --all-missing classifies every movie/tv "
            "document that is missing valid microgenres. --empty-scores "
            "reclassifies idx:media movie/tv documents whose microgenre_scores "
            "object is empty."
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
        "--all-missing",
        action="store_true",
        help=(
            "Classify every movie/tv document missing valid microgenres. "
            "Skips the RT-score and last-3-years eligibility gate."
        ),
    )
    parser.add_argument(
        "--empty-scores",
        action="store_true",
        help=(
            "Reclassify idx:media movie/tv documents whose microgenre_scores "
            "object is {}. Does not select missing microgenres or populated scores."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reclassify/overwrite docs that already have valid microgenres.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help=(
            "Max concurrent LLM classifications in missing mode "
            f"(default {DEFAULT_CONCURRENCY} for OpenAI, "
            f"{DEFAULT_CEREBRAS_CONCURRENCY} for Cerebras)."
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
        default=True,
        help="Enable OpenAI date-gated web_search (default: on). Use --no-web-search to disable.",
    )
    parser.add_argument(
        "--llm",
        choices=["openai", "cerebras"],
        default="openai",
        help="Classifier backend for missing mode (default: openai).",
    )
    parser.add_argument(
        "--finalize",
        action="store_true",
        help="After MM pushes, drain the queue and call finalize-publish.",
    )
    parser.add_argument("--redis-host", default=os.getenv("REDIS_HOST", "localhost"))
    parser.add_argument("--redis-port", type=int, default=int(os.getenv("REDIS_PORT", "6380")))
    parser.add_argument("--redis-password", default=os.getenv("REDIS_PASSWORD") or None)
    parser.add_argument(
        "--redis-label",
        default=os.getenv("MICROGENRE_SCAN_REDIS_LABEL", "local"),
        help="REDIS=local|dev label for default scan cache path (Makefile sets this).",
    )
    parser.add_argument(
        "--scan-cache-file",
        type=Path,
        default=None,
        help="Scan cache JSON path (default: data/microgenre-backfill/scan-<redis-label>.json).",
    )
    parser.add_argument(
        "--use-scan-cache",
        action="store_true",
        help="Load candidate keys from scan cache (skip Redis SCAN).",
    )
    parser.add_argument(
        "--refresh-scan-cache",
        action="store_true",
        help="Force a live SCAN and overwrite the scan cache.",
    )
    parser.add_argument(
        "--no-scan-cache",
        action="store_true",
        help="Disable scan cache read/write and interactive reuse prompt.",
    )
    return parser.parse_args()


async def run(args: argparse.Namespace) -> int:
    llm_provider = cast(MicroGenreProvider, args.llm)
    if args.mode == "missing" and args.web_search and llm_provider == "cerebras":
        raise SystemExit("--web-search is only supported with --llm openai")
    if args.mode == "missing" and llm_provider == "cerebras" and not os.getenv("CEREBRAS_API_KEY"):
        raise SystemExit("CEREBRAS_API_KEY is required when --llm cerebras")
    if args.mode == "missing" and llm_provider == "openai" and not os.getenv("OPENAI_API_KEY"):
        raise SystemExit("OPENAI_API_KEY is required when --llm openai")
    if not args.dry_run and args.push_to_mm and not os.getenv("MEDIA_MANAGER_API_URL"):
        raise SystemExit("MEDIA_MANAGER_API_URL is required unless --no-push-to-mm is specified")

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
            logger.info("Loaded %d successful sidecar rows", len(rows))
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
                await maybe_finalize_mm_publish(args.push_to_mm, args.dry_run, args.finalize)
            elapsed = time.time() - start
            logger.info("Sidecar backfill done in %.2fs (updated=%d)", elapsed, stats.updated)
            return 0 if stats.malformed_rows == 0 and stats.mm_errors == 0 else 1

        if args.empty_scores and args.all_missing:
            logger.error("--empty-scores cannot be combined with --all-missing")
            return 2

        cache_path = args.scan_cache_file or default_scan_cache_path(
            redis_label=args.redis_label
        )
        scan_request = _cache_request(
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            mc_type=args.mc_type,
            all_missing=args.all_missing,
            empty_scores=args.empty_scores,
            force=args.force,
            limit=args.limit,
        )

        if args.empty_scores:
            candidates, stats = await collect_empty_score_docs(redis, args.mc_type, args.limit)
        else:
            scan_source = resolve_scan_source(
                cache_path,
                use_scan_cache=args.use_scan_cache,
                refresh_scan_cache=args.refresh_scan_cache,
                no_scan_cache=args.no_scan_cache,
            )
            if scan_source == "cache":
                payload = read_scan_cache(cache_path)
                if (
                    payload is not None
                    and isinstance(payload.get("request"), dict)
                    and cache_request_matches(payload["request"], scan_request)
                ):
                    raw_keys = payload.get("candidate_keys")
                    if isinstance(raw_keys, list):
                        cached_stats = stats_from_cache_dict(
                            payload["stats"] if isinstance(payload.get("stats"), dict) else {}
                        )
                        candidates, stats = await load_candidates_from_cache_keys(
                            redis,
                            [str(k) for k in raw_keys],
                            mc_type=args.mc_type,
                            force=args.force,
                            limit=args.limit,
                            cached_stats=cached_stats,
                        )
                        logger.info(
                            "Scan cache hit: %s (%d candidates after re-filter)",
                            cache_path,
                            len(candidates),
                        )
                    else:
                        scan_source = "scan"
                else:
                    logger.warning(
                        "Scan cache at %s does not match this request; running live SCAN",
                        cache_path,
                    )
                    scan_source = "scan"

            if scan_source == "scan":
                candidates, stats = await collect_missing_docs(
                    redis,
                    args.mc_type,
                    args.force,
                    args.limit,
                    all_missing=args.all_missing,
                )
                if not args.no_scan_cache and not args.dry_run:
                    write_scan_cache(
                        cache_path,
                        candidate_keys=[key for key, _ in candidates],
                        stats=stats,
                        request=scan_request,
                    )

        logger.info(
            "Redis scan complete: redis_keys=%d movie_tv_scanned=%d already_populated=%d "
            "ineligible=%d candidates=%d%s",
            stats.keys_seen,
            stats.scanned_docs,
            stats.already_has_microgenres,
            stats.skipped_ineligible,
            stats.candidates,
            f" (limit={args.limit})" if args.limit is not None else " (no limit)",
        )
        if stats.candidates and stats.sample_mc_ids:
            logger.info("Sample gap mc_ids: %s", ", ".join(stats.sample_mc_ids))

        stats = await fill_missing(
            redis,
            candidates,
            stats,
            args.dry_run,
            _resolve_concurrency(args.concurrency, llm_provider),
            args.push_to_mm,
            args.web_search,
            llm_provider,
        )
        if stats.mm_errors == 0:
            await maybe_finalize_mm_publish(args.push_to_mm, args.dry_run, args.finalize)
        elapsed = time.time() - start
        logger.info(
            "Missing-fill done in %.2fs: updated=%d unresolved=%d classify_errors=%d classified=%d "
            "mm_errors=%d",
            elapsed,
            stats.updated,
            stats.unresolved,
            stats.failed,
            stats.classified,
            stats.mm_errors,
        )
        if stats.unresolved:
            logger.info(
                "Unresolved=%d titles: classifier returned no scores (empty microgenre_scores); "
                "expected at catalog scale",
                stats.unresolved,
            )
        if stats.failed:
            logger.info(
                "Classify_errors=%d titles: API/timeout/exception or invalid model output "
                "(per-title; does not fail the run)",
                stats.failed,
            )
        if stats.mm_errors:
            logger.error(
                "Media Manager errors=%d; exiting with status 1",
                stats.mm_errors,
            )
            return 1
        return 0
    finally:
        await redis.aclose()


def main() -> None:
    raise SystemExit(asyncio.run(run(parse_args())))


if __name__ == "__main__":
    main()
