#!/usr/bin/env python3
"""Backfill compact microgenre metadata onto existing Redis media documents."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from adapters.config import load_env  # noqa: E402

load_env()

from redis.asyncio import Redis  # noqa: E402

from ai.microgenre_batch_models import MicroGenreBatchSidecarRecord  # noqa: E402
from ai.microgenre_document import microgenre_sidecar_to_redis  # noqa: E402
from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)

DEFAULT_DATA_DIR = (
    _project_root / "data" / "microgenre-classifications" / "april_25_2026"
)
DEFAULT_PATHS = {
    "tv": DEFAULT_DATA_DIR / "microgenre-results-tv.jsonl",
    "movie": DEFAULT_DATA_DIR / "microgenre-results-movie.jsonl",
}
WRITE_BATCH_SIZE = 500


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill media:* Redis documents from microgenre JSONL sidecars."
    )
    parser.add_argument("--mc-type", choices=["tv", "movie", "both"], default="both")
    parser.add_argument("--tv-path", type=Path, default=DEFAULT_PATHS["tv"])
    parser.add_argument("--movie-path", type=Path, default=DEFAULT_PATHS["movie"])
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing $.microgenres values instead of preserving them.",
    )
    parser.add_argument("--redis-host", default=os.getenv("REDIS_HOST", "localhost"))
    parser.add_argument("--redis-port", type=int, default=int(os.getenv("REDIS_PORT", "6380")))
    parser.add_argument("--redis-password", default=os.getenv("REDIS_PASSWORD") or None)
    return parser.parse_args()


def load_rows(
    paths: list[Path],
    mc_type: str,
    limit: int | None,
) -> tuple[list[MicroGenreBatchSidecarRecord], BackfillStats]:
    stats = BackfillStats()
    rows: list[MicroGenreBatchSidecarRecord] = []
    selected_types = {"tv", "movie"} if mc_type == "both" else {mc_type}

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


async def backfill_rows(
    redis: Redis,  # type: ignore[type-arg]
    rows: list[MicroGenreBatchSidecarRecord],
    stats: BackfillStats,
    dry_run: bool,
    force: bool,
) -> BackfillStats:
    now_ts = int(datetime.now(UTC).timestamp())

    for batch_start in range(0, len(rows), WRITE_BATCH_SIZE):
        batch = rows[batch_start : batch_start + WRITE_BATCH_SIZE]
        keys = [f"media:{row.mc_id}" for row in batch]
        existing_docs = await redis.execute_command("JSON.MGET", *keys, "$.microgenres")
        if not isinstance(existing_docs, list):
            existing_docs = [None] * len(batch)
        if len(existing_docs) != len(batch):
            existing_docs = [*existing_docs[: len(batch)], *([None] * (len(batch) - len(existing_docs)))]

        exists_pipe = redis.pipeline()
        for key in keys:
            exists_pipe.exists(key)
        exists_values = await exists_pipe.execute()

        pipe = redis.pipeline()
        write_count = 0
        for key, row, existing, exists in zip(keys, batch, existing_docs, exists_values, strict=True):
            microgenres = microgenre_sidecar_to_redis(row)
            if microgenres is None:
                stats.skipped_error_rows += 1
                continue

            if not exists:
                stats.missing_docs += 1
                continue

            if not force and _has_existing_microgenres(existing):
                stats.already_has_microgenres += 1
                continue

            if dry_run:
                stats.dry_run_updates += 1
                continue

            pipe.json().set(key, "$.microgenres", microgenres)
            pipe.json().set(key, "$.modified_at", now_ts)
            write_count += 1

        if write_count:
            await pipe.execute()
            stats.updated += write_count

        logger.info(
            "Progress: %d/%d rows handled (updated=%d dry_run=%d missing=%d existing=%d)",
            min(batch_start + len(batch), len(rows)),
            len(rows),
            stats.updated,
            stats.dry_run_updates,
            stats.missing_docs,
            stats.already_has_microgenres,
        )

    return stats


def _has_existing_microgenres(value: object) -> bool:
    if isinstance(value, str):
        try:
            return _has_existing_microgenres(json.loads(value))
        except json.JSONDecodeError:
            return bool(value.strip())
    if isinstance(value, list):
        if not value:
            return False
        return _has_existing_microgenres(value[0])
    return isinstance(value, dict) and bool(value)


async def run(args: argparse.Namespace) -> int:
    paths: list[Path] = []
    if args.mc_type in ("tv", "both"):
        paths.append(args.tv_path)
    if args.mc_type in ("movie", "both"):
        paths.append(args.movie_path)

    rows, stats = load_rows(paths, args.mc_type, args.limit)
    logger.info(
        "Loaded %d successful sidecar rows from %d path(s) (scanned=%d malformed=%d errors=%d)",
        len(rows),
        len(paths),
        stats.scanned_rows,
        stats.malformed_rows,
        stats.skipped_error_rows,
    )
    if not rows:
        return 0

    redis = Redis(
        host=args.redis_host,
        port=args.redis_port,
        password=args.redis_password,
        decode_responses=True,
    )
    try:
        await redis.ping()  # type: ignore[misc]
        start = time.time()
        stats = await backfill_rows(redis, rows, stats, args.dry_run, args.force)
        elapsed = time.time() - start
    finally:
        await redis.aclose()

    logger.info("=" * 60)
    logger.info("Microgenre Backfill Summary")
    logger.info("=" * 60)
    logger.info("  Scanned rows:       %d", stats.scanned_rows)
    logger.info("  Successful rows:    %d", stats.successful_rows)
    logger.info("  Updated:            %d", stats.updated)
    logger.info("  Dry-run updates:    %d", stats.dry_run_updates)
    logger.info("  Already populated:  %d", stats.already_has_microgenres)
    logger.info("  Missing docs:       %d", stats.missing_docs)
    logger.info("  Error rows skipped: %d", stats.skipped_error_rows)
    logger.info("  Malformed rows:     %d", stats.malformed_rows)
    logger.info("  Duration:           %.2fs", elapsed)
    return 0 if stats.malformed_rows == 0 else 1


def main() -> None:
    raise SystemExit(asyncio.run(run(parse_args())))


if __name__ == "__main__":
    main()
