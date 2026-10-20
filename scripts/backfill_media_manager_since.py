#!/usr/bin/env python3
"""Push Redis media documents modified since a date to Media Manager.

Selects movie and TV documents from ``idx:media`` with
``modified_at >= since`` (UTC midnight, inclusive) and submits the ones
that pass ``passes_media_manager_filter`` to ``/insert-docs``.

This is a catch-up for the nightly ETL push. It does not write Redis.
Publish finalization stays off unless ``--finalize`` is passed; the next
aggregate ETL run owns ``/api/etl/finalize-publish``.

Usage:
    python scripts/backfill_media_manager_since.py --dry-run
    python scripts/backfill_media_manager_since.py --dry-run --limit 20
    python scripts/backfill_media_manager_since.py --since 2026-10-10
    python scripts/backfill_media_manager_since.py --since 2026-10-10 --finalize
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from redis.asyncio import Redis

from adapters.media_manager_client import MediaManagerClient
from etl.media_manager_filter import passes_media_manager_filter
from utils.get_logger import get_logger

_project_root = Path(__file__).resolve().parent.parent
load_dotenv(_project_root / "config" / "local.env")
load_dotenv(_project_root / "config" / "etl.dev.env", override=True)
_default_sa_path = _project_root / "config" / "media-circle-3db28338ecf4.json"
_configured_sa = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not _configured_sa:
    if _default_sa_path.is_file():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(_default_sa_path)
elif not Path(_configured_sa).is_absolute():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(_project_root / _configured_sa)

logger = get_logger(__name__)

INDEX_NAME = "idx:media"
DEFAULT_SINCE = "2026-10-10"
CURSOR_COUNT = 1000
MGET_BATCH_SIZE = 100
MAX_BATCH_SIZE = 100
AGGREGATE_TIMEOUT_MS = 60_000


def _connect_redis() -> Redis:
    host = os.getenv("PUBLIC_REDIS_HOST", "localhost")
    port = int(os.getenv("PUBLIC_REDIS_PORT", "6381"))
    password = os.getenv("PUBLIC_REDIS_PASSWORD") or os.getenv("REDIS_PASSWORD") or None
    return Redis(
        host=host,
        port=port,
        password=password,
        decode_responses=True,
    )


def _parse_utc_date(value: str) -> datetime:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Expected YYYY-MM-DD, got {value!r}"
        ) from exc
    return parsed.replace(tzinfo=UTC)


def _inclusive_end(day: datetime) -> int:
    """Last unix second of a UTC calendar day."""
    next_day = day + timedelta(days=1)
    return int(next_day.timestamp()) - 1


def _json_doc(raw: object) -> dict[str, Any] | None:
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


def _cursor_token(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if text in {"", "0"}:
        return None
    return text


def _split_cursor_page(raw: object) -> tuple[list[object], str | None]:
    if not isinstance(raw, list) or len(raw) != 2:
        return [], None
    page, cursor = raw
    rows = page if isinstance(page, list) else []
    return rows, _cursor_token(cursor)


def _mc_ids_from_page(rows: list[object]) -> list[str]:
    found: list[str] = []
    for row in rows[1:]:
        if not isinstance(row, list):
            continue
        parsed: dict[str, str] = {}
        for index in range(0, len(row) - 1, 2):
            key = row[index]
            value = row[index + 1]
            if isinstance(key, str) and isinstance(value, str):
                parsed[key] = value
        mc_id = parsed.get("$.mc_id") or parsed.get("mc_id")
        if mc_id:
            found.append(mc_id.removeprefix("media:"))
    return found


def _redis_key(mc_id: str) -> str:
    if mc_id.startswith("media:"):
        return mc_id
    return f"media:{mc_id}"


async def _close_cursor(redis: Redis, cursor_id: str | None) -> None:
    if cursor_id is None:
        return
    try:
        await redis.execute_command("FT.CURSOR", "DEL", INDEX_NAME, cursor_id)
    except Exception:
        logger.warning("Failed to close idx:media cursor %s", cursor_id)


async def _load_docs(redis: Redis, mc_ids: list[str]) -> list[dict[str, Any] | None]:
    if not mc_ids:
        return []
    keys = [_redis_key(mc_id) for mc_id in mc_ids]
    raw: object = await redis.execute_command("JSON.MGET", *keys, "$")
    if not isinstance(raw, list):
        return [None for _ in keys]
    return [_json_doc(item) for item in raw]


async def _submit_batch(
    client: MediaManagerClient | None,
    batch: list[dict[str, Any]],
    stats: dict[str, int],
    errors: list[str],
    *,
    dry_run: bool,
    metadata_only: bool,
) -> None:
    stats["submitted"] += len(batch)
    if dry_run or client is None:
        return
    try:
        response = await client.insert_docs(batch, metadata_only=metadata_only)
    except Exception as exc:
        stats["errors"] += len(batch)
        message = f"batch failed ({len(batch)} docs): {exc}"
        errors.append(message)
        logger.error("insert-docs %s", message)
        return
    stats["queued"] += response["queued"]
    stats["skipped"] += response["skipped"]
    for err in response["errors"]:
        stats["errors"] += 1
        text = str(err)
        errors.append(text)
        logger.warning("insert-docs error: %s", text)
    logger.info(
        "Batch submitted: queued=%d skipped=%d queue_depth=%d",
        response["queued"],
        response["skipped"],
        response["queue_depth"],
    )


async def backfill(
    since_ts: int,
    until_ts: int | None,
    limit: int | None,
    batch_size: int,
    dry_run: bool,
    metadata_only: bool,
    finalize: bool,
    no_wait: bool,
) -> dict[str, Any]:
    """Query modified media docs and push eligible ones to Media Manager."""
    stats: dict[str, int] = {
        "index_rows": 0,
        "loaded": 0,
        "missing_docs": 0,
        "filtered": 0,
        "movies": 0,
        "tv": 0,
        "submitted": 0,
        "queued": 0,
        "skipped": 0,
        "errors": 0,
    }
    filter_reasons: Counter[str] = Counter()
    errors: list[str] = []
    until_bound = "+inf" if until_ts is None else str(until_ts)
    query = f"@mc_type:{{movie|tv}} @modified_at:[{since_ts} {until_bound}]"

    redis = _connect_redis()
    client: MediaManagerClient | None = None
    cursor_id: str | None = None
    pending: list[dict[str, Any]] = []
    reached_limit = False

    try:
        await redis.execute_command("PING")
        logger.info(
            "Public Redis connected (%s:%s)",
            os.getenv("PUBLIC_REDIS_HOST", "localhost"),
            os.getenv("PUBLIC_REDIS_PORT", "6381"),
        )

        if not os.getenv("MEDIA_MANAGER_API_URL"):
            raise SystemExit("MEDIA_MANAGER_API_URL is required")
        client = MediaManagerClient()
        await client.health_check()

        logger.info(
            "Media Manager since-backfill: query=%s batch_size=%d limit=%s "
            "dry_run=%s metadata_only=%s finalize=%s",
            query,
            batch_size,
            limit,
            dry_run,
            metadata_only,
            finalize,
        )

        raw: object = await redis.execute_command(
            "FT.AGGREGATE",
            INDEX_NAME,
            query,
            "LOAD",
            "1",
            "$.mc_id",
            "TIMEOUT",
            str(AGGREGATE_TIMEOUT_MS),
            "WITHCURSOR",
            "COUNT",
            str(CURSOR_COUNT),
        )

        while True:
            rows, cursor_id = _split_cursor_page(raw)
            mc_ids = _mc_ids_from_page(rows)
            stats["index_rows"] += len(mc_ids)

            for offset in range(0, len(mc_ids), MGET_BATCH_SIZE):
                chunk = mc_ids[offset : offset + MGET_BATCH_SIZE]
                docs = await _load_docs(redis, chunk)
                for doc in docs:
                    if doc is None:
                        stats["missing_docs"] += 1
                        continue
                    stats["loaded"] += 1
                    passed, reason = passes_media_manager_filter(doc)
                    if not passed:
                        stats["filtered"] += 1
                        filter_reasons[reason] += 1
                        continue

                    mc_type = doc.get("mc_type")
                    if mc_type == "movie":
                        stats["movies"] += 1
                    elif mc_type == "tv":
                        stats["tv"] += 1

                    pending.append(doc)
                    if len(pending) >= batch_size:
                        await _submit_batch(
                            client,
                            pending,
                            stats,
                            errors,
                            dry_run=dry_run,
                            metadata_only=metadata_only,
                        )
                        pending = []

                    eligible = stats["movies"] + stats["tv"]
                    if limit is not None and eligible >= limit:
                        reached_limit = True
                        break
                if reached_limit:
                    break

            logger.info(
                "Progress: index_rows=%d loaded=%d filtered=%d eligible=%d",
                stats["index_rows"],
                stats["loaded"],
                stats["filtered"],
                stats["movies"] + stats["tv"],
            )

            if reached_limit or cursor_id is None:
                break

            raw = await redis.execute_command(
                "FT.CURSOR",
                "READ",
                INDEX_NAME,
                cursor_id,
                "COUNT",
                str(CURSOR_COUNT),
            )

        await _close_cursor(redis, cursor_id)
        cursor_id = None

        if pending:
            await _submit_batch(
                client,
                pending,
                stats,
                errors,
                dry_run=dry_run,
                metadata_only=metadata_only,
            )
            pending = []

        finalize_status: str | None = None
        drain_error: str | None = None
        if not dry_run and client is not None and stats["submitted"] > 0 and not no_wait:
            logger.info("Polling until the Media Manager queue drains")
            try:
                await client.poll_until_drained()
            except TimeoutError as exc:
                drain_error = str(exc)
                logger.error("%s", drain_error)
            else:
                if finalize:
                    logger.info("Calling /api/etl/finalize-publish")
                    publish = await client.finalize_publish()
                    finalize_status = publish["status"]
                    logger.info("Finalize response status=%s", finalize_status)
    finally:
        await _close_cursor(redis, cursor_id)
        if client is not None:
            await client.close()
        await redis.aclose()

    return {
        **stats,
        "since": since_ts,
        "until": until_ts,
        "filter_reasons": dict(filter_reasons),
        "insert_errors": errors,
        "finalize_status": finalize_status,
        "drain_error": drain_error,
    }


def _print_stats(stats: Mapping[str, Any], elapsed: float, dry_run: bool) -> None:
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{'=' * 60}")
    print(f"{prefix}Media Manager since-backfill")
    print("=" * 60)
    since = stats.get("since")
    until = stats.get("until")
    if isinstance(since, int):
        since_label = datetime.fromtimestamp(since, tz=UTC).date().isoformat()
        until_label = (
            datetime.fromtimestamp(until, tz=UTC).date().isoformat()
            if isinstance(until, int)
            else "open"
        )
        print(f"  window: {since_label} .. {until_label} UTC")
    for key in (
        "index_rows",
        "loaded",
        "missing_docs",
        "filtered",
        "movies",
        "tv",
        "submitted",
        "queued",
        "skipped",
        "errors",
    ):
        value = stats.get(key, 0)
        if isinstance(value, int):
            print(f"  {key}: {value:,}")
    print(f"  elapsed: {elapsed:.2f}s")
    finalize_status = stats.get("finalize_status")
    if isinstance(finalize_status, str):
        print(f"  finalize_status: {finalize_status}")
    drain_error = stats.get("drain_error")
    if isinstance(drain_error, str):
        print(f"  drain_error: {drain_error}")

    reasons = stats.get("filter_reasons")
    if isinstance(reasons, dict) and reasons:
        print("\n  Filter reasons:")
        ranked = sorted(reasons.items(), key=lambda item: item[1], reverse=True)
        for reason, count in ranked[:15]:
            print(f"    {count:,}  {reason}")

    insert_errors = stats.get("insert_errors")
    if isinstance(insert_errors, list) and insert_errors:
        print(f"\n  Endpoint errors ({len(insert_errors)}):")
        for err in insert_errors[:20]:
            print(f"    - {err}")
        if len(insert_errors) > 20:
            print(f"    ... and {len(insert_errors) - 20} more")
    print()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Push movie and TV Redis documents modified since a UTC date "
            "to Media Manager"
        )
    )
    parser.add_argument(
        "--since",
        type=_parse_utc_date,
        default=_parse_utc_date(DEFAULT_SINCE),
        help=f"Inclusive UTC start date YYYY-MM-DD (default {DEFAULT_SINCE})",
    )
    parser.add_argument(
        "--until",
        type=_parse_utc_date,
        default=None,
        help="Inclusive UTC end date YYYY-MM-DD (default: open-ended)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max eligible documents to submit",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=MAX_BATCH_SIZE,
        help=f"Documents per POST (max {MAX_BATCH_SIZE})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Query Redis and apply the intake filter without calling Media Manager",
    )
    parser.add_argument(
        "--metadata-only",
        action="store_true",
        help=(
            "Update FAISS metadata only. Documents missing from Media Manager "
            "still fall through to the full insert pipeline"
        ),
    )
    parser.add_argument(
        "--finalize",
        action="store_true",
        help="After the queue drains, call /api/etl/finalize-publish",
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Submit batches and return without waiting for the queue to drain",
    )
    return parser


async def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    if args.batch_size < 1 or args.batch_size > MAX_BATCH_SIZE:
        parser.error(f"--batch-size must be between 1 and {MAX_BATCH_SIZE}")
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be greater than zero")
    if args.finalize and args.no_wait:
        parser.error("--finalize and --no-wait cannot be used together")
    if args.until is not None and args.until < args.since:
        parser.error("--until must be on or after --since")

    since_ts = int(args.since.timestamp())
    until_ts = _inclusive_end(args.until) if args.until is not None else None

    started = time.time()
    result = await backfill(
        since_ts=since_ts,
        until_ts=until_ts,
        limit=args.limit,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        metadata_only=args.metadata_only,
        finalize=args.finalize,
        no_wait=args.no_wait,
    )
    _print_stats(result, time.time() - started, args.dry_run)


if __name__ == "__main__":
    asyncio.run(main())
