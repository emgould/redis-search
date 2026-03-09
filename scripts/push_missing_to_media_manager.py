#!/usr/bin/env python3
"""
Push media documents that are missing from Media Manager into the FAISS insertion queue.

Reads the output of `scripts/audit_media_manager_coverage.py`
(`data/audit/missing_from_media_manager.json` by default) and submits each
`redis_doc` payload to `/insert-docs` in batches.

No content filtering is applied here; each selected `redis_doc` is forwarded
unchanged (subject only to `--skip`/`--take` windowing).

Usage:
    # Dry-run against the latest audit file
    python scripts/push_missing_to_media_manager.py --dry-run

    # Send all misses from custom path, submit in 50-size batches
    python scripts/push_missing_to_media_manager.py --input data/audit/missing_from_media_manager.json --batch-size 50

    # Send and flush queue after submission
    python scripts/push_missing_to_media_manager.py
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import TypedDict, cast

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from adapters.config import load_env  # noqa: E402

load_env()

from adapters.media_manager_client import MediaManagerClient  # noqa: E402
from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)


class MissingEntry(TypedDict, total=False):
    mc_id: str
    redis_key: str
    redis_doc: dict[str, object]


class LoaderStats(TypedDict):
    prepared: int
    skip_option: int
    mm_skipped: int
    submitted: int
    queued: int
    errors: int
    take: int


def _load_missing_items(input_path: Path) -> list[dict[str, object]]:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    with input_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, list):
        raise ValueError("Input JSON must be a list of missing entries")

    return cast(list[dict[str, object]], payload)


def _extract_documents(
    missing_items: list[dict[str, object]],
    skip: int,
    take: int | None,
) -> tuple[list[dict[str, object]], int]:
    if skip < 0:
        raise ValueError("skip must be >= 0")
    if take is not None and take < 0:
        raise ValueError("take must be >= 0")

    total_items = len(missing_items)
    if skip >= total_items:
        return [], skip

    stop = None if take is None else min(total_items, skip + take)
    selected_items = missing_items[skip:stop]

    documents: list[dict[str, object]] = []
    for item in selected_items:
        if not isinstance(item, dict):
            raise ValueError("Audit file contains non-object entries")

        entry = cast(MissingEntry, item)
        redis_doc_obj = entry.get("redis_doc")
        if not isinstance(redis_doc_obj, dict):
            raise ValueError(
                f"Missing or invalid redis_doc for entry: {entry.get('mc_id', 'unknown')}"
            )

        doc = dict(redis_doc_obj)
        if not isinstance(doc.get("mc_id"), str):
            mc_id = entry.get("mc_id")
            if isinstance(mc_id, str) and mc_id:
                doc["mc_id"] = mc_id
            else:
                raise ValueError("Entry missing redis_doc.mc_id and top-level mc_id")
        documents.append(doc)

    return documents, skip


async def _send_batches(
    media_manager: MediaManagerClient,
    documents: list[dict[str, object]],
    batch_size: int,
    dry_run: bool,
    metadata_only: bool = False,
) -> tuple[LoaderStats, list[str]]:
    stats: LoaderStats = {
        "prepared": len(documents),
        "skip_option": 0,
        "mm_skipped": 0,
        "submitted": 0,
        "queued": 0,
        "errors": 0,
        "take": len(documents),
    }
    batch_errors: list[str] = []

    for idx in range(0, len(documents), batch_size):
        batch = documents[idx : idx + batch_size]
        try:
            response = await media_manager.insert_docs(
                [dict(item) for item in batch],
                dry_run=dry_run,
                metadata_only=metadata_only,
            )
        except Exception as exc:
            stats["errors"] += 1
            batch_errors.append(f"Batch {idx // batch_size + 1} failed: {exc!s}")
            continue

        stats["submitted"] += len(batch)
        stats["queued"] += response["queued"]
        stats["mm_skipped"] += response["skipped"]
        batch_api_errors = response.get("errors", [])
        if batch_api_errors:
            stats["errors"] += len(batch_api_errors)
            batch_errors.extend(batch_api_errors)

        logger.info(
            "Batch %d submitted: queued=%d, skipped=%d, queue_depth=%d",
            idx // batch_size + 1,
            response["queued"],
            response["skipped"],
            response["queue_depth"],
        )

    return stats, batch_errors


async def push_missing_to_media_manager(
    input_path: Path,
    batch_size: int,
    skip: int,
    take: int | None,
    dry_run: bool,
    no_flush: bool,
    poll_interval: float,
    max_wait: float,
    metadata_only: bool = False,
) -> None:
    parsed_batch_size = min(max(batch_size, 1), 100)
    if parsed_batch_size != batch_size:
        logger.info("Batch size clamped to %d (from %d)", parsed_batch_size, batch_size)

    missing_items = _load_missing_items(input_path)
    documents, skipped_count = _extract_documents(missing_items, skip, take)

    if not documents:
        print("No documents to submit after applying skip/take.")
        print(f"Total rows in input: {len(missing_items)}")
        print(f"Skipped by --skip: {skipped_count}")
        return

    mm_client = MediaManagerClient()
    mm_errors: list[str] = []
    requested_take = len(documents) if take is None else take
    stats: LoaderStats = {
        "prepared": len(documents),
        "skip_option": skipped_count,
        "submitted": 0,
        "queued": 0,
        "errors": 0,
        "mm_skipped": 0,
        "take": requested_take,
    }

    try:
        await mm_client.health_check()

        logger.info(
            "Loaded %d rows from %s", len(missing_items), input_path
        )
        logger.info(
            "Prepared %d documents for insertion (skip=%d, take=%s)",
            len(documents),
            skipped_count,
            "all" if take is None else str(take),
        )

        batch_stats, batch_errors = await _send_batches(
            media_manager=mm_client,
            documents=documents,
            batch_size=parsed_batch_size,
            dry_run=dry_run,
            metadata_only=metadata_only,
        )
        stats["submitted"] = batch_stats["submitted"]
        stats["queued"] = batch_stats["queued"]
        stats["mm_skipped"] = batch_stats["mm_skipped"]
        stats["errors"] = batch_stats["errors"]
        mm_errors.extend(batch_errors)

        if not dry_run and not no_flush:
            logger.info("Polling until queue is drained before flush...")
            await mm_client.poll_until_drained(
                poll_interval=poll_interval,
                max_wait=max_wait,
            )
            logger.info("Calling /insert-docs/flush...")
            flush_response = await mm_client.flush()
            logger.info(
                "Flush response: status=%s, movies_added=%d, tv_added=%d, "
                "movies_updated=%d, tv_updated=%d",
                flush_response["status"],
                flush_response["movies_added"],
                flush_response["tv_added"],
                flush_response["movies_updated"],
                flush_response["tv_updated"],
            )
        elif dry_run:
            logger.info("[DRY RUN] Skipping queue drain + flush")
        elif no_flush:
            logger.info("Skipping flush (--no-flush)")
    finally:
        await mm_client.close()

    print("\n" + "=" * 60)
    print("Media Manager missing-doc loader — Summary")
    print("=" * 60)
    print(f"  input_file: {input_path}")
    print(f"  prepared: {stats['prepared']}")
    print(f"  skipped_by_option: {stats['skip_option']}")
    print(f"  submitted: {stats['submitted']}")
    print(f"  queued: {stats['queued']}")
    print(f"  mm_skipped: {stats['mm_skipped']}")
    print(f"  take: {stats['take']}")
    print(f"  errors: {stats['errors']}")

    if mm_errors:
        print(f"\n  errors ({len(mm_errors)}):")
        for err in mm_errors:
            print(f"    - {err}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load audited missing media docs into Media Manager"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/audit/missing_from_media_manager.json"),
        help="Path to audit output JSON (default: data/audit/missing_from_media_manager.json)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Max docs per /insert-docs request (default: 100)",
    )
    parser.add_argument(
        "--skip",
        type=int,
        default=0,
        help="Skip the first N entries from the input JSON",
    )
    parser.add_argument(
        "--take",
        type=int,
        default=None,
        help="Process only this many entries from the skipped input",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Submit with dry_run=true; validate endpoint response without insertion",
    )
    parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Update FAISS metadata only; skip wiki/LLM/embedding pipeline",
    )
    parser.add_argument(
        "--no-flush",
        action="store_true",
        help="Submit batches and do not call /insert-docs/flush",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=5.0,
        help="Seconds between /insert-docs/status polls",
    )
    parser.add_argument(
        "--max-wait",
        type=float,
        default=1800.0,
        help="Maximum seconds to wait before poll timeout",
    )
    return parser.parse_args()


async def main() -> None:
    args = _parse_args()
    start = time.time()
    os.environ.setdefault("PYTHONWARNINGS", "ignore")
    await push_missing_to_media_manager(
        input_path=args.input,
        batch_size=args.batch_size,
        skip=args.skip,
        take=args.take,
        dry_run=args.dry_run,
        no_flush=args.no_flush,
        poll_interval=args.poll_interval,
        max_wait=args.max_wait,
        metadata_only=args.metadata_only,
    )
    elapsed = time.time() - start
    print(f"  elapsed_seconds: {elapsed:.2f}\n")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
