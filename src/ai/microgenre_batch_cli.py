from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

from redis.asyncio import Redis

from adapters.config import load_env
from ai.microgenre_batch import run_microgenre_batch
from ai.microgenre_batch_models import (
    DEFAULT_BATCH_CONCURRENCY,
    DEFAULT_BATCH_SIZE,
    MAX_BATCH_CONCURRENCY,
    MicroGenreBatchConfig,
    MicroGenreBatchStatus,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch classify Redis media documents into sparse micro-genre sidecar rows."
    )
    parser.add_argument("--media-type", choices=["tv", "movie", "both"], default="both")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--concurrency", type=int, default=DEFAULT_BATCH_CONCURRENCY)
    parser.add_argument("--score-threshold", type=float, default=0.1)
    parser.add_argument(
        "--rt-threshold",
        type=float,
        default=None,
        help="Optional historical batch filter: only classify movies above this RT score.",
    )
    parser.add_argument("--skip", type=int, default=0)
    parser.add_argument("--take", type=int)
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--retry-errors",
        action="store_true",
        help="Retry rows that previously failed under the current prompt/hash.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--checkpoint-every", type=int, default=1)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--retry-delay-seconds", type=float, default=1.0)
    parser.add_argument("--output-path", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--redis-host", default=os.getenv("REDIS_HOST", "localhost"))
    parser.add_argument("--redis-port", type=int, default=int(os.getenv("REDIS_PORT", "6380")))
    parser.add_argument("--redis-password", default=os.getenv("REDIS_PASSWORD") or None)
    return parser.parse_args()


def _print_status(status: MicroGenreBatchStatus) -> None:
    print(
        " | ".join(
            [
                f"stage={status.stage}",
                f"batch={status.current_batch}/{status.total_batches}",
                f"selected={status.total_selected}",
                f"planned={status.planned}",
                f"processed={status.processed}",
                f"ok={status.succeeded}",
                f"failed={status.failed}",
                f"skipped={status.skipped_existing}",
                f"avg_llm={status.average_execution_time:.2f}s",
                f"output={status.output_path}",
            ]
        ),
        flush=True,
    )


async def main() -> int:
    load_env()
    args = parse_args()
    if args.concurrency > MAX_BATCH_CONCURRENCY:
        print(f"concurrency must be <= {MAX_BATCH_CONCURRENCY}")
        return 2
    if args.resume and args.output_path is None and not args.run_id:
        print("--resume requires --output-path or --run-id")
        return 2

    config_kwargs = {
        "media_type": args.media_type,
        "batch_size": args.batch_size,
        "concurrency": args.concurrency,
        "score_threshold": args.score_threshold,
        "rt_threshold": args.rt_threshold,
        "skip": args.skip,
        "take": args.take,
        "force": args.force,
        "retry_errors": args.retry_errors,
        "dry_run": args.dry_run,
        "resume": args.resume,
        "checkpoint_every": args.checkpoint_every,
        "max_retries": args.max_retries,
        "retry_delay_seconds": args.retry_delay_seconds,
        "output_path": args.output_path,
    }
    if args.run_id:
        config_kwargs["run_id"] = args.run_id
    config = MicroGenreBatchConfig(**config_kwargs)

    redis = Redis(
        host=args.redis_host,
        port=args.redis_port,
        password=args.redis_password,
        decode_responses=True,
    )
    try:
        status = await run_microgenre_batch(redis, config, status_callback=_print_status)
        return 1 if status.error or status.failed else 0
    finally:
        await redis.aclose()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
