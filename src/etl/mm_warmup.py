"""Media Manager warmup — keep Cloud Run instance warm before and during ETL.

Started by cron before the nightly ETL run. Pings the Media Manager
/health endpoint at a fixed interval so the Cloud Run instance stays
warm and responsive when the ETL needs it.

Usage:
    python -m etl.mm_warmup                # default 30 min, 5s interval
    python -m etl.mm_warmup --duration 600 # 10 minutes
    python -m etl.mm_warmup --interval 10  # ping every 10s
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from adapters.media_manager_client import MediaManagerClient
from utils.get_logger import get_logger

logger = get_logger(__name__)

DEFAULT_DURATION_SECONDS = 18000  # 5 hours (covers full VM uptime window)
DEFAULT_INTERVAL_SECONDS = 60


async def warmup_loop(duration: int, interval: int) -> None:
    try:
        client = MediaManagerClient()
    except ValueError as exc:
        logger.error("Cannot start warmup: %s", exc)
        return

    logger.info(
        "Starting Media Manager warmup: duration=%ds, interval=%ds, url=%s",
        duration,
        interval,
        client._base_url,
    )

    elapsed = 0
    ok_count = 0
    fail_count = 0

    try:
        while elapsed < duration:
            try:
                await client.health_check()
                ok_count += 1
            except Exception as exc:
                fail_count += 1
                logger.warning("Warmup ping failed (%d/%d): %s", fail_count, ok_count + fail_count, exc)

            await asyncio.sleep(interval)
            elapsed += interval
    finally:
        await client.close()
        logger.info(
            "Warmup finished: %d ok, %d failed out of %d pings over %ds",
            ok_count,
            fail_count,
            ok_count + fail_count,
            elapsed,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Media Manager warmup pinger")
    parser.add_argument(
        "--duration",
        type=int,
        default=DEFAULT_DURATION_SECONDS,
        help=f"Total run time in seconds (default: {DEFAULT_DURATION_SECONDS})",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL_SECONDS,
        help=f"Seconds between pings (default: {DEFAULT_INTERVAL_SECONDS})",
    )
    args = parser.parse_args()

    from adapters.config import load_env

    load_env()
    asyncio.run(warmup_loop(args.duration, args.interval))


if __name__ == "__main__":
    main()
    sys.exit(0)
