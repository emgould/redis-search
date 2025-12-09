#!/usr/bin/env python3
"""
Nightly ETL runner for TMDB and PodcastIndex.

This script runs the nightly ETL process and sends email notifications.
It's designed to be run via cron or manually.

Usage:
    python -m src.etl.run_nightly_etl                    # Run all enabled jobs
    python -m src.etl.run_nightly_etl --job tv           # Run only TV job
    python -m src.etl.run_nightly_etl --job podcast      # Run only Podcast job
    python -m src.etl.run_nightly_etl --start-date 2025-12-01  # Override start date
    python -m src.etl.run_nightly_etl --dry-run          # Show what would run

Environment variables (required):
    REDIS_HOST          - Redis server host
    REDIS_PORT          - Redis server port
    REDIS_PASSWORD      - Redis password
    TMDB_READ_TOKEN     - TMDB API token
    PODCASTINDEX_API_KEY - PodcastIndex API key (for podcast job)
    PODCASTINDEX_API_SECRET - PodcastIndex API secret

Environment variables (optional):
    ETL_CONFIG_PATH     - Path to etl_jobs.yaml (default: config/etl_jobs.yaml)
    GCS_BUCKET          - GCS bucket for metadata storage
    GCS_ETL_PREFIX      - GCS prefix for ETL metadata
    ETL_NOTIFICATION_EMAIL - Email for notifications
    SENDGRID_*          - SendGrid configuration for email
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime

from etl.etl_runner import ETLConfig, ETLRunner
from utils.get_logger import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run nightly ETL process")
    parser.add_argument(
        "--job",
        "-j",
        type=str,
        help="Run specific job only (tv, movie, person, podcast)",
        choices=["tv", "movie", "person", "podcast"],
    )
    parser.add_argument(
        "--start-date",
        "-s",
        type=str,
        help="Override start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        "-e",
        type=str,
        help="Override end date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be run without executing",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=0,
        help="Limit batches per job for testing (0=no limit)",
    )
    return parser.parse_args()


async def main() -> int:
    """Main entry point."""
    args = parse_args()

    start_time = datetime.now()
    logger.info(f"ETL starting at {start_time.isoformat()}")

    # Validate required environment variables
    required_vars = ["REDIS_HOST", "REDIS_PORT", "TMDB_READ_TOKEN"]
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        logger.error(f"Missing required environment variables: {missing}")
        return 1

    # Log configuration
    logger.info(f"Redis: {os.getenv('REDIS_HOST')}:{os.getenv('REDIS_PORT')}")
    logger.info(f"Config: {os.getenv('ETL_CONFIG_PATH', 'config/etl_jobs.yaml')}")

    if args.dry_run:
        logger.info("DRY RUN - showing configuration only")
        config = ETLConfig.from_env()
        for job in config.jobs:
            if job.enabled:
                logger.info(f"  Would run: {job.name} ({len(job.runs)} runs)")
        return 0

    try:
        # Load configuration
        config = ETLConfig.from_env()

        # Apply max_batches override for testing
        if args.max_batches > 0:
            logger.info(f"⚠️  TESTING MODE: Limiting to {args.max_batches} batches per job")
            for job in config.jobs:
                for params in job.runs:
                    params.max_batches = args.max_batches

        runner = ETLRunner(config)

        # Build job filter if specific job requested
        job_filter = None
        if args.job:
            if args.job == "podcast":
                job_filter = ["podcastindex_changes"]
            else:
                job_filter = [f"tmdb_{args.job}_changes"]

        # Run ETL
        result = await runner.run_all(
            start_date_override=args.start_date,
            end_date_override=args.end_date,
            job_filter=job_filter,
        )

        # Log results
        duration = datetime.now() - start_time
        logger.info(f"ETL completed in {duration}")
        logger.info(f"Jobs: {result.jobs_completed}/{result.total_jobs}")
        logger.info(f"Errors: {result.jobs_failed}")

        # Note: Email notification is sent by ETLRunner.run_all() - no need to send again here

        return 0 if result.jobs_failed == 0 else 1

    except Exception as e:
        logger.error(f"ETL failed with error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
