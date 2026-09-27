"""Wednesday FlixPatrol weekly maintenance and Redis title-file application."""

from dataclasses import dataclass, field
from datetime import UTC, datetime

from redis.asyncio import Redis

from api.subapi.flixpatrol.history import run_nightly_flixpatrol
from etl.flixpatrol_payload import apply_flixpatrol_title_files, flixpatrol_titles_dir
from utils.get_logger import get_logger

logger = get_logger(__name__)


@dataclass
class FlixPatrolPhaseStats:
    """Stats for one ETL phase (compatible with the ETL runner)."""

    phase: str = ""
    items_processed: int = 0
    items_success: int = 0
    items_failed: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass
class FlixPatrolETLStats:
    """Statistics from a FlixPatrol nightly maintenance run."""

    started_at: datetime | None = None
    completed_at: datetime | None = None
    skipped_not_wednesday: bool = False
    weeks_fetched: int = 0
    title_files_written: int = 0
    redis_docs_updated: int = 0
    media_type: str = "flixpatrol"
    total_changes_found: int = 0
    failed_filter: int = 0
    fetch_phase: FlixPatrolPhaseStats = field(
        default_factory=lambda: FlixPatrolPhaseStats(phase="fetch")
    )
    load_phase: FlixPatrolPhaseStats = field(
        default_factory=lambda: FlixPatrolPhaseStats(phase="load")
    )

    def finalize(self) -> None:
        self.total_changes_found = self.weeks_fetched + self.title_files_written
        self.fetch_phase.items_success = self.weeks_fetched
        self.load_phase.items_success = self.redis_docs_updated


async def run_flixpatrol_nightly_etl(
    media_type: str = "flixpatrol",
    redis_host: str = "localhost",
    redis_port: int = 6380,
    redis_password: str | None = None,
    verbose: bool = False,
    max_batches: int = 0,
) -> FlixPatrolETLStats:
    """Run Wednesday US weekly fetch, rebuild title files, stamp Redis documents."""
    del media_type, verbose, max_batches
    stats = FlixPatrolETLStats(started_at=datetime.now(UTC))
    try:
        result = await run_nightly_flixpatrol()
        if result is None:
            stats.skipped_not_wednesday = True
            logger.info("FlixPatrol nightly skipped: not Wednesday")
            return stats

        stats.weeks_fetched = sum(0 if week.skipped else 1 for week in result.weeks)
        stats.title_files_written = result.title_count

        titles_dir = flixpatrol_titles_dir()
        if titles_dir is None:
            stats.load_phase.errors.append("No FlixPatrol title files directory found")
            stats.load_phase.items_failed = 1
            return stats

        redis = Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=True,
        )
        try:
            stats.redis_docs_updated = await apply_flixpatrol_title_files(redis, titles_dir)
        finally:
            await redis.aclose()
    except Exception as exc:
        stats.fetch_phase.items_failed = 1
        stats.fetch_phase.errors.append(str(exc))
        logger.exception("FlixPatrol nightly ETL failed")
    finally:
        stats.completed_at = datetime.now(UTC)
        stats.finalize()
    return stats
