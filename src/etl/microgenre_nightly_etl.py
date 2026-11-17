"""Weekly microgenre gap-fill: scan Redis, classify missing movie/tv docs, push MM."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import UTC, datetime

from adapters.media_manager_client import MediaManagerClient
from ai.prompts.microgenre_classifier import MicroGenreProvider
from etl.microgenre_gap_fill import (
    DEFAULT_CEREBRAS_CONCURRENCY,
    DEFAULT_CONCURRENCY,
    MissingStats,
    _resolve_concurrency,
    collect_empty_score_docs,
    run_missing_gap_fill,
)
from etl.tmdb_nightly_etl import ETLPhaseStats
from utils.get_logger import get_logger

logger = get_logger(__name__)

_WEEKDAY_BY_NAME: dict[str, int] = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}
DEFAULT_MICROGENRE_WEEKLY_WEEKDAY = "sunday"


@dataclass
class MicrogenreNightlyETLStats:
    """Statistics compatible with ETLRunner aggregation."""

    media_type: str = "microgenre"
    skipped_not_scheduled_day: bool = False
    total_changes_found: int = 0
    failed_filter: int = 0
    fetch_phase: ETLPhaseStats = field(default_factory=lambda: ETLPhaseStats("scan"))
    load_phase: ETLPhaseStats = field(default_factory=lambda: ETLPhaseStats("classify"))
    mm_docs_sent: int = 0
    mm_docs_queued: int = 0
    mm_docs_filtered: int = 0
    mm_errors: int = 0
    microgenres_generated: int = 0
    microgenres_failed: int = 0
    gap_fill: MissingStats | None = None

    def finalize_from(self, stats: MissingStats) -> None:
        self.gap_fill = stats
        self.total_changes_found = stats.candidates
        self.failed_filter = stats.skipped_ineligible
        self.fetch_phase.items_processed = stats.scanned_docs
        self.fetch_phase.items_success = stats.candidates
        self.load_phase.items_processed = stats.candidates
        self.load_phase.items_success = stats.updated
        self.load_phase.items_failed = stats.failed
        self.mm_docs_sent = stats.mm_queued
        self.mm_docs_queued = stats.mm_queued
        self.mm_docs_filtered = stats.mm_filtered
        self.mm_errors = stats.mm_errors
        self.microgenres_generated = stats.classified
        self.microgenres_failed = stats.failed


def _resolve_nightly_limit(max_batches: int) -> int | None:
    if max_batches > 0:
        return max_batches
    raw = os.getenv("MICROGENRE_NIGHTLY_LIMIT", "").strip()
    if not raw:
        return None
    try:
        parsed = int(raw)
    except ValueError:
        logger.warning("Invalid MICROGENRE_NIGHTLY_LIMIT=%r; ignoring", raw)
        return None
    return parsed if parsed > 0 else None


def _resolve_nightly_llm() -> MicroGenreProvider:
    value = os.getenv("MICROGENRE_NIGHTLY_LLM", "openai").strip().lower()
    if value == "cerebras":
        return "cerebras"
    return "openai"


def _resolve_nightly_concurrency(llm_provider: MicroGenreProvider) -> int:
    raw = os.getenv("MICROGENRE_ETL_CONCURRENCY", "").strip()
    if raw:
        try:
            requested = int(raw)
            return int(_resolve_concurrency(requested, llm_provider))
        except ValueError:
            logger.warning("Invalid MICROGENRE_ETL_CONCURRENCY=%r; using default", raw)
    default = DEFAULT_CEREBRAS_CONCURRENCY if llm_provider == "cerebras" else DEFAULT_CONCURRENCY
    return int(_resolve_concurrency(default, llm_provider))


def _all_missing_enabled() -> bool:
    return os.getenv("MICROGENRE_NIGHTLY_ALL_MISSING", "true").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _resolve_weekly_weekday() -> tuple[int, str]:
    """Return (weekday 0=Mon..6=Sun, label) for the configured weekly run day."""
    raw = os.getenv("MICROGENRE_WEEKLY_WEEKDAY", DEFAULT_MICROGENRE_WEEKLY_WEEKDAY).strip().lower()
    if raw.isdigit():
        value = int(raw)
        if 0 <= value <= 6:
            names = {v: k for k, v in _WEEKDAY_BY_NAME.items()}
            return value, names.get(value, raw)
    weekday = _WEEKDAY_BY_NAME.get(raw)
    if weekday is not None:
        return weekday, raw
    logger.warning(
        "Invalid MICROGENRE_WEEKLY_WEEKDAY=%r; using %s",
        raw,
        DEFAULT_MICROGENRE_WEEKLY_WEEKDAY,
    )
    return _WEEKDAY_BY_NAME[DEFAULT_MICROGENRE_WEEKLY_WEEKDAY], DEFAULT_MICROGENRE_WEEKLY_WEEKDAY


def microgenre_gap_fill_due(*, now: datetime | None = None) -> bool:
    """True when the scheduled weekly microgenre gap-fill should run (UTC weekday)."""
    if os.getenv("MICROGENRE_WEEKLY_DISABLE", "").strip().lower() in ("1", "true", "yes"):
        return True
    instant = now or datetime.now(UTC)
    weekday, _ = _resolve_weekly_weekday()
    return instant.weekday() == weekday


async def run_microgenre_nightly_etl(
    media_type: str = "microgenre",
    redis_host: str = "localhost",
    redis_port: int = 6380,
    redis_password: str | None = None,
    verbose: bool = False,
    max_batches: int = 0,
    media_manager_client: MediaManagerClient | None = None,
) -> MicrogenreNightlyETLStats:
    """
    After TMDB movie/tv ETL: heal missing microgenres on indexed media and push MM.

    Runs on one UTC weekday per week (default Sunday). Other nights no-op so gap-fill
    does not re-classify unknown titles daily. Set ``MICROGENRE_WEEKLY_DISABLE=true``
    to run every night (legacy). Override day via ``MICROGENRE_WEEKLY_WEEKDAY``.

    Defaults mirror nightly TMDB attach: OpenAI with date-gated web_search, and
    classify every movie/tv doc missing valid microgenres (``MICROGENRE_NIGHTLY_ALL_MISSING``).
    """
    del verbose
    if media_type not in ("microgenre", "microgenres", "both", "movie", "tv"):
        raise ValueError(f"Invalid media_type for microgenre nightly ETL: {media_type}")

    _, scheduled_label = _resolve_weekly_weekday()
    if not microgenre_gap_fill_due():
        today_names = {v: k for k, v in _WEEKDAY_BY_NAME.items()}
        today_label = today_names.get(datetime.now(UTC).weekday(), "?")
        out = MicrogenreNightlyETLStats()
        out.skipped_not_scheduled_day = True
        out.fetch_phase.phase = "skip"
        out.fetch_phase.completed_at = datetime.now(UTC)
        logger.info(
            "Microgenre gap-fill skipped: weekly schedule is %s UTC (today=%s)",
            scheduled_label,
            today_label,
        )
        return out

    mc_type = "both"
    if media_type in ("movie", "tv"):
        mc_type = media_type

    llm_provider = _resolve_nightly_llm()
    if llm_provider == "cerebras" and not os.getenv("CEREBRAS_API_KEY"):
        raise ValueError("CEREBRAS_API_KEY is required when MICROGENRE_NIGHTLY_LLM=cerebras")
    if llm_provider == "openai" and not os.getenv("OPENAI_API_KEY"):
        raise ValueError("OPENAI_API_KEY is required when MICROGENRE_NIGHTLY_LLM=openai")

    empty_scores = os.getenv("MICROGENRE_NIGHTLY_EMPTY_SCORES", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    limit = _resolve_nightly_limit(max_batches)
    concurrency = _resolve_nightly_concurrency(llm_provider)
    push_to_mm = bool(media_manager_client or os.getenv("MEDIA_MANAGER_API_URL"))
    enable_web_search = os.getenv("MICROGENRE_NIGHTLY_WEB_SEARCH", "true").strip().lower() in (
        "1",
        "true",
        "yes",
    )

    logger.info(
        "Microgenre nightly gap-fill starting (mc_type=%s all_missing=%s limit=%s "
        "llm=%s concurrency=%d empty_scores=%s)",
        mc_type,
        _all_missing_enabled(),
        limit,
        llm_provider,
        concurrency,
        empty_scores,
    )

    out = MicrogenreNightlyETLStats()
    out.fetch_phase.started_at = datetime.now(UTC)
    out.fetch_phase.phase = "scan"

    if empty_scores:
        from redis.asyncio import Redis

        redis = Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=True,
        )
        try:
            await redis.ping()  # type: ignore[misc]
            candidates, scan_stats = await collect_empty_score_docs(redis, mc_type, limit)
            out.fetch_phase.completed_at = datetime.now(UTC)
            out.load_phase.started_at = datetime.now(UTC)
            from etl.microgenre_gap_fill import fill_missing

            gap_stats = await fill_missing(
                redis,
                candidates,
                scan_stats,
                dry_run=False,
                concurrency=concurrency,
                push_to_mm=push_to_mm,
                enable_web_search=enable_web_search,
                llm_provider=llm_provider,
                media_manager_client=media_manager_client,
            )
        finally:
            await redis.aclose()
    else:
        gap_stats = await run_missing_gap_fill(
            redis_host,
            redis_port,
            redis_password,
            mc_type=mc_type,
            all_missing=_all_missing_enabled(),
            limit=limit,
            concurrency=concurrency,
            llm_provider=llm_provider,
            enable_web_search=enable_web_search,
            push_to_mm=push_to_mm,
            media_manager_client=media_manager_client,
        )

    out.fetch_phase.completed_at = datetime.now(UTC)
    out.load_phase.started_at = out.fetch_phase.completed_at
    out.load_phase.completed_at = datetime.now(UTC)
    out.finalize_from(gap_stats)

    logger.info(
        "Microgenre nightly complete: candidates=%d updated=%d unresolved=%d failed=%d mm_queued=%d",
        gap_stats.candidates,
        gap_stats.updated,
        gap_stats.unresolved,
        gap_stats.failed,
        gap_stats.mm_queued,
    )
    return out
