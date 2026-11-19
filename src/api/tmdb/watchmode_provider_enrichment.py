"""Watchmode /releases fallback when TMDB watch/providers has no display payload."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from api.tmdb.models import MCBaseMediaItem
from api.tmdb.utils.provider_utils import (
    build_streaming_platform_summary,
    resolve_provider_display_entry_by_name,
)
from utils.get_logger import get_logger

logger = get_logger(__name__)

NEW_RELEASE_WINDOW_DAYS = 60


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _as_int(value: object) -> int | None:
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str) and value.strip().isdigit():
        parsed = int(value.strip())
        return parsed if parsed > 0 else None
    return None


def has_provider_display_payload(watch_providers: object) -> bool:
    """True when watch_providers already has list-backed provider slots (MediaCircle parity)."""
    if not isinstance(watch_providers, dict):
        return False
    for provider_key in (
        "streaming_platforms",
        "on_demand_platforms",
        "flatrate",
        "rent",
        "buy",
    ):
        providers = watch_providers.get(provider_key)
        if isinstance(providers, list) and providers:
            return True
    return False


def _watchmode_release_content_types(content_type: str) -> str:
    if content_type == "tv":
        return "tv,tv_special,tv_miniseries"
    return "movie"


def _watchmode_release_is_on_demand(source_type: str | None) -> bool:
    if source_type is None:
        return False
    normalized = source_type.lower()
    return normalized in {"rent", "buy", "purchase", "digital_purchase"}


async def apply_watchmode_release_provider_enrichment(
    response: MCBaseMediaItem,
    tmdb_id: int,
    content_type: str,
) -> None:
    """
    When TMDB leaves watch_providers empty, match this tmdb_id in Watchmode /releases/
    and build the same streaming_platform_summary shape as custom_watch_provider.
    """
    if has_provider_display_payload(response.watch_providers):
        return

    try:
        from api.watchmode.auth import watchmode_auth
        from api.watchmode.core import WatchmodeService

        api_key = watchmode_auth.watchmode_api_key
        if not api_key:
            return

        now = datetime.now(UTC)
        start_date = now - timedelta(days=30)
        end_date = now + timedelta(days=NEW_RELEASE_WINDOW_DAYS)
        service = WatchmodeService(api_key=api_key)
        releases_response = await service.get_new_releases(
            region="US",
            types=_watchmode_release_content_types(content_type),
            limit=250,
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
            max_retries=1,
        )
    except Exception as error:
        logger.debug(
            "Watchmode release provider enrichment failed for tmdb_id=%s %s: %s",
            tmdb_id,
            content_type,
            error,
        )
        return

    if not isinstance(releases_response, dict):
        return
    releases = releases_response.get("releases")
    if not isinstance(releases, list):
        return

    flat_rate_providers: list[dict[str, Any]] = []
    on_demand_providers: list[dict[str, Any]] = []
    seen_provider_ids: set[str] = set()

    for release in releases:
        if not isinstance(release, dict) or _as_int(release.get("tmdb_id")) != tmdb_id:
            continue

        source_name = _as_str(release.get("source_name"))
        if source_name is None:
            continue

        provider = resolve_provider_display_entry_by_name(source_name)
        if provider is None:
            continue

        provider_id = provider.get("provider_id")
        provider_key = str(provider_id)
        if provider_key in seen_provider_ids:
            continue

        source_type = _as_str(release.get("source_type"))
        if _watchmode_release_is_on_demand(source_type):
            on_demand_providers.append(provider)
        else:
            flat_rate_providers.append(provider)
        seen_provider_ids.add(provider_key)

    if not flat_rate_providers and not on_demand_providers:
        return

    response.watch_providers = build_streaming_platform_summary(
        "US",
        flat_rate_providers,
        on_demand_providers,
    )
    primary_provider = response.watch_providers.get("primary_provider")
    if isinstance(primary_provider, dict):
        response.streaming_platform = _as_str(primary_provider.get("provider_name"))
    response.availability_type = _as_str(response.watch_providers.get("primary_provider_type"))
