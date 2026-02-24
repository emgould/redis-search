"""
TMDB Async Wrappers - Firebase Functions compatible async wrappers
Provides standalone async functions for Firebase Functions integration.
These maintain backward compatibility with existing Firebase Functions.
"""

import json
from pathlib import Path
from typing import Any

from api.tmdb.wrappers import (
    get_movie_now_playing_ids_async,
    get_providers_async,
    get_watch_providers_for_title_async,
)
from contracts.models import (
    MCType,
)
from utils.get_logger import get_logger

from .update_provider_map import MasterEntry, reconcile_provider_map

logger = get_logger(__name__)

_UNKNOWN_PROVIDER_ORDER = 10_000


def get_provider_display_map() -> list[dict[str, Any]]:
    _PROVIDER_MAP_PATH = Path(__file__).resolve().parent / "data" / "provider_map.json"
    _PROVIDER_DISPLAY_MAP: list[dict[str, Any]] = []
    try:
        with _PROVIDER_MAP_PATH.open(encoding="utf-8") as provider_map_file:
            loaded_map = json.load(provider_map_file)
            if isinstance(loaded_map, list):
                _PROVIDER_DISPLAY_MAP = [
                    provider for provider in loaded_map if isinstance(provider, dict)
                ]
            else:
                logger.warning("provider_map.json has unexpected format, expected a list")
    except (FileNotFoundError, json.JSONDecodeError, OSError) as error:
        logger.warning(
            "Unable to load provider_map.json, falling back to empty map",
            extra={"error": str(error)},
        )
        _PROVIDER_DISPLAY_MAP = []
    return _PROVIDER_DISPLAY_MAP


def get_full_provider_map(provider_display_map: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    aggregator_map = dict[str, dict[str, Any]]()
    for provider in provider_display_map:
        aggregator_ids = [p.get("id") for p in provider.get("packages", [])] + [
            p.get("id") for p in provider.get("channels", [])
        ]
        aggregator_map[str(provider.get("provider_id"))] = provider

        for aggregator_id in aggregator_ids:
            aggregator_map[aggregator_id] = provider
    return aggregator_map


def _extract_provider_items(
    provider_ids: list[Any],
    provider_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized_providers: dict[str, dict[str, Any]] = {}
    for provider_id in provider_ids:
        if provider_id is None:
            continue

        provider_entry = provider_map.get(str(provider_id))
        if provider_entry is None and isinstance(provider_id, str):
            provider_entry = provider_map.get(int(provider_id))
        if provider_entry is None and isinstance(provider_id, int):
            provider_entry = provider_map.get(provider_id)

        if provider_entry is None:
            normalized_provider_id = (
                int(provider_id) if isinstance(provider_id, str) and provider_id.isdigit() else provider_id
            )
            provider_entry_display = {
                "provider_name": f"Provider {normalized_provider_id}",
                "provider_id": normalized_provider_id,
                "logo_path": None,
                "display_priority": _UNKNOWN_PROVIDER_ORDER,
                "mkt_share_order": _UNKNOWN_PROVIDER_ORDER,
            }
        else:
            provider_entry_display = _provider_display_entry(provider_entry)

        provider_key = str(provider_entry_display["provider_id"])
        if provider_key not in normalized_providers:
            normalized_providers[provider_key] = provider_entry_display

    return sorted(
        normalized_providers.values(),
        key=lambda x: x.get("display_priority", _UNKNOWN_PROVIDER_ORDER),
    )


def _provider_display_entry(provider: dict[str, Any]) -> dict[str, Any]:
    mkt_share_order = provider.get("mkt_share_order")
    if mkt_share_order is None:
        mkt_share_order = provider.get("display_priority")
    if mkt_share_order is None:
        mkt_share_order = _UNKNOWN_PROVIDER_ORDER

    return {
        "provider_name": provider.get("base_brand") or provider.get("provider_name"),
        "provider_id": provider.get("provider_id"),
        "logo_path": provider.get("logo_path"),
        "display_priority": mkt_share_order,
        "mkt_share_order": mkt_share_order,
    }


def _empty_streaming_platform_summary(watch_region: str) -> dict[str, Any]:
    return {
        "streaming_platform_ids": [],
        "streaming_platforms": [],
        "primary_provider": None,
        "primary_provider_id": None,
        "primary_provider_type": None,
        "watch_region": watch_region,
        "on_demand_platform_ids": [],
        "on_demand_platforms": [],
    }


async def fetch_watch_provider_data_for_title(
    tmdb_id: int, content_type: str, watch_region: str = "US"
) -> dict[str, list[dict[str, Any]]]:
    providers_response = await get_watch_providers_for_title_async(
        tmdb_id, content_type, region=watch_region
    )
    provider_payload = providers_response.get("watch_providers", {})

    flat_rate_provider_ids = [p.get("provider_id") for p in provider_payload.get("flatrate", [])]
    on_demand_provider_ids = [
        p.get("provider_id") for p in provider_payload.get("buy", []) + provider_payload.get("rent", [])
    ]
    provider_map = get_full_provider_map(get_provider_display_map())

    return {
        "flat_rate_providers": _extract_provider_items(
            flat_rate_provider_ids, provider_map
        ),
        "on_demand_providers": _extract_provider_items(
            on_demand_provider_ids, provider_map
        ),
    }


def build_streaming_platform_summary(
    watch_region: str,
    flat_rate_providers: list[dict[str, Any]],
    on_demand_providers: list[dict[str, Any]],
) -> dict[str, Any]:
    response = _empty_streaming_platform_summary(watch_region)

    primary_provider_full = flat_rate_providers[0] if flat_rate_providers else {}
    if primary_provider_full:
        response["primary_provider"] = _provider_display_entry(primary_provider_full)
        response["primary_provider_id"] = primary_provider_full.get("provider_id")
        response["primary_provider_type"] = "flatrate"

    if flat_rate_providers:
        response["streaming_platforms"] = [_provider_display_entry(p) for p in flat_rate_providers]
        response["streaming_platform_ids"] = [
            p.get("provider_id") for p in flat_rate_providers
        ]

    if on_demand_providers:
        response["on_demand_platforms"] = [
            _provider_display_entry(p) for p in on_demand_providers
        ]
        response["on_demand_platform_ids"] = [
            p.get("provider_id") for p in on_demand_providers
        ]

    return response


async def get_streaming_platform_summary_for_title(
    tmdb_id: int, content_type: str, watch_region: str = "US"
) -> dict[str, Any] | None:
    """
    Resolve streaming platform ids, platform names, and primary platform for a title.

    The summary is built from:
    - watch/providers/{movie|tv} (via watch_region) for canonical ids/names
    - watch providers on the title (primary/flatrate/rent/buy)

    Args:
        tmdb_id: TMDB ID of the movie or TV title
        content_type: "movie" or "tv"
        watch_region: TMDB watch provider region (default: "US")

    Returns:
    Dict with:
        {
          "streaming_platform_ids": list[int],
          "streaming_platforms": list[dict[str, Any]],
          "primary_provider": dict[str, Any] | None,
          "primary_provider_id": int | None,
          "primary_provider_type": str | None,
          "on_demand_platform_ids": list[int],
          "on_demand_platforms": list[dict[str, Any]],
        }
    `streaming_platform_ids`/`streaming_platforms` are resolved from the selected
    primary platform plus its mapped aggregator ids.

    Note:
      - Movie content uses TMDB's `display_priority` values when ranking providers.
      - TV content prefers network-derived base-provider ranking from the static display map.
    """
    try:
        if content_type not in {"movie", "tv"}:
            raise ValueError(f"Invalid content_type: {content_type}")

        provider_data = await fetch_watch_provider_data_for_title(
            tmdb_id, content_type, watch_region
        )
        response = build_streaming_platform_summary(
            watch_region=watch_region,
            flat_rate_providers=provider_data["flat_rate_providers"],
            on_demand_providers=provider_data["on_demand_providers"],
        )

        is_in_theater = False
        if content_type == "movie":
            try:
                now_playing_movie_ids = await get_movie_now_playing_ids_async(region="US")
                is_in_theater = tmdb_id in now_playing_movie_ids
            except Exception as error:
                logger.warning(
                    "Unable to verify now-playing status for tmdb_id=%s, region=%s: %s",
                    tmdb_id,
                    watch_region,
                    error,
                )
            if is_in_theater:
                response["primary_provider_type"] = "in theater"
                if (
                    not response["primary_provider"]
                    and response["on_demand_platforms"]
                    and len(response["on_demand_platform_ids"]) > 0
                ):
                    response["primary_provider"] = response["on_demand_platforms"][0]
                    response["primary_provider_id"] = response["on_demand_platform_ids"][0]

        if (
            not response["primary_provider"]
            and response["on_demand_platforms"]
            and len(response["on_demand_platform_ids"]) > 0
        ):
            response["primary_provider"] = response["on_demand_platforms"][0]
            response["primary_provider_id"] = response["on_demand_platform_ids"][0]
            response["primary_provider_type"] = "on_demand"

        return response
    except Exception as e:
        logger.error(
            "Error generating streaming platform summary for %s %s: %s",
            content_type,
            tmdb_id,
            e,
        )
        return None


async def refresh_provider_map(region: str = "US") -> dict[str, Any]:
    """
    Fetch the full TMDB watch-provider catalogue for both movie and TV,
    deduplicate, and reconcile against provider_map.json.

    Args:
        region: TMDB watch-provider region (default "US").

    Returns:
        Summary dict with counts for skipped, attached, added, and total entries.
    """
    provider_map_path = Path(__file__).resolve().parent / "data" / "provider_map.json"

    movie_response = await get_providers_async(MCType.MOVIE, region)
    tv_response = await get_providers_async(MCType.TV_SERIES, region)

    seen_ids: set[int] = set()
    master_entries: list[MasterEntry] = []
    for provider in [*movie_response.results, *tv_response.results]:
        if provider.provider_id not in seen_ids:
            seen_ids.add(provider.provider_id)
            master_entries.append(
                {
                    "provider_id": provider.provider_id,
                    "provider_name": provider.provider_name,
                    "logo_path": provider.logo_path,
                }
            )

    report = reconcile_provider_map(master_entries, provider_map_path)

    logger.info(
        "Provider map refreshed: %d skipped, %d attached, %d added, %d total",
        len(report["skipped"]),
        len(report["attached"]),
        len(report["added"]),
        report["total_entries"],
    )

    return {
        "skipped": len(report["skipped"]),
        "attached": len(report["attached"]),
        "added": len(report["added"]),
        "total_entries": report["total_entries"],
    }
