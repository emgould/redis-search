"""
TMDB Async Wrappers - Firebase Functions compatible async wrappers
Provides standalone async functions for Firebase Functions integration.
These maintain backward compatibility with existing Firebase Functions.
"""

import json
from pathlib import Path
from typing import Any

from utils.get_logger import get_logger

logger = get_logger(__name__)

_UNKNOWN_PROVIDER_ORDER = 10_000


def get_provider_display_map() -> list[dict[str, Any]]:
    _PROVIDER_MAP_PATH = Path(__file__).resolve().parent.parent / "data" / "provider_map.json"
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


def get_master_provider_display_map() -> dict[str, Any]:
    _MASTER_PROVIDER_MAP_PATH = (
        Path(__file__).resolve().parent.parent / "data" / "master_providers.json"
    )
    _MASTER_PROVIDER_DISPLAY_MAP: dict[str, Any] = {}
    try:
        with _MASTER_PROVIDER_MAP_PATH.open(encoding="utf-8") as master_provider_map_file:
            loaded_map = json.load(master_provider_map_file)
            results = loaded_map.get("results", [])
            if isinstance(results, list):
                for provider in results:
                    _MASTER_PROVIDER_DISPLAY_MAP[str(provider.get("provider_id"))] = provider
    except (FileNotFoundError, json.JSONDecodeError, OSError) as error:
        logger.warning(
            "Unable to load master_providers.json, falling back to empty map",
            extra={"error": str(error)},
        )
        _MASTER_PROVIDER_DISPLAY_MAP = {}
    return _MASTER_PROVIDER_DISPLAY_MAP


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


def extract_provider_items(
    provider_ids: list[Any],
    provider_map: dict[str, dict[str, Any]],
    master_provider_map: dict[str, Any],
) -> list[dict[str, Any]]:
    normalized_providers: dict[str, dict[str, Any]] = {}
    for provider_id in provider_ids:
        if provider_id is None:
            continue

        provider_entry = provider_map.get(str(provider_id))

        if provider_entry is None:
            provider_entry = master_provider_map.get(str(provider_id))

        if provider_entry is None:
            normalized_provider_id = (
                int(provider_id)
                if isinstance(provider_id, str) and provider_id.isdigit()
                else provider_id
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
        response["streaming_platform_ids"] = [p.get("provider_id") for p in flat_rate_providers]

    if on_demand_providers:
        response["on_demand_platforms"] = [_provider_display_entry(p) for p in on_demand_providers]
        response["on_demand_platform_ids"] = [p.get("provider_id") for p in on_demand_providers]

    return response


def preprocess_watch_provider_data(
    watch_providers: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    provider_payload = watch_providers

    flat_rate_provider_ids = [p.get("provider_id") for p in provider_payload.get("flatrate", [])]
    on_demand_provider_ids = [
        p.get("provider_id")
        for p in provider_payload.get("buy", []) + provider_payload.get("rent", [])
    ]
    provider_map = get_full_provider_map(get_provider_display_map())
    master_provider_map = get_master_provider_display_map()
    return {
        "flat_rate_providers": extract_provider_items(
            flat_rate_provider_ids, provider_map, master_provider_map
        ),
        "on_demand_providers": extract_provider_items(
            on_demand_provider_ids, provider_map, master_provider_map
        ),
    }
