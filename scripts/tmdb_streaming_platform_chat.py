#!/usr/bin/env python3
"""
Interactive chat-style script for querying streaming platform summaries from TMDB.

Example usage:
  python tmdb_streaming_platform_chat.py

At the prompt:
  - Enter a TMDB id directly to use defaults.
  - Enter: "<tmdb_id> <content_type> <watch_region>" for custom values.
  - Use "/type movie|tv" to change content type default.
  - Use "/region XX" to change watch_region default (e.g. US, CA).
  - Use "/help" to view commands.
  - Use "/exit" or "/quit" to stop.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Literal

from api.tmdb.get_providers import get_streaming_platform_summary_for_title

ContentType = Literal["movie", "tv"]


def _to_int(value: str) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None


def _normalize_content_type(value: str) -> ContentType | None:
    normalized = value.strip().lower()
    if normalized in {"movie", "tv"}:
        return "movie" if normalized == "movie" else "tv"
    return None

def _normalize_region(value: str) -> str:
    normalized = value.strip().upper()
    return normalized if normalized else "US"


async def _query_platform_summary(
    tmdb_id: int, content_type: ContentType, watch_region: str
) -> dict[str, Any] | None:
    return await get_streaming_platform_summary_for_title(
        tmdb_id=tmdb_id,
        content_type=content_type,
        watch_region=watch_region,
    )


def _print_help() -> None:
    print(
        """
Commands:
  <id>                     Query by default content type and region.
  <id> <movie|tv>          Query with a custom content type.
  <id> <movie|tv> <REGION> Query with custom content type and region.
  /type <movie|tv>         Set default content type.
  /region <REGION>         Set default watch_region.
  /defaults                Show current defaults.
  /help                    Show this help.
  /quit, /exit             Exit.
"""
    )


def _print_result(result: dict[str, Any] | None) -> None:
    if result is None:
        print("Failed to fetch streaming platform summary.")
        return

    provider_ids = result.get("streaming_platform_ids", [])
    provider_names = result.get("streaming_platforms", [])
    priorities = result.get("streaming_platform_display_priorities", {})
    priority_order = result.get("streaming_platform_display_priority_order", {})
    provider_count = len(provider_ids)

    print(
        json.dumps(
            {
                "primary_provider": result.get("primary_provider"),
                "primary_provider_id": result.get("primary_provider_id"),
                "primary_provider_type": result.get("primary_provider_type"),
                "streaming_platform_ids": provider_ids,
                "streaming_platforms": result.get("streaming_platforms", []),
                "on_demand_platform_ids": result.get("on_demand_platform_ids", []),
                "on_demand_platforms": result.get("on_demand_platforms", []),
                "watch_region": result.get("watch_region"),
            },
            indent=2,
        )
    )
    print("Providers:")
    for index, provider_id in enumerate(provider_ids):
        priority = priorities.get(provider_id)
        rank = priority_order.get(provider_id)
        platform_name = (
            provider_names[index] if index < len(provider_names) else f"Provider {provider_id}"
        )
        print(
            f"  - id={provider_id}, name={platform_name}, display_priority={priority}, rank={rank}"
        )
    print(f"Total providers found: {provider_count}")


async def main() -> None:
    print("TMDB streaming platform summary chat")
    print("Type /help for commands.")

    content_type: ContentType = "movie"
    watch_region = "US"

    while True:
        raw = input("tmdb> ").strip()
        if not raw:
            continue

        lowered = raw.lower()
        if lowered in {"/quit", "/exit", "quit", "exit"}:
            break

        if lowered == "/help":
            _print_help()
            continue

        if lowered == "/defaults":
            print(
                json.dumps(
                    {"content_type": content_type, "watch_region": watch_region},
                    indent=2,
                )
            )
            continue

        if lowered.startswith("/type "):
            new_type = _normalize_content_type(raw.split(" ", 1)[1] if " " in raw else "")
            if new_type is None:
                print('Invalid content type. Use "movie" or "tv".')
                continue
            content_type = new_type
            print(f"Default content_type set to {content_type}")
            continue

        if lowered.startswith("/region "):
            new_region = _normalize_region(raw.split(" ", 1)[1] if " " in raw else "")
            watch_region = new_region
            print(f"Default watch_region set to {watch_region}")
            continue

        parts = raw.split()
        if not parts:
            continue

        tmdb_id = _to_int(parts[0])
        if tmdb_id is None:
            print("Please enter a valid TMDB ID (numeric).")
            continue

        command_content_type = content_type
        command_region = watch_region

        if len(parts) >= 2:
            parsed_type = _normalize_content_type(parts[1])
            if parsed_type is None:
                print("Second argument must be movie or tv when provided.")
                continue
            command_content_type = parsed_type

        if len(parts) >= 3:
            command_region = _normalize_region(parts[2])

        result = await _query_platform_summary(
            tmdb_id=tmdb_id,
            content_type=command_content_type,
            watch_region=command_region,
        )
        _print_result(result)


if __name__ == "__main__":
    asyncio.run(main())
