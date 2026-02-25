#!/usr/bin/env python3
"""Interactive script for testing `get_media_details` with live TMDB calls."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from core.normalize import document_to_redis, normalize_document
from utils.genre_mapping import get_genre_mapping_with_fallback

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

load_dotenv(str(PROJECT_ROOT / "config" / "local.env"))

from api.tmdb.core import TMDBService
from contracts.models import MCType


def _parse_args() -> argparse.Namespace:
    """Parse optional CLI defaults for the chat session."""
    parser = argparse.ArgumentParser(description="Interactive TMDB get_media_details tester.")
    parser.add_argument("--region", default="US", help="Default region (default: US).")
    parser.add_argument("--indent", type=int, default=2, help="JSON indent (default: 2).")
    return parser.parse_args()


def _normalize_media_type(raw_type: str) -> str:
    """Normalize and validate user-provided media type."""
    value = raw_type.strip().lower()
    if value not in {"movie", "tv"}:
        raise ValueError(f"Unsupported media type: {raw_type!r}")
    return value


def _parse_query(raw: str, default_region: str) -> tuple[int, str, str]:
    """Parse a query like `<tmdb_id> <movie|tv> [region]`."""
    parts = raw.strip().split()
    if len(parts) < 2:
        raise ValueError("Expected: <tmdb_id> <movie|tv> [region]")

    tmdb_id = int(parts[0])
    media_type = _normalize_media_type(parts[1])
    region = (parts[2].upper() if len(parts) > 2 else default_region).upper()
    return tmdb_id, media_type, region


def _to_serializable(payload: Any) -> dict[str, Any] | None:
    """Convert a result model to a serializable dict."""
    if payload is None:
        return None
    if hasattr(payload, "to_dict"):
        return payload.to_dict()  # type: ignore[no-any-return]
    if hasattr(payload, "dict"):
        return payload.dict()  # type: ignore[no-any-return]
    if isinstance(payload, dict):
        return payload
    return {"value": str(payload)}


def _media_type_to_enum(media_type: str) -> MCType:
    """Convert CLI media type to MCType enum."""
    return MCType.TV_SERIES if media_type == "tv" else MCType.MOVIE


async def run_once(tmdb_id: int, media_type: str, region: str, indent: int) -> int:
    """Run one lookup and print the response."""
    if not os.getenv("TMDB_READ_TOKEN"):
        print(
            "TMDB_READ_TOKEN is not set. Source config/local.env or export it in your shell.",
            file=sys.stderr,
        )
        return 1

    service = TMDBService()
    details = await service.get_media_details(
        tmdb_id=tmdb_id,
        media_type=_media_type_to_enum(media_type),
        region=region,
        no_cache=True,
    )

    item_dict = _to_serializable(details)
    print(json.dumps(item_dict, indent=indent, default=str, ensure_ascii=False))

    if item_dict is None:
        print("Failed to serialize media details.", file=sys.stderr)
        sys.exit(2)

    item_dict["_media_type"] = media_type
    genre_mapping = await get_genre_mapping_with_fallback(allow_fallback=True)
    doc = normalize_document(item_dict, genre_mapping=genre_mapping)
    if doc is None:
        print("Normalizer returned None — item did not produce a document.", file=sys.stderr)
        sys.exit(2)

    redis_doc = document_to_redis(doc)
    print(json.dumps(redis_doc, indent=indent, default=str, ensure_ascii=False))

    return 0 if details and not details.error else 2


async def main() -> None:
    """Run the interactive chat session."""
    args = _parse_args()
    print("TMDB Media Details Chat")
    print("Enter queries as: <tmdb_id> <movie|tv> [region]")
    print("Examples: '559 tv' or '234234324 movie US'")
    print("Press Enter with no input to quit.\n")

    while True:
        try:
            raw = input("query> ").strip()
            if not raw:
                print("Goodbye.")
                return

            tmdb_id, media_type, region = _parse_query(raw, default_region=args.region)
            await run_once(
                tmdb_id=tmdb_id,
                media_type=media_type,
                region=region,
                indent=args.indent,
            )
        except KeyboardInterrupt:
            print("\nInterrupted. Goodbye.")
            return
        except ValueError as exc:
            print(f"Invalid input: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
