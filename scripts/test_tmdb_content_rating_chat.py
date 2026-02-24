"""Interactive script for manually testing TMDB content ratings.

Usage examples:
  python scripts/test_tmdb_content_rating_chat.py
  python scripts/test_tmdb_content_rating_chat.py --tmdb-id 1396 --media-type tv --region US
"""

from __future__ import annotations

import argparse
import asyncio
from typing import Literal

from api.tmdb.wrappers import get_content_rating_async


def _normalize_media_type(raw: str) -> Literal["tv", "movie"]:
    """Normalize and validate user-provided media type."""
    value = raw.strip().lower()
    if value not in {"tv", "movie"}:
        raise ValueError(f"Unsupported media type: {raw!r}")
    return value


async def _fetch_content_rating(tmdb_id: int, media_type: str, region: str) -> dict[str, str | None] | None:
    """Fetch and return cached/remote content rating details."""
    return await get_content_rating_async(tmdb_id=tmdb_id, region=region, content_type=media_type)


def _print_payload(tmdb_id: int, media_type: str, region: str, payload: dict[str, str | None] | None) -> None:
    """Print a consistent result format for each query."""
    print(f"\nTMDB {media_type.upper()} {tmdb_id} ({region})")
    if payload is None:
        print("  rating: None")
        print("  release_date: None")
        return

    print(f'  rating: {payload.get("rating")!r}')
    print(f'  release_date: {payload.get("release_date")!r}')


def _prompt_tmdb_id() -> int:
    """Prompt until the user provides a valid integer TMDB ID."""
    while True:
        raw_tmdb_id = input("TMDB ID (required, numeric): ").strip()
        if not raw_tmdb_id:
            print("Please provide a TMDB ID.")
            continue
        try:
            return int(raw_tmdb_id)
        except ValueError:
            print("Invalid TMDB ID; enter an integer (for example, 1396).")


def _prompt_media_type() -> str:
    """Prompt until the user provides a valid media type."""
    while True:
        raw_type = input("Media type [tv/movie] (default: tv): ").strip().lower() or "tv"
        try:
            return _normalize_media_type(raw_type)
        except ValueError as exc:
            print(exc)


def _prompt_region(default_region: str = "US") -> str:
    """Prompt for region code."""
    return input(f"Region code (default: {default_region}): ").strip().upper() or default_region


async def run_interactive() -> None:
    """Run an interactive loop for manual testing."""
    print("\nTMDB Content Rating Chat Script")
    print("Enter Ctrl+C or type 'quit' at any prompt to stop.\n")

    while True:
        try:
            raw_tmdb = input("TMDB ID (or 'quit'): ").strip()
            if raw_tmdb.lower() in {"quit", "exit", "q"}:
                print("Goodbye.")
                return
            if raw_tmdb:
                try:
                    tmdb_id = int(raw_tmdb)
                except ValueError:
                    print("TMDB ID must be an integer.")
                    continue
            else:
                tmdb_id = _prompt_tmdb_id()

            raw_media_type = input("Media type [tv/movie] (default: tv): ").strip()
            if raw_media_type.lower() in {"quit", "exit", "q"}:
                print("Goodbye.")
                return
            media_type = _normalize_media_type(raw_media_type or "tv")

            region = input("Region (default: US): ").strip().upper() or "US"
            if region.lower() in {"quit", "exit", "q"}:
                print("Goodbye.")
                return
            payload = await _fetch_content_rating(tmdb_id=tmdb_id, media_type=media_type, region=region)
            _print_payload(tmdb_id=tmdb_id, media_type=media_type, region=region, payload=payload)

            while True:
                again = input("Another query? [y/N]: ").strip().lower()
                if again in {"n", "no", "", "q", "quit", "exit"}:
                    if again in {"q", "quit", "exit"}:
                        print("Goodbye.")
                    return
                if again in {"y", "yes"}:
                    break
                print("Please answer 'y' or 'n'.")
        except KeyboardInterrupt:
            print("\nInterrupted. Goodbye.")
            return


def parse_args() -> argparse.Namespace:
    """Parse optional one-shot mode arguments."""
    parser = argparse.ArgumentParser(
        description="Test TMDB content ratings for TV and movie IDs.",
    )
    parser.add_argument(
        "--tmdb-id",
        type=int,
        help="TMDB ID for one-shot testing mode.",
    )
    parser.add_argument(
        "--media-type",
        choices=["tv", "movie"],
        default="tv",
        help="Media type to query.",
    )
    parser.add_argument(
        "--region",
        default="US",
        help="ISO 3166-1 region code.",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Run interactive loop instead of one-shot mode.",
    )
    return parser.parse_args()


async def run_once(tmdb_id: int, media_type: str, region: str) -> None:
    """Run a single rating lookup."""
    payload = await _fetch_content_rating(tmdb_id=tmdb_id, media_type=media_type, region=region)
    _print_payload(tmdb_id=tmdb_id, media_type=media_type, region=region, payload=payload)


async def main() -> None:
    """Entrypoint."""
    args = parse_args()
    if args.interactive or args.tmdb_id is None:
        await run_interactive()
        return

    await run_once(tmdb_id=args.tmdb_id, media_type=args.media_type, region=args.region.upper())


if __name__ == "__main__":
    asyncio.run(main())

