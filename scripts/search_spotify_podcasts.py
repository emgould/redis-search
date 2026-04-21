#!/usr/bin/env python3
"""
Search Spotify podcast shows and print linked podcast Redis docs as JSON.

Run from repo root with venv activated:
    ENV_FILE=config/local.env python scripts/search_spotify_podcasts.py "survivor tv show"
    ENV_FILE=config/local.env python scripts/search_spotify_podcasts.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

import _bootstrap
from dotenv import load_dotenv

from api.subapi.spotify.wrappers import spotify_wrapper

_ = _bootstrap


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Search Spotify podcast shows and print linked podcast Redis docs.",
    )
    parser.add_argument(
        "query",
        nargs="?",
        help="Search query. If omitted, the script prompts for one.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Number of results to return per Spotify type (default: 20).",
    )
    parser.add_argument(
        "--include-episodes",
        action="store_true",
        help="Include episode matches in addition to show results.",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        help="JSON indent (default: 2).",
    )
    parser.add_argument(
        "--raw-spotify",
        action="store_true",
        help="Print raw Spotify search results instead of linked Redis docs.",
    )
    return parser.parse_args()


def _resolve_query(raw_query: str | None) -> str:
    if raw_query and raw_query.strip():
        return raw_query.strip()

    try:
        typed_query = input("Spotify show search: ").strip()
    except EOFError as exc:
        raise SystemExit("No query provided.") from exc

    if not typed_query:
        raise SystemExit("No query provided.")
    return typed_query


async def _run() -> int:
    args = _parse_args()

    env_file = os.getenv("ENV_FILE", "config/local.env")
    load_dotenv(env_file)

    if not os.getenv("SPOTIFY_CLIENT_ID") or not os.getenv("SPOTIFY_CLIENT_SECRET"):
        print(
            f"Spotify credentials are not set. Load them first (ENV_FILE={env_file}).",
            file=sys.stderr,
        )
        return 1

    query = _resolve_query(args.query)
    if args.raw_spotify:
        response = await spotify_wrapper.search_podcasts(
            query=query,
            limit=args.limit,
            include_episodes=args.include_episodes,
        )
        payload: object = response.model_dump(mode="json")
    else:
        payload = await spotify_wrapper.search_podcast_redis_docs(
            query=query,
            limit=args.limit,
            include_episodes=args.include_episodes,
        )

    print(json.dumps(payload, indent=args.indent, ensure_ascii=False))
    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
