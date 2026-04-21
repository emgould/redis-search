#!/usr/bin/env python3
"""Test driver for `GET /api/podcast/related-to-tv`.

Modes:
- One-shot: ``python scripts/test_podcasts_related_to_tv.py tmdb_1399 --limit 5``
- Interactive (no positional ``mc_id``): prompts for ``mc_id [limit]`` per line.

Targets the locally-running web app by default (``http://localhost:9001``).
Override with ``--host`` (e.g. ``--host https://search.example.com``).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass
from typing import Any

import httpx

DEFAULT_HOST = "http://localhost:9001"
DEFAULT_LIMIT = 5
ENDPOINT_PATH = "/api/podcast/related-to-tv"


@dataclass(frozen=True)
class PrintOpts:
    """Render options for endpoint responses."""

    indent: int
    titles_only: bool


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Hit GET /api/podcast/related-to-tv and pretty-print the response.",
    )
    parser.add_argument(
        "mc_id",
        nargs="?",
        help="TV show mc_id (e.g. tmdb_1399). If omitted, drops into interactive mode.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Max podcasts to return (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help=f"Base URL of the web app (default: {DEFAULT_HOST}).",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        help="JSON indent (default: 2).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="HTTP timeout in seconds (default: 15).",
    )
    parser.add_argument(
        "--titles-only",
        action="store_true",
        help="Print just `mc_id  title  (itunes_id)` per row instead of full JSON.",
    )
    return parser.parse_args()


def _print_payload(items: list[dict[str, Any]], opts: PrintOpts) -> None:
    """Render a successful payload using ``opts``."""
    if not items:
        print("[] (no related podcasts found)")
        return

    if opts.titles_only:
        for item in items:
            mc_id = item.get("mc_id", "?")
            title = item.get("title") or "(no title)"
            itunes_id = item.get("itunes_id", "?")
            print(f"{mc_id}\t{title}\t(itunes_id={itunes_id})")
        return

    print(json.dumps(items, indent=opts.indent, ensure_ascii=False))


async def _hit_endpoint(
    client: httpx.AsyncClient,
    host: str,
    mc_id: str,
    limit: int,
    opts: PrintOpts,
) -> int:
    """Call the endpoint once. Returns a process exit code."""
    url = f"{host.rstrip('/')}{ENDPOINT_PATH}"
    params = {"mc_id": mc_id, "limit": limit}

    try:
        resp = await client.get(url, params=params)
    except httpx.HTTPError as exc:
        print(f"HTTP error calling {url}: {exc}", file=sys.stderr)
        return 2

    print(f"# {resp.request.method} {resp.request.url}  ->  {resp.status_code}")

    try:
        payload = resp.json()
    except ValueError:
        print(resp.text)
        return 1 if resp.status_code >= 400 else 0

    if resp.status_code >= 400:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 1

    if not isinstance(payload, list):
        print(f"Expected a JSON array, got {type(payload).__name__}:")
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 1

    _print_payload(payload, opts)
    return 0


async def _interactive_loop(
    client: httpx.AsyncClient,
    host: str,
    default_limit: int,
    opts: PrintOpts,
) -> None:
    """Read `mc_id [limit]` per line from stdin until EOF/blank."""
    print(f"Hitting {host}{ENDPOINT_PATH}")
    print("Enter queries as: <mc_id> [limit]   (e.g. 'tmdb_1399 10')")
    print("Press Enter with no input to quit.\n")

    while True:
        try:
            raw = input("mc_id> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if not raw:
            return

        parts = raw.split()
        mc_id = parts[0]
        limit = default_limit
        if len(parts) >= 2:
            try:
                limit = int(parts[1])
            except ValueError:
                print(f"Invalid limit: {parts[1]!r}; using default {default_limit}.")

        await _hit_endpoint(client, host=host, mc_id=mc_id, limit=limit, opts=opts)
        print()


async def _main() -> int:
    """Entry point."""
    args = _parse_args()
    opts = PrintOpts(indent=args.indent, titles_only=args.titles_only)

    async with httpx.AsyncClient(timeout=args.timeout) as client:
        if args.mc_id:
            return await _hit_endpoint(
                client, host=args.host, mc_id=args.mc_id, limit=args.limit, opts=opts
            )
        await _interactive_loop(
            client, host=args.host, default_limit=args.limit, opts=opts
        )
        return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
