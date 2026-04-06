#!/usr/bin/env python3
"""
Verify title_compact derivation, query builders, ranking, and optional Redis state.

Run from repo root with venv activated:
    python scripts/verify_title_compact.py
    python scripts/verify_title_compact.py --redis   # also ping Redis + sample keys

Exit code 0 on success, 1 on any assertion failure.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from collections.abc import Awaitable

# Quiet module loggers (IPTC, search query builders, etc.) during import and tests.
logging.basicConfig(level=logging.WARNING)
logging.disable(logging.INFO)

# Repo imports (same pattern as other scripts)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv  # noqa: E402

from core.normalize import compact_title  # noqa: E402
from core.ranking import score_media_result  # noqa: E402
from core.search_queries import (  # noqa: E402
    _is_compact_query,
    build_autocomplete_query,
    build_fuzzy_fulltext_query,
    build_minimal_autocomplete_query,
)
from services.search_service import _rerank_results  # noqa: E402


def _fail(msg: str) -> None:
    print(f"FAIL: {msg}")
    raise AssertionError(msg)


def test_compact_title() -> None:
    cases = [
        ("Good Will Hunting", "goodwillhunting"),
        ("Spider-Man: No Way Home", "spidermannowayhome"),
        ("It's Complicated", "itscomplicated"),
        ("", ""),
    ]
    for raw, want in cases:
        got = compact_title(raw)
        if got != want:
            _fail(f"compact_title({raw!r}) = {got!r}, want {want!r}")


def test_compact_query_detection() -> None:
    if not _is_compact_query(["goodwillhunting"]):
        _fail("single long token should be compact-intent")
    if _is_compact_query(["good", "will"]):
        _fail("two words should not be compact-intent")
    if _is_compact_query(["abc"]):
        _fail("short single token should not be compact-intent")


def test_query_builders() -> None:
    q = "goodwillhunting"
    minimal = build_minimal_autocomplete_query(q)
    if "@title_compact:" not in minimal or "@search_title:" not in minimal:
        _fail(f"minimal query missing compact clause: {minimal!r}")

    auto_no_tags = build_autocomplete_query(q, include_tag_fields=False)
    if "@title_compact:" not in auto_no_tags:
        _fail(f"autocomplete (no tags) missing compact: {auto_no_tags!r}")

    auto_tags = build_autocomplete_query(q, include_tag_fields=True)
    if "@title_compact:" not in auto_tags:
        _fail(f"autocomplete (tags) missing compact: {auto_tags!r}")

    spaced = "good will hunting"
    minimal_spaced = build_minimal_autocomplete_query(spaced)
    if "@title_compact:" not in minimal_spaced:
        _fail(f"spaced query MUST add title_compact (collapse all words): {minimal_spaced!r}")

    mistyped = "goodwill hunting"
    minimal_mistyped = build_minimal_autocomplete_query(mistyped)
    if "@title_compact:" not in minimal_mistyped:
        _fail(f"mistyped query MUST add title_compact: {minimal_mistyped!r}")

    auto_mistyped = build_autocomplete_query(mistyped, include_tag_fields=True)
    if "@title_compact:" not in auto_mistyped:
        _fail(f"autocomplete mistyped MUST add title_compact: {auto_mistyped!r}")

    fuzzy_mistyped = build_fuzzy_fulltext_query(mistyped)
    if "@title_compact:" not in fuzzy_mistyped:
        _fail(f"fuzzy mistyped MUST add title_compact: {fuzzy_mistyped!r}")

    fuzzy = build_fuzzy_fulltext_query(q)
    if "@title_compact:" not in fuzzy:
        _fail(f"fuzzy missing compact: {fuzzy!r}")


def test_score_media_result() -> None:
    doc = {
        "search_title": "Good Will Hunting",
        "title": "Good Will Hunting",
        "title_compact": "goodwillhunting",
        "year": 1997,
        "popularity": 10.0,
    }
    tier, _, _ = score_media_result("goodwillhunting", doc)
    if tier != 1:
        _fail(f"compact exact should be tier 1, got {tier}")

    tier2, _, _ = score_media_result("goodwillhunt", doc)
    if tier2 not in (11, 13):
        _fail(f"compact prefix/substring expected tier 11 or 13, got {tier2}")


def test_rerank_results() -> None:
    results = [
        {"search_title": "Other Movie", "title": "Other Movie", "release_date": "2020-01-01"},
        {
            "search_title": "Good Will Hunting",
            "title": "Good Will Hunting",
            "title_compact": "goodwillhunting",
            "release_date": "1997-12-25",
        },
    ]
    out = _rerank_results(results, ["goodwillhunting"], limit=10)
    if out[0].get("search_title") != "Good Will Hunting":
        _fail(f"rerank should promote compact match first, got {out[0]!r}")


async def verify_redis_optional() -> None:
    from redis.asyncio import Redis

    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6380"))
    password = os.getenv("REDIS_PASSWORD") or None

    r = Redis(host=host, port=port, password=password, decode_responses=True)
    try:
        ping_res = r.ping()
        ok = await ping_res if isinstance(ping_res, Awaitable) else ping_res
        if not ok:
            print("WARN: Redis ping returned falsy — skipping Redis checks")
            return
        print(f"OK: Redis ping at {host}:{port}")

        info_raw = await r.execute_command("FT.INFO", "idx:media")
        if not isinstance(info_raw, list):
            print("WARN: FT.INFO idx:media unexpected shape — skipping schema check")
            return
        # FT.INFO returns alternating key/value list
        info_str = " ".join(str(x) for x in info_raw)
        if "title_compact" in info_str:
            print("OK: idx:media schema mentions title_compact")
        else:
            print(
                "WARN: idx:media FT.INFO does not mention title_compact "
                "(run backfill with --alter-index if not yet applied)"
            )

        cursor = 0
        sample_key: str | None = None
        for _ in range(5):
            cursor, keys = await r.scan(cursor=cursor, match="media:*", count=50)
            if keys:
                sample_key = keys[0]
                break
            if cursor == 0:
                break
        if not sample_key:
            print("WARN: no media:* keys — skipping JSON.GET sample")
            return

        raw = await r.execute_command(
            "JSON.GET", sample_key, "$.title_compact", "$.search_title"
        )
        print(f"OK: sample key {sample_key} title_compact/search_title slice = {raw!r}")
    finally:
        await r.aclose()


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify title_compact implementation")
    parser.add_argument(
        "--redis",
        action="store_true",
        help="Also connect to Redis (REDIS_* from ENV_FILE) and sanity-check index",
    )
    args = parser.parse_args()

    env_file = os.getenv("ENV_FILE", "config/local.env")
    load_dotenv(env_file)

    tests = [
        ("compact_title", test_compact_title),
        ("compact query detection", test_compact_query_detection),
        ("query builders", test_query_builders),
        ("score_media_result", test_score_media_result),
        ("_rerank_results", test_rerank_results),
    ]
    for name, fn in tests:
        fn()
        print(f"OK: {name}")

    if args.redis:
        asyncio.run(verify_redis_optional())

    print("All checks passed.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError:
        raise SystemExit(1)
