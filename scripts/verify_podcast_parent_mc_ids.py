#!/usr/bin/env python3
"""
Verify Redis podcast schema and parent_mc_ids linkage for After-Shows docs.

Run from repo root with venv activated:
    python scripts/verify_podcast_parent_mc_ids.py
    python scripts/verify_podcast_parent_mc_ids.py --parent-mc-id tmdb_tv_124101

Exit code 0 on success, 1 on any assertion failure.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os

import _bootstrap
from dotenv import load_dotenv
from redis.asyncio import Redis

_ = _bootstrap

AFTER_SHOWS_TAG = "after_shows"


def _fail(msg: str) -> None:
    print(f"FAIL: {msg}")
    raise AssertionError(msg)


def _normalize_category(value: str) -> str:
    return value.strip().lower().replace("-", "_").replace(" ", "_")


async def _count_index_query(r: Redis, query: str) -> int:
    result = await r.execute_command("FT.SEARCH", "idx:podcasts", query, "LIMIT", "0", "0")
    if not isinstance(result, list) or not result:
        _fail(f"unexpected FT.SEARCH response for query {query!r}: {result!r}")
    return int(result[0])


async def _find_linked_after_shows_doc(r: Redis) -> tuple[str, list[str]] | None:
    cursor = 0
    while True:
        cursor, keys = await r.scan(cursor=cursor, match="podcast:*", count=250)
        if keys:
            docs = await r.json().mget(keys, "$")
            for key, doc_value in zip(keys, docs, strict=True):
                payload = doc_value[0] if isinstance(doc_value, list) and doc_value else doc_value
                if not isinstance(payload, dict):
                    continue
                categories = payload.get("categories")
                if not isinstance(categories, list):
                    continue
                normalized_categories = [
                    _normalize_category(category) for category in categories if isinstance(category, str)
                ]
                if AFTER_SHOWS_TAG not in normalized_categories:
                    continue
                parent_mc_ids = payload.get("parent_mc_ids")
                if isinstance(parent_mc_ids, list):
                    linked = [value for value in parent_mc_ids if isinstance(value, str) and value]
                    if linked:
                        return key, linked
        if cursor == 0:
            break
    return None


async def verify_redis(parent_mc_id: str | None) -> None:
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6380"))
    password = os.getenv("REDIS_PASSWORD") or None

    r = Redis(host=host, port=port, password=password, decode_responses=True)
    try:
        ok = await r.ping()
        if not ok:
            _fail(f"Redis ping failed at {host}:{port}")
        print(f"OK: Redis ping at {host}:{port}")

        info_raw = await r.execute_command("FT.INFO", "idx:podcasts")
        info_text = " ".join(str(part) for part in info_raw) if isinstance(info_raw, list) else str(info_raw)
        if "parent_mc_ids" not in info_text:
            _fail("idx:podcasts schema does not contain parent_mc_ids")
        print("OK: idx:podcasts schema contains parent_mc_ids")

        after_shows_count = await _count_index_query(r, f"@categories:{{{AFTER_SHOWS_TAG}}}")
        if after_shows_count <= 0:
            _fail("idx:podcasts returned zero after_shows documents")
        print(f"OK: idx:podcasts after_shows count = {after_shows_count:,}")

        linked_doc = await _find_linked_after_shows_doc(r)
        if linked_doc is None:
            _fail("no after_shows podcast documents with non-empty parent_mc_ids were found")
        sample_key, sample_parent_ids = linked_doc
        print(f"OK: found linked after_shows doc {sample_key} -> {json.dumps(sample_parent_ids)}")

        if parent_mc_id:
            reverse_count = await _count_index_query(r, f"@parent_mc_ids:{{{parent_mc_id}}}")
            if reverse_count <= 0:
                _fail(f"reverse lookup returned zero docs for {parent_mc_id}")
            print(f"OK: reverse lookup count for {parent_mc_id} = {reverse_count:,}")
    finally:
        await r.aclose()


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Redis parent_mc_ids linkage for podcast docs")
    parser.add_argument(
        "--parent-mc-id",
        default=None,
        help="Optional media mc_id to verify via reverse lookup (e.g. tmdb_tv_124101)",
    )
    args = parser.parse_args()

    env_file = os.getenv("ENV_FILE", "config/local.env")
    load_dotenv(env_file)
    asyncio.run(verify_redis(args.parent_mc_id))
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError:
        raise SystemExit(1)
