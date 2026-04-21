#!/usr/bin/env python3
"""
Add ``spotify_url`` as an indexed TAG field on the live ``idx:podcasts`` index.

Run from repo root with venv activated:
    ENV_FILE=config/local.env python scripts/alter_podcast_index_add_spotify_url.py
"""

from __future__ import annotations

import asyncio
import os

import _bootstrap
from dotenv import load_dotenv
from redis.asyncio import Redis

_ = _bootstrap


async def main() -> int:
    env_file = os.getenv("ENV_FILE", "config/local.env")
    load_dotenv(env_file)

    redis = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )
    try:
        await redis.execute_command(
            "FT.ALTER",
            "idx:podcasts",
            "SCHEMA",
            "ADD",
            "$.spotify_url",
            "AS",
            "spotify_url",
            "TAG",
        )
        print("Added spotify_url TAG field to idx:podcasts")
    except Exception as exc:
        message = str(exc)
        if "Duplicate" in message or "already exists" in message.lower():
            print("spotify_url TAG field already present on idx:podcasts")
            return 0
        raise
    finally:
        await redis.aclose()

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
