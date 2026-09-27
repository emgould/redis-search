"""Apply FlixPatrol history onto Redis media documents.

Title files come from ``api.subapi.flixpatrol.history.transform_flixpatrol_rankings``.
Set ``FLIXPATROL_TITLES_DIR`` to override the default ``data/flixpatrol/us-titles``
directory under the repo root.
"""

import asyncio
import json
import os
from pathlib import Path
from typing import cast

from redis.asyncio import Redis
from redis.commands.json.commands import JsonType


def _repo_titles_dir() -> Path:
    """Default ``data/flixpatrol/us-titles`` under the repository root."""
    return Path(__file__).resolve().parents[2] / "data" / "flixpatrol" / "us-titles"


def flixpatrol_titles_dir() -> Path | None:
    """Return the title-file directory when configured or populated."""
    raw = os.getenv("FLIXPATROL_TITLES_DIR")
    if raw is not None and raw.strip():
        return Path(raw)
    titles_root = _repo_titles_dir()
    has_files = next(titles_root.glob("*.json"), None) is not None
    if titles_root.is_dir() and has_files:
        return titles_root
    return None


def _read_object(path: Path) -> dict[str, object] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return cast(dict[str, object], payload)


def flixpatrol_data_from_title_file(path: Path) -> dict[str, object] | None:
    """Return the ``data`` object from one title file."""
    payload = _read_object(path)
    if payload is None:
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    return cast(dict[str, object], data)


def attach_flixpatrol(
    redis_doc: dict[str, object],
    existing_doc: dict[str, object] | None,
    titles_dir: Path | None,
) -> None:
    """Set ``flixpatrol`` from a title file, or copy it from the existing document."""
    mc_id = redis_doc.get("mc_id")
    if titles_dir is not None and isinstance(mc_id, str) and mc_id:
        from_file = flixpatrol_data_from_title_file(titles_dir / f"{mc_id}.json")
        if from_file is not None:
            redis_doc["flixpatrol"] = from_file
            return
    if existing_doc is None:
        return
    current = existing_doc.get("flixpatrol")
    if isinstance(current, dict):
        redis_doc["flixpatrol"] = current


async def apply_flixpatrol_title_files(redis: Redis, titles_dir: Path) -> int:
    """Set ``$.flixpatrol`` on existing ``media:{mc_id}`` documents."""
    pending: list[tuple[str, dict[str, object]]] = []
    for path in sorted(titles_dir.glob("*.json")):
        payload = _read_object(path)
        if payload is None:
            continue
        data = payload.get("data")
        mc_id = payload.get("mc_id")
        if not isinstance(data, dict) or not isinstance(mc_id, str) or not mc_id:
            continue
        key = f"media:{mc_id}"
        if not await redis.exists(key):
            continue
        pending.append((key, cast(dict[str, object], data)))
    updated = 0
    for offset in range(0, len(pending), 100):
        chunk = pending[offset : offset + 100]
        pipe = redis.pipeline()
        for key, data in chunk:
            pipe.json().set(key, "$.flixpatrol", cast(JsonType, data))
        await pipe.execute()
        updated += len(chunk)
    return updated


async def _apply_from_env() -> int:
    titles = flixpatrol_titles_dir()
    if titles is None:
        raise SystemExit("FLIXPATROL_TITLES_DIR is required or us-titles must exist")
    redis = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )
    try:
        return await apply_flixpatrol_title_files(redis, titles)
    finally:
        await redis.aclose()


if __name__ == "__main__":
    print(asyncio.run(_apply_from_env()))
