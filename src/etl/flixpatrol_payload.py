"""Apply FlixPatrol history onto Redis media documents.

Title files come from ``api.subapi.flixpatrol.history.transform_flixpatrol_rankings``.
Set ``FLIXPATROL_TITLES_DIR`` to override the default ``data/flixpatrol/us-titles``
directory under the repo root.

Full chart history is stored at ``flixpatrol:{mc_id}``. The ``media:{mc_id}`` document
carries an abridged ``flixpatrol`` block (summary fields plus first and last record).
"""

import asyncio
import copy
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


def flixpatrol_sidecar_key(mc_id: str) -> str:
    """Redis key for the full FlixPatrol history object."""
    return f"flixpatrol:{mc_id}"


def abridge_flixpatrol_records(data: dict[str, object]) -> dict[str, object]:
    """Return a copy of ``data`` with ``records`` reduced to first and last elements."""
    out = copy.deepcopy(data)
    records_raw = out.get("records")
    if not isinstance(records_raw, list):
        out["records"] = []
        return out
    records: list[object] = records_raw
    if len(records) <= 1:
        out["records"] = list(records)
        return out
    out["records"] = [records[0], records[-1]]
    return out


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
) -> dict[str, object] | None:
    """Set abridged ``flixpatrol`` on ``redis_doc``.

    When a title file supplies history, returns the full object for the sidecar key.
    When copying from an existing document, returns ``None`` and does not rewrite the sidecar.
    """
    mc_id = redis_doc.get("mc_id")
    if titles_dir is not None and isinstance(mc_id, str) and mc_id:
        from_file = flixpatrol_data_from_title_file(titles_dir / f"{mc_id}.json")
        if from_file is not None:
            redis_doc["flixpatrol"] = abridge_flixpatrol_records(from_file)
            return copy.deepcopy(from_file)
    if existing_doc is None:
        return None
    current = existing_doc.get("flixpatrol")
    if isinstance(current, dict):
        redis_doc["flixpatrol"] = current
    return None


async def apply_flixpatrol_title_files(redis: Redis, titles_dir: Path) -> int:
    """Set abridged ``$.flixpatrol`` on media docs and full history on sidecar keys."""
    pending: list[tuple[str, str, dict[str, object]]] = []
    for path in sorted(titles_dir.glob("*.json")):
        payload = _read_object(path)
        if payload is None:
            continue
        data = payload.get("data")
        mc_id = payload.get("mc_id")
        if not isinstance(data, dict) or not isinstance(mc_id, str) or not mc_id:
            continue
        media_key = f"media:{mc_id}"
        if not await redis.exists(media_key):
            continue
        pending.append((media_key, flixpatrol_sidecar_key(mc_id), cast(dict[str, object], data)))
    updated = 0
    for offset in range(0, len(pending), 100):
        chunk = pending[offset : offset + 100]
        pipe = redis.pipeline()
        for media_key, sidecar_key, full_data in chunk:
            abridged = abridge_flixpatrol_records(full_data)
            pipe.json().set(media_key, "$.flixpatrol", cast(JsonType, abridged))
            pipe.json().set(sidecar_key, "$", cast(JsonType, full_data))
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
