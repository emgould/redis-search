"""Local scan cache for microgenre gap-fill (candidate Redis keys + scan stats)."""

from __future__ import annotations

import json
import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from redis.asyncio import Redis

from etl.microgenre_gap_fill import (
    MGET_BATCH_SIZE,
    MissingStats,
    _doc_media_type,
    _has_populated_microgenres,
    _json_doc,
)
from utils.get_logger import get_logger

logger = get_logger(__name__)

SCAN_CACHE_VERSION = 1
DEFAULT_CACHE_DIR = Path(__file__).resolve().parents[2] / "data" / "microgenre-backfill"


def default_scan_cache_path(*, redis_label: str) -> Path:
    """Path for a REDIS=local|dev label (see Makefile)."""
    return DEFAULT_CACHE_DIR / f"scan-{redis_label}.json"


def _cache_request(
    *,
    redis_host: str,
    redis_port: int,
    mc_type: str,
    all_missing: bool,
    empty_scores: bool,
    force: bool,
    limit: int | None,
) -> dict[str, Any]:
    return {
        "redis_host": redis_host,
        "redis_port": redis_port,
        "mc_type": mc_type,
        "all_missing": all_missing,
        "empty_scores": empty_scores,
        "force": force,
        "limit": limit,
    }


def cache_request_matches(cached: dict[str, Any], current: dict[str, Any]) -> bool:
    for key in (
        "redis_host",
        "redis_port",
        "mc_type",
        "all_missing",
        "empty_scores",
        "force",
        "limit",
    ):
        if cached.get(key) != current.get(key):
            return False
    return True


def resolve_scan_source(
    cache_path: Path,
    *,
    use_scan_cache: bool,
    refresh_scan_cache: bool,
    no_scan_cache: bool,
) -> Literal["scan", "cache"]:
    """Decide whether to Redis SCAN or reload candidate keys from disk."""
    if no_scan_cache or refresh_scan_cache:
        return "scan"
    if use_scan_cache:
        if not cache_path.is_file():
            logger.warning("Scan cache not found at %s; running live SCAN", cache_path)
            return "scan"
        return "cache"
    if not cache_path.is_file():
        return "scan"
    if not sys.stdin.isatty():
        logger.info(
            "Scan cache exists at %s (non-interactive); re-scanning. "
            "Pass --use-scan-cache to reuse.",
            cache_path,
        )
        return "scan"

    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "scan"
    created = payload.get("created_at", "?")
    n_keys = len(payload.get("candidate_keys", []))
    print(
        f"\nMicrogenre scan cache: {cache_path}\n"
        f"  created: {created}\n"
        f"  candidates: {n_keys}\n",
        file=sys.stderr,
    )
    while True:
        answer = input("Use cached scan? [Y/n/refresh] ").strip().lower()
        if answer in {"", "y", "yes"}:
            return "cache"
        if answer in {"n", "no", "scan"}:
            return "scan"
        if answer in {"r", "refresh"}:
            return "scan"
        print("Enter Y, n, or refresh", file=sys.stderr)


def write_scan_cache(
    cache_path: Path,
    *,
    candidate_keys: list[str],
    stats: MissingStats,
    request: dict[str, Any],
) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": SCAN_CACHE_VERSION,
        "created_at": datetime.now(UTC).isoformat(),
        "request": request,
        "candidate_keys": candidate_keys,
        "stats": asdict(stats),
    }
    cache_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    logger.info("Wrote scan cache: %s (%d candidate keys)", cache_path, len(candidate_keys))


def read_scan_cache(cache_path: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Invalid scan cache %s: %s", cache_path, exc)
        return None
    if not isinstance(raw, dict) or raw.get("version") != SCAN_CACHE_VERSION:
        logger.warning("Unsupported scan cache format in %s", cache_path)
        return None
    return raw


async def load_candidates_from_cache_keys(
    redis: Redis,  # type: ignore[type-arg]
    candidate_keys: list[str],
    *,
    mc_type: str,
    force: bool,
    limit: int | None,
    cached_stats: MissingStats | None = None,
) -> tuple[list[tuple[str, dict[str, Any]]], MissingStats]:
    """MGET cached keys and rebuild candidate list (re-filter if Redis changed)."""
    from etl.microgenre_gap_fill import _selected_types

    stats = cached_stats if cached_stats is not None else MissingStats()
    selected = _selected_types(mc_type)
    candidates: list[tuple[str, dict[str, Any]]] = []
    stats.keys_seen = len(candidate_keys)

    keys = candidate_keys if limit is None else candidate_keys[:limit]
    for batch_start in range(0, len(keys), MGET_BATCH_SIZE):
        batch = keys[batch_start : batch_start + MGET_BATCH_SIZE]
        raw_docs = await redis.execute_command("JSON.MGET", *batch, "$")
        if not isinstance(raw_docs, list):
            continue
        for key, raw_doc in zip(batch, raw_docs, strict=False):
            doc = _json_doc(raw_doc)
            if doc is None:
                continue
            stats.scanned_docs += 1
            media_type = _doc_media_type(doc)
            if media_type is None or media_type not in selected:
                continue
            if not force and _has_populated_microgenres(doc.get("microgenres")):
                stats.already_has_microgenres += 1
                continue
            candidates.append((key, doc))
            if len(stats.sample_mc_ids) < 20:
                mc_id = doc.get("mc_id") or doc.get("id")
                if isinstance(mc_id, str):
                    stats.sample_mc_ids.append(mc_id)
            if limit is not None and len(candidates) >= limit:
                break
        if limit is not None and len(candidates) >= limit:
            break

    stats.candidates = len(candidates)
    return candidates, stats


def stats_from_cache_dict(data: dict[str, Any]) -> MissingStats:
    stats = MissingStats()
    for field_name, value in data.items():
        if not hasattr(stats, field_name):
            continue
        if field_name == "sample_mc_ids" and isinstance(value, list):
            stats.sample_mc_ids = [str(v) for v in value]
        elif field_name == "keys_seen" and isinstance(value, int):
            stats.keys_seen = value
        elif field_name == "scanned_docs" and isinstance(value, int):
            stats.scanned_docs = value
        elif field_name == "already_has_microgenres" and isinstance(value, int):
            stats.already_has_microgenres = value
        elif field_name == "skipped_ineligible" and isinstance(value, int):
            stats.skipped_ineligible = value
        elif field_name == "candidates" and isinstance(value, int):
            stats.candidates = value
    return stats
