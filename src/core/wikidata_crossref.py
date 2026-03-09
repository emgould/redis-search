"""Wikidata cross-reference identifier loader.

Loads ``data/wikidata_tmdb_tms_crossref.json`` and provides helpers to look up
and merge additional external identifiers (Rotten Tomatoes, Metacritic,
Letterboxd, JustWatch, TCM) into the ``external_ids`` dict carried by each
Redis media document.

The crossref file is keyed by ``"movie:{tmdb_id}"`` / ``"tv:{tmdb_id}"`` and
each entry may contain: ``imdb_id``, ``justwatch_id``, ``letterboxd_id``,
``metacritic_id``, ``rt_id``, ``tcm_id``, ``wikidata_id``.

Keys that TMDB already provides (``imdb_id``, ``wikidata_id``) are never
overwritten — the merge adds only *new* keys with non-null values.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from utils.get_logger import get_logger

logger = get_logger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_CROSSREF_PATH = _PROJECT_ROOT / "data" / "wikidata_tmdb_tms_crossref.json"

_crossref_cache: dict[str, dict[str, str]] | None = None


def load_crossref(path: Path | None = None) -> dict[str, dict[str, str]]:
    """Load the crossref JSON file and return the raw dict (keyed by ``movie:{id}``/``tv:{id}``).

    The file is loaded once and cached at module level.  Pass *path* to
    override the default location, or set the ``WIKIDATA_CROSSREF_PATH``
    environment variable.
    """
    global _crossref_cache  # noqa: PLW0603
    if _crossref_cache is not None:
        return _crossref_cache

    if path is None:
        env_path = os.environ.get("WIKIDATA_CROSSREF_PATH")
        path = Path(env_path) if env_path else _DEFAULT_CROSSREF_PATH

    if not path.exists():
        logger.warning("Wikidata crossref file not found at %s — crossref enrichment disabled", path)
        _crossref_cache = {}
        return _crossref_cache

    logger.info("Loading wikidata crossref from %s …", path)
    with path.open("r", encoding="utf-8") as fh:
        data: dict[str, dict[str, str]] = json.load(fh)
    logger.info("Loaded %s entries from wikidata crossref", f"{len(data):,}")

    _crossref_cache = data
    return _crossref_cache


def get_crossref_ids(mc_type: str, tmdb_id: int | str) -> dict[str, str] | None:
    """Look up crossref identifiers for a media item.

    Parameters
    ----------
    mc_type:
        ``"movie"`` or ``"tv"``.
    tmdb_id:
        The TMDB numeric ID.

    Returns
    -------
    A dict of non-null identifier fields, or ``None`` if no entry exists.
    """
    crossref = load_crossref()
    if not crossref:
        return None

    key = f"{mc_type}:{tmdb_id}"
    entry = crossref.get(key)
    if entry is None:
        return None

    return {k: v for k, v in entry.items() if v is not None}


def merge_crossref_ids(
    existing_external_ids: dict[str, Any] | None,
    crossref_ids: dict[str, str],
) -> dict[str, Any]:
    """Merge crossref identifiers into an existing ``external_ids`` dict.

    Only keys that are **absent** from *existing_external_ids* are added.
    This preserves TMDB-sourced values as authoritative.
    """
    base: dict[str, Any] = dict(existing_external_ids) if existing_external_ids else {}
    for k, v in crossref_ids.items():
        if k not in base:
            base[k] = v
    return base


def enrich_external_ids(
    mc_type: str,
    source_id: int | str,
    external_ids: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """One-call convenience: look up crossref and merge into *external_ids*.

    Returns the (possibly enriched) ``external_ids`` dict, or the original
    value unchanged when no crossref match is found.
    """
    crossref_ids = get_crossref_ids(mc_type, source_id)
    if crossref_ids is None:
        return external_ids
    return merge_crossref_ids(external_ids, crossref_ids)


def reset_cache() -> None:
    """Clear the module-level crossref cache (useful for testing)."""
    global _crossref_cache  # noqa: PLW0603
    _crossref_cache = None
