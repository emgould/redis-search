"""Shared streaming provider constants for ETL filtering."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_PROVIDER_MAP_PATH = (
    Path(__file__).resolve().parent.parent / "api" / "tmdb" / "data" / "provider_map.json"
)

MAJOR_PROVIDER_MKT_SHARE_CUTOFF = 12


def _load_major_provider_ids(cutoff: int = MAJOR_PROVIDER_MKT_SHARE_CUTOFF) -> frozenset[int]:
    """Provider IDs (including packages) with mkt_share_order <= cutoff."""
    with _PROVIDER_MAP_PATH.open() as f:
        provider_map: list[dict[str, Any]] = json.load(f)
    ids: set[int] = set()
    for entry in provider_map:
        if entry.get("mkt_share_order", 999) <= cutoff:
            ids.add(entry["provider_id"])
            for pkg in entry.get("packages", []):
                ids.add(pkg["id"])
    return frozenset(ids)


MAJOR_PROVIDER_IDS: frozenset[int] = _load_major_provider_ids()

MAJOR_STREAMING_PROVIDERS = {
    "Amazon Prime",
    "Netflix",
    "Hulu",
    "HBO Max",
    "Max",
    "Disney Plus",
    "Peacock",
    "Apple TV",
    "Paramount Plus",
    "fuboTV",
}

TV_SHOW_CUTOFF_DATE = "2023-01-01"
