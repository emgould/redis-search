from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Any

from core.streaming_providers import MAJOR_STREAMING_PROVIDERS

DOCUMENTARY_GENRE_IDS = {"99"}
DOCUMENTARY_GENRE_NAMES = {"documentary"}
_NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9]+")


def _normalize_provider_name(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower().replace("+", " plus ")
    normalized = _NON_ALNUM_PATTERN.sub(" ", normalized).strip()
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


MAJOR_STREAMING_PROVIDERS_NORMALIZED = {
    normalized_name
    for name in MAJOR_STREAMING_PROVIDERS
    if (normalized_name := _normalize_provider_name(name))
}


def _normalize_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    return value.strip().lower()


def _iter_provider_payloads(item: Mapping[str, Any]) -> list[Any]:
    payloads: list[Any] = []
    watch_providers = item.get("watch_providers")
    if isinstance(watch_providers, Mapping):
        payloads.append(watch_providers)
    watch_provider_results = item.get("watch/providers")
    if isinstance(watch_provider_results, Mapping):
        for country_payload in watch_provider_results.get("results", {}).values() if isinstance(
            watch_provider_results.get("results"), Mapping
        ) else []:
            if isinstance(country_payload, Mapping):
                payloads.append(country_payload)
    return payloads


def _to_provider_names(item: Mapping[str, Any]) -> set[str]:
    provider_names: set[str] = set()
    for provider_payload in _iter_provider_payloads(item):
        flatrate = provider_payload.get("flatrate", [])
        if isinstance(flatrate, Sequence) and not isinstance(flatrate, (str, bytes, bytearray)):
            for provider in flatrate:
                provider_name = provider.get("provider_name") if isinstance(provider, Mapping) else None
                if isinstance(provider_name, str) and provider_name:
                    normalized_name = _normalize_provider_name(provider_name)
                    if normalized_name:
                        provider_names.add(normalized_name)
    return provider_names


def is_documentary(item: Mapping[str, Any]) -> bool:
    genre_ids = item.get("genre_ids")
    if isinstance(genre_ids, Sequence) and not isinstance(genre_ids, (str, bytes, bytearray)):
        for genre_id in genre_ids:
            if isinstance(genre_id, str) and genre_id in DOCUMENTARY_GENRE_IDS:
                return True
            if isinstance(genre_id, int) and str(genre_id) in DOCUMENTARY_GENRE_IDS:
                return True

    genres = item.get("genres")
    if isinstance(genres, Sequence) and not isinstance(genres, (str, bytes, bytearray)):
        for genre in genres:
            if isinstance(genre, str) and _normalize_text(genre) in DOCUMENTARY_GENRE_NAMES:
                return True
            if isinstance(genre, Mapping):
                name = genre.get("name")
                if _normalize_text(name) in DOCUMENTARY_GENRE_NAMES:
                    return True
    return False


def _has_poster(item: Mapping[str, Any]) -> bool:
    poster_path = item.get("poster_path")
    return isinstance(poster_path, str) and bool(poster_path.strip())


def _has_streaming_platform_signal(item: Mapping[str, Any], require_major_provider: bool) -> bool:
    streaming_platform = item.get("streaming_platform")
    if isinstance(streaming_platform, str) and streaming_platform.strip():
        if not require_major_provider:
            return True
        normalized_platform = _normalize_provider_name(streaming_platform)
        return bool(normalized_platform and normalized_platform in MAJOR_STREAMING_PROVIDERS_NORMALIZED)

    provider_names = _to_provider_names(item)
    if not provider_names:
        return False

    if require_major_provider:
        return bool(provider_names & MAJOR_STREAMING_PROVIDERS_NORMALIZED)
    return True


def _release_date(item: Mapping[str, Any]) -> date | None:
    release_raw = item.get("release_date")
    if not isinstance(release_raw, str) and "first_air_date" in item:
        release_raw = item.get("first_air_date")
    if not isinstance(release_raw, str):
        return None
    if len(release_raw) < 10:
        return None
    try:
        return datetime.strptime(release_raw[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _within_last_years(target_date: date, as_of: date, years_back: int) -> bool:
    try:
        cutoff = as_of.replace(year=as_of.year - years_back)
    except ValueError:
        # Handle leap-year edge case (e.g., 2024-02-29 -> 2014-02-28)
        cutoff = as_of.replace(month=2, day=28, year=as_of.year - years_back)
    return cutoff <= target_date <= as_of


def is_eligible_documentary(
    item: Mapping[str, Any], years_back: int = 10, as_of: date | None = None, require_major_provider: bool = False
) -> bool:
    if not is_documentary(item):
        return False
    if not _has_poster(item):
        return False
    if not _has_streaming_platform_signal(item, require_major_provider=require_major_provider):
        return False
    reference_date = as_of or date.today()
    release_date = _release_date(item)
    if release_date is None:
        return False
    return _within_last_years(release_date, reference_date, years_back=years_back)
