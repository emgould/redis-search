"""Intake filter for documents propagated to Media Manager FAISS index.

Operates on Redis media documents (output of ``document_to_redis``).
Used by both the backfill script and the nightly ETL push path.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.streaming_providers import MAJOR_STREAMING_PROVIDERS

VALID_PROVIDER_TYPES = frozenset({"flatrate", "buy", "rent", "in theater", "on_demand"})
CANCELED_STATUS = "Canceled"
MIN_VOTE_COUNT = 5
MIN_VOTE_AVERAGE = 4.0


def _get_primary_provider_name(watch_providers: Mapping[str, Any]) -> str | None:
    primary = watch_providers.get("primary_provider")
    if isinstance(primary, dict):
        name = primary.get("provider_name")
        if isinstance(name, str) and name.strip():
            return name.strip()
    return None


def _get_primary_provider_type(watch_providers: Mapping[str, Any]) -> str | None:
    ptype = watch_providers.get("primary_provider_type")
    if isinstance(ptype, str) and ptype.strip():
        return ptype.strip().lower()
    return None


def _passes_provider_filter(doc: Mapping[str, Any]) -> bool:
    """Check provider availability criteria.

    Pass if:
      - primary_provider.provider_name in MAJOR_STREAMING_PROVIDERS
        AND primary_provider_type in (flatrate, buy, rent, in theater, on_demand)
    OR:
      - primary_provider is null AND primary_provider_type == 'in theater'
    """
    wp = doc.get("watch_providers")
    if not isinstance(wp, Mapping):
        return False

    provider_name = _get_primary_provider_name(wp)
    provider_type = _get_primary_provider_type(wp)

    if provider_name and provider_name in MAJOR_STREAMING_PROVIDERS:
        return provider_type is not None and provider_type in VALID_PROVIDER_TYPES

    return provider_name is None and provider_type == "in theater"


def passes_media_manager_filter(doc: Mapping[str, Any]) -> tuple[bool, str]:
    """Determine if a Redis media document should be sent to Media Manager.

    Returns (passed, reason) where reason describes the rejection cause
    (empty string when passed is True).
    """
    title = doc.get("title") or doc.get("name")
    if not title:
        return False, "no title or name"

    overview = doc.get("overview")
    if not overview or (isinstance(overview, str) and not overview.strip()):
        return False, "empty overview"

    image = doc.get("image")
    if not image or (isinstance(image, str) and not image.strip()):
        return False, "no poster image"

    vote_count = doc.get("vote_count")
    if not isinstance(vote_count, (int, float)) or vote_count < MIN_VOTE_COUNT:
        return False, f"vote_count {vote_count} < {MIN_VOTE_COUNT}"

    vote_average = doc.get("vote_average")
    if not isinstance(vote_average, (int, float)) or vote_average < MIN_VOTE_AVERAGE:
        return False, f"vote_average {vote_average} < {MIN_VOTE_AVERAGE}"

    status = doc.get("status")
    if isinstance(status, str) and status == CANCELED_STATUS:
        return False, "status is Canceled"

    if not _passes_provider_filter(doc):
        return False, "does not meet provider criteria"

    return True, ""
