"""
IPTC (International Press Telecommunications Council) keyword expansion.

This module loads IPTC Media Topic alias mappings and provides
functionality to expand TMDB keywords to all related aliases.

The alias map maps normalized terms to qcodes (e.g., "abduction" -> "medtop:20000100").
We build a reverse map to find all aliases for a given qcode, enabling
keyword expansion for better search coverage.
"""

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

from utils.get_logger import get_logger

logger = get_logger(__name__)

# Path to IPTC data files
IPTC_DATA_DIR = Path(__file__).parent.parent.parent / "data" / "itpc"
ALIAS_MAP_FILE = IPTC_DATA_DIR / "cptall-en-US-alias-map.json"


def normalize_tag(value: str) -> str:
    """
    Normalize a value for use as a Redis TAG.

    - Lowercase
    - Strip whitespace
    - Replace spaces and special characters with underscore
    - Remove leading/trailing underscores

    Examples:
        "Science Fiction" -> "science_fiction"
        "Tom Hanks" -> "tom_hanks"
        "US" -> "us"
        "R&B" -> "r_b"
    """
    if not value:
        return ""
    value = value.strip().lower()
    # Replace any non-alphanumeric characters with underscore
    value = re.sub(r"[^a-z0-9]+", "_", value)
    # Remove leading/trailing underscores
    return value.strip("_")


@lru_cache(maxsize=1)
def load_alias_map() -> dict[str, str]:
    """
    Load the IPTC alias map from disk (cached).

    Returns:
        Dict mapping normalized aliases to qcodes.
        e.g., {"abduction": "medtop:20000100", "abduct": "medtop:20000100"}
    """
    if not ALIAS_MAP_FILE.exists():
        logger.warning(f"IPTC alias map not found at {ALIAS_MAP_FILE}")
        return {}

    with open(ALIAS_MAP_FILE, encoding="utf-8") as f:
        data: dict[str, str] = json.load(f)
    logger.info(f"Loaded {len(data):,} IPTC aliases")
    return data


@lru_cache(maxsize=1)
def build_reverse_map() -> dict[str, list[str]]:
    """
    Build reverse map from qcode to list of all aliases.

    Returns:
        Dict mapping qcodes to all their aliases.
        e.g., {"medtop:20000100": ["abduct", "abduction", "kidnap", ...]}
    """
    alias_map = load_alias_map()
    reverse: dict[str, list[str]] = {}

    for alias, qcode in alias_map.items():
        if qcode not in reverse:
            reverse[qcode] = []
        reverse[qcode].append(alias)

    logger.info(f"Built reverse map with {len(reverse):,} qcodes")
    return reverse


class IPTCKeywordExpander:
    """
    Expands TMDB keywords using IPTC Media Topic aliases.

    Usage:
        expander = IPTCKeywordExpander()
        keywords = expander.expand([{"id": 123, "name": "time travel"}])
        # Returns normalized + expanded keyword list
    """

    def __init__(self) -> None:
        """Initialize the expander with IPTC data."""
        self._alias_map = load_alias_map()
        self._reverse_map = build_reverse_map()
        self._stats = {"lookups": 0, "hits": 0, "expansions": 0}

    def _normalize_for_lookup(self, keyword: str) -> str:
        """
        Normalize a keyword for IPTC lookup.

        IPTC aliases use spaces, not underscores, and are lowercase.
        """
        return keyword.strip().lower()

    def expand_single(self, keyword_name: str) -> list[str]:
        """
        Expand a single keyword to all IPTC aliases.

        Args:
            keyword_name: The keyword name (e.g., "time travel")

        Returns:
            List of normalized aliases including the original.
        """
        self._stats["lookups"] += 1
        normalized = normalize_tag(keyword_name)
        result = {normalized}  # Always include the normalized original

        # Try lookup with IPTC format (spaces, no underscores)
        lookup_key = self._normalize_for_lookup(keyword_name)
        qcode = self._alias_map.get(lookup_key)

        if qcode:
            self._stats["hits"] += 1
            aliases = self._reverse_map.get(qcode, [])
            for alias in aliases:
                normalized_alias = normalize_tag(alias)
                if normalized_alias:
                    result.add(normalized_alias)
            self._stats["expansions"] += len(aliases)

        return sorted(result)

    def expand(self, tmdb_keywords: list[dict[str, Any]]) -> list[str]:
        """
        Expand a list of TMDB keywords to all IPTC aliases.

        Args:
            tmdb_keywords: List of TMDB keyword dicts with "id" and "name" keys.
                          e.g., [{"id": 123, "name": "time travel"}]

        Returns:
            Sorted list of unique normalized keywords including all aliases.
        """
        if not tmdb_keywords:
            return []

        expanded: set[str] = set()

        for kw in tmdb_keywords:
            name = kw.get("name", "")
            if name:
                aliases = self.expand_single(name)
                expanded.update(aliases)

        return sorted(expanded)

    @property
    def stats(self) -> dict[str, int]:
        """Return expansion statistics."""
        return self._stats.copy()

    def reset_stats(self) -> None:
        """Reset expansion statistics."""
        self._stats = {"lookups": 0, "hits": 0, "expansions": 0}


# Module-level singleton for convenience
_expander: IPTCKeywordExpander | None = None


def get_keyword_expander() -> IPTCKeywordExpander:
    """Get the singleton keyword expander instance."""
    global _expander
    if _expander is None:
        _expander = IPTCKeywordExpander()
    return _expander


def expand_keywords(tmdb_keywords: list[dict[str, Any]]) -> list[str]:
    """
    Convenience function to expand TMDB keywords.

    Args:
        tmdb_keywords: List of TMDB keyword dicts.

    Returns:
        Sorted list of normalized + expanded keywords.
    """
    return get_keyword_expander().expand(tmdb_keywords)
