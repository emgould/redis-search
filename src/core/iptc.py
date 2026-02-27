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
import unicodedata
from functools import lru_cache
from pathlib import Path
from typing import Any

from utils.get_logger import get_logger

logger = get_logger(__name__)

# Path to IPTC data files
IPTC_DATA_DIR = Path(__file__).parent.parent.parent / "data" / "itpc"
ALIAS_MAP_FILE = IPTC_DATA_DIR / "cptall-en-US-alias-map.json"

# Path to query expansion file (abbreviations, nicknames, etc.)
EXPANSIONS_FILE = Path(__file__).parent.parent.parent / "data" / "aliases" / "expansions.jsonc"

# Custom podcast-specific aliases that supplement IPTC
# Maps normalized query terms to additional aliases not in IPTC vocabulary
CUSTOM_PODCAST_ALIASES: dict[str, list[str]] = {
    # "True Crime" is a podcast category, but IPTC only has generic "crime"
    "crime": ["crime", "true_crime"],
    "true_crime": ["crime", "true_crime"],
}


@lru_cache(maxsize=1)
def load_query_expansions() -> dict[str, str]:
    """
    Load query expansion map (abbreviations, nicknames, etc.).

    Handles JSONC format (JSON with comments).

    Returns:
        Dict mapping normalized abbreviations to their expansions.
        e.g., {"ny": "New York", "la": "Louisiana", "bob": "Robert"}
    """
    if not EXPANSIONS_FILE.exists():
        logger.warning(f"Query expansions file not found at {EXPANSIONS_FILE}")
        return {}

    with open(EXPANSIONS_FILE, encoding="utf-8") as f:
        content = f.read()

    # Remove JSONC comments (/* ... */ and // ...)
    # Remove block comments
    content = re.sub(r"/\*.*?\*/", "", content, flags=re.DOTALL)
    # Remove line comments
    content = re.sub(r"//.*$", "", content, flags=re.MULTILINE)

    data: dict[str, str] = json.loads(content)

    # Normalize keys to lowercase
    normalized: dict[str, str] = {k.lower().strip(): v for k, v in data.items()}

    logger.info(f"Loaded {len(normalized):,} query expansions")
    return normalized


def expand_query_tokens(tokens: list[str]) -> list[list[str]]:
    """
    Expand query tokens using the expansions map.

    For each token, returns a list of alternatives (original + expanded).
    This allows building OR queries for each token position.

    Args:
        tokens: List of normalized query tokens (lowercase)

    Returns:
        List of lists, where each inner list contains alternatives for that position.
        e.g., ["ny", "jets"] -> [["ny", "new", "york"], ["jets"]]

    Example:
        Input: ["ny", "jets"]
        Output: [["ny", "new", "york"], ["jets"]]

        This enables building: (ny|new york) jets
    """
    expansions = load_query_expansions()
    result: list[list[str]] = []

    for token in tokens:
        token_lower = token.lower()
        alternatives = [token]  # Always include original

        # Check for expansion
        expansion = expansions.get(token_lower)
        if expansion:
            # Split expansion into tokens and add them
            expansion_tokens = expansion.lower().split()
            if expansion_tokens != [token_lower]:
                alternatives.extend(expansion_tokens)

        result.append(alternatives)

    return result


def expand_query_string(query: str) -> list[str]:
    """
    Expand a query string into multiple search variations.

    Takes a query like "NY Jets" and returns variations:
    - "NY Jets" (original)
    - "New York Jets" (with NY expanded)

    Args:
        query: The original search query

    Returns:
        List of query variations to search (original + expanded versions)
    """
    tokens = query.lower().split()
    if not tokens:
        return [query]

    expansions = load_query_expansions()
    variations: list[str] = [query]  # Always include original

    # Check each token for expansions
    expanded_tokens = list(tokens)  # Copy
    has_expansion = False

    for i, token in enumerate(tokens):
        expansion = expansions.get(token)
        if expansion:
            # Replace this token with its expansion
            expanded_tokens[i] = expansion.lower()
            has_expansion = True

    if has_expansion:
        # Build the expanded query
        expanded_query = " ".join(expanded_tokens)
        if expanded_query.lower() != query.lower():
            variations.append(expanded_query)

    return variations


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
    value = unicodedata.normalize("NFKD", value)
    value = value.encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^a-z0-9]+", "_", value)
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


@lru_cache(maxsize=1)
def build_normalized_alias_map() -> dict[str, list[str]]:
    """
    Build a map from normalized word to all normalized aliases for the same concept.

    This is used at SEARCH TIME to expand a query to all related aliases.
    e.g., "ai" -> ["ai", "artificial_intelligence", "machine_intelligence", ...]

    Returns:
        Dict mapping normalized alias to list of all normalized aliases for same qcode.
    """
    alias_map = load_alias_map()
    reverse_map = build_reverse_map()

    # Build: normalized_alias -> [all normalized aliases for same qcode]
    normalized_map: dict[str, list[str]] = {}

    for alias, qcode in alias_map.items():
        # Normalize this alias
        normalized = normalize_tag(alias)
        if not normalized:
            continue

        # Get all aliases for this qcode and normalize them
        all_aliases = reverse_map.get(qcode, [])
        normalized_aliases = sorted({normalize_tag(a) for a in all_aliases if normalize_tag(a)})

        # Store the mapping
        normalized_map[normalized] = normalized_aliases

    logger.info(f"Built normalized alias map with {len(normalized_map):,} entries")
    return normalized_map


def get_search_aliases(normalized_query: str) -> list[str]:
    """
    Get all aliases for a normalized query term.

    Combines:
    1. Custom podcast-specific aliases (e.g., crime -> true_crime)
    2. IPTC Media Topic aliases

    Used at search time to expand a query to all related keywords.

    Args:
        normalized_query: The normalized search term (e.g., "ai")

    Returns:
        List of all normalized aliases for the same concept,
        or [normalized_query] if no aliases found.
    """
    result: set[str] = set()

    # Check custom podcast aliases first
    custom = CUSTOM_PODCAST_ALIASES.get(normalized_query)
    if custom:
        result.update(custom)

    # Check IPTC aliases
    alias_map = build_normalized_alias_map()
    iptc_aliases = alias_map.get(normalized_query)
    if iptc_aliases:
        result.update(iptc_aliases)

    # If we found aliases, return them sorted
    if result:
        return sorted(result)

    # No expansion found, return the original
    return [normalized_query]


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
