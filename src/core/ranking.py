"""
Media result ranking/scoring system.

Implements a tiered scoring algorithm for ranking search results based on
match quality across title and TAG fields.

Scoring Tiers (lower = better):
    0:  EXACT title (raw)
    1:  EXACT title (normalized)
    2:  EXACT director_name
    3:  EXACT cast_names (any)
    4:  EXACT keywords (any)
    5:  EXACT genres (any)
    6:  CONTAINS_WORD title
    7:  CONTAINS_WORD director_name
    8:  CONTAINS_WORD cast_names
    9:  CONTAINS_WORD keywords
    10: CONTAINS_WORD genres
    11: CONTAINS_SUBSTRING title
    12: CONTAINS_SUBSTRING any TAG
    13: PREFIX title
    14: PREFIX any TAG
    15: OTHER (fallback)

Within each tier, results are sorted by year (desc), then popularity (desc).

Performance optimizations:
- No regex - uses string methods only
- Pre-normalizes query once
- Early returns on first tier match
- Short-circuits array checks with any() generators
"""

from typing import Any


def normalize_for_match(value: str) -> str:
    """
    Normalize a string for matching.

    - Lowercase
    - Replace non-alphanumeric with underscore
    - Collapse multiple underscores
    - Strip leading/trailing underscores

    Uses only string methods - no regex for performance.
    """
    if not value:
        return ""

    result: list[str] = []
    for c in value.lower():
        if c.isalnum():
            result.append(c)
        elif result and result[-1] != "_":
            result.append("_")

    # Strip leading/trailing underscores
    s = "".join(result)
    return s.strip("_")


def score_media_result(query: str, doc: dict[str, Any]) -> tuple[int, int, float]:
    """
    Score a media result against a search query.

    Returns a tuple for sorting: (tier, -year, -popularity)
    Lower tier = better match. Negative year/popularity for descending sort.

    Args:
        query: The search query string
        doc: The media document with fields like search_title, director_name, etc.

    Returns:
        Tuple of (tier, -year, -popularity) for use as sort key
    """
    # Pre-normalize query ONCE
    query_lower = query.lower().strip()
    query_norm = normalize_for_match(query)

    # Extract fields ONCE
    title_raw = (doc.get("search_title") or doc.get("title") or "").lower().strip()
    title_norm = normalize_for_match(title_raw)
    director = doc.get("director_name") or ""
    cast_names: list[str] = doc.get("cast_names") or []
    keywords: list[str] = doc.get("keywords") or []
    genres: list[str] = doc.get("genres") or []

    year = int(doc.get("year") or 0)
    popularity = float(doc.get("popularity") or 0)

    # Tier 0: EXACT title (raw) - case-insensitive but not normalized
    if query_lower == title_raw:
        return (0, -year, -popularity)

    # Tier 1: EXACT title (normalized)
    if query_norm == title_norm:
        return (1, -year, -popularity)

    # Tier 2: EXACT director_name
    if query_norm == director:
        return (2, -year, -popularity)

    # Tier 3: EXACT cast_names (any match)
    if any(query_norm == c for c in cast_names):
        return (3, -year, -popularity)

    # Tier 4: EXACT keywords (any match)
    if any(query_norm == k for k in keywords):
        return (4, -year, -popularity)

    # Tier 5: EXACT genres (any match)
    if any(query_norm == g for g in genres):
        return (5, -year, -popularity)

    # Tier 6: CONTAINS_WORD title (query is complete token in title)
    title_tokens = title_norm.split("_")
    if query_norm in title_tokens:
        return (6, -year, -popularity)

    # Tier 7: CONTAINS_WORD director_name
    if director and query_norm in director.split("_"):
        return (7, -year, -popularity)

    # Tier 8: CONTAINS_WORD cast_names
    if any(query_norm in c.split("_") for c in cast_names):
        return (8, -year, -popularity)

    # Tier 9: CONTAINS_WORD keywords
    if any(query_norm in k.split("_") for k in keywords):
        return (9, -year, -popularity)

    # Tier 10: CONTAINS_WORD genres
    if any(query_norm in g.split("_") for g in genres):
        return (10, -year, -popularity)

    # Tier 11: CONTAINS_SUBSTRING title
    if query_norm in title_norm:
        return (11, -year, -popularity)

    # Tier 12: CONTAINS_SUBSTRING any TAG field
    if (
        (director and query_norm in director)
        or any(query_norm in c for c in cast_names)
        or any(query_norm in k for k in keywords)
        or any(query_norm in g for g in genres)
    ):
        return (12, -year, -popularity)

    # Tier 13: PREFIX title
    if title_norm.startswith(query_norm):
        return (13, -year, -popularity)

    # Tier 14: PREFIX any TAG field
    if (
        (director and director.startswith(query_norm))
        or any(c.startswith(query_norm) for c in cast_names)
        or any(k.startswith(query_norm) for k in keywords)
        or any(g.startswith(query_norm) for g in genres)
    ):
        return (14, -year, -popularity)

    # Tier 15: Fallback (no match found)
    return (15, -year, -popularity)


def score_person_result(query: str, doc: dict[str, Any]) -> tuple[int, int, float]:
    """
    Score a person result against a search query.

    Simpler scoring for people - mainly based on name matching.

    Tiers:
        0: EXACT name
        1: EXACT normalized name
        2: CONTAINS_WORD name
        3: CONTAINS_SUBSTRING name
        4: PREFIX name
        5: Fallback

    Returns:
        Tuple of (tier, name_length, -popularity) for sorting
    """
    query_lower = query.lower().strip()
    query_norm = normalize_for_match(query)

    name = (doc.get("search_title") or doc.get("name") or "").lower().strip()
    name_norm = normalize_for_match(name)
    popularity = float(doc.get("popularity") or 0)

    # Tier 0: EXACT name
    if query_lower == name:
        return (0, len(name), -popularity)

    # Tier 1: EXACT normalized name
    if query_norm == name_norm:
        return (1, len(name), -popularity)

    # Tier 2: CONTAINS_WORD name
    if query_norm in name_norm.split("_"):
        return (2, len(name), -popularity)

    # Tier 3: CONTAINS_SUBSTRING name
    if query_norm in name_norm:
        return (3, len(name), -popularity)

    # Tier 4: PREFIX name
    if name_norm.startswith(query_norm):
        return (4, len(name), -popularity)

    # Tier 5: Fallback
    return (5, len(name), -popularity)
