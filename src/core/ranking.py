"""
Search result ranking/scoring system.

Implements tiered scoring algorithms for ranking search results based on
match quality across title and TAG fields. Supports media, podcasts, people, and books.

Media Scoring Tiers (lower = better):
    0-5:   EXACT matches (title, director, cast, keywords, genres)
    6-10:  CONTAINS_WORD matches
    11-12: CONTAINS_SUBSTRING matches (including IPTC alias matches)
    13-14: PREFIX matches
    15:    Fallback

Podcast Scoring Tiers (lower = better):
    0-3:   EXACT matches (title, author, category)
    4-6:   CONTAINS_WORD matches
    7-8:   CONTAINS_SUBSTRING matches
    9-10:  PREFIX matches
    11:    Fallback

Within each tier, results are sorted by popularity (desc), then episode_count (desc).

Book Scoring Tiers (lower = better):
    0-2:   EXACT matches (title raw/normalized, title starts with)
    3-4:   EXACT matches (author, subject)
    5-7:   CONTAINS_WORD matches (title, author, subject)
    8-10:  CONTAINS_SUBSTRING/PREFIX matches
    11-12: Description matches (word, substring)
    13:    Fallback

Within each tier, results are sorted by popularity_score (desc).

Person Scoring Tiers (lower = better):
    0-1:   EXACT name matches
    2:     CONTAINS_WORD name
    3:     CONTAINS_SUBSTRING name
    4:     PREFIX name
    5:     Fallback

Within each tier, results are sorted by year (desc) and/or popularity (desc).

Performance optimizations:
- No regex - uses string methods only
- Pre-normalizes query once
- Early returns on first tier match
- Short-circuits array checks with any() generators
"""

from typing import Any

from core.iptc import get_search_aliases


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

    # Get IPTC expanded aliases for later checks
    query_aliases = get_search_aliases(query_norm)
    expanded_aliases = [a for a in query_aliases if a != query_norm and len(a) > 3]

    # Tier 4.1: Title contains IPTC alias (e.g., "AI" -> "artificial_intelligence"
    # matches title "A.I. Artificial Intelligence" which normalizes to contain "artificial_intelligence")
    # Title matches rank higher than keyword-only matches
    if expanded_aliases and any(alias in title_norm for alias in expanded_aliases):
        return (4, -year, -popularity)

    # Tier 4.2: Keyword starts with IPTC alias (e.g., "AI" -> "artificial_intelligence"
    # matches keyword "artificial_intelligence_a_i")
    # This ranks higher than simple word/substring matches because it indicates
    # the content is specifically ABOUT the topic
    if expanded_aliases and any(
        any(k.startswith(alias) for alias in expanded_aliases) for k in keywords
    ):
        return (5, -year, -popularity)

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

    # Tier 11: CONTAINS_SUBSTRING title (raw query or IPTC alias)
    # e.g., "AI" query with title "A.I. Artificial Intelligence" - check if any IPTC alias
    # like "artificial_intelligence" is in the normalized title
    if query_norm in title_norm:
        return (11, -year, -popularity)

    # Also check IPTC-expanded aliases in title (e.g., "AI" -> "artificial_intelligence")
    query_aliases = get_search_aliases(query_norm)
    if any(alias in title_norm for alias in query_aliases if alias != query_norm):
        return (11, -year, -popularity)

    # Tier 12: CONTAINS_SUBSTRING any TAG field (raw query or IPTC alias)
    if (
        (director and query_norm in director)
        or any(query_norm in c for c in cast_names)
        or any(query_norm in k for k in keywords)
        or any(query_norm in g for g in genres)
    ):
        return (12, -year, -popularity)

    # Also check IPTC-expanded aliases in keywords
    # e.g., "AI" -> ["artificial_intelligence"] matches keyword "artificial_intelligence_a_i"
    if any(any(alias in k for alias in query_aliases if alias != query_norm) for k in keywords):
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


def score_podcast_result(query: str, doc: dict[str, Any]) -> tuple[int, float, int]:
    """
    Score a podcast result against a search query.

    Tiers (lower = better):
        0: EXACT title (raw)
        1: EXACT title (normalized)
        2: TITLE STARTS WITH query (e.g., "AI in Action" for query "ai")
        3: EXACT author (normalized)
        4: EXACT category (any)
        5: CONTAINS_WORD title (word in title but not at start)
        6: CONTAINS_WORD author
        7: CONTAINS_WORD category
        8: CONTAINS_SUBSTRING title
        9: CONTAINS_SUBSTRING author/category
        10: PREFIX author/category
        11: Fallback

    Within each tier, sorted by -popularity (descending), then -episode_count (descending).

    Args:
        query: The search query string
        doc: The podcast document

    Returns:
        Tuple of (tier, -popularity, -episode_count) for use as sort key
    """
    query_lower = query.lower().strip()
    query_norm = normalize_for_match(query)

    # Extract fields ONCE
    title_raw = (doc.get("search_title") or doc.get("title") or "").lower().strip()
    title_norm = normalize_for_match(title_raw)
    author = doc.get("author_normalized") or normalize_for_match(doc.get("author") or "")
    categories: list[str] = doc.get("categories") or []

    popularity = float(doc.get("popularity") or 0)
    episode_count = int(doc.get("episode_count") or 0)

    # Tier 0: EXACT title (raw)
    if query_lower == title_raw:
        return (0, -popularity, -episode_count)

    # Tier 1: EXACT title (normalized)
    if query_norm == title_norm:
        return (1, -popularity, -episode_count)

    # Tier 2: TITLE STARTS WITH query
    # "AI in Action" ranks higher than "The Agile Brand... AI"
    title_tokens = title_norm.split("_")
    if title_tokens and title_tokens[0] == query_norm:
        return (2, -popularity, -episode_count)

    # Tier 3: EXACT author
    if author and query_norm == author:
        return (3, -popularity, -episode_count)

    # Tier 4: EXACT category (any)
    if any(query_norm == c for c in categories):
        return (4, -popularity, -episode_count)

    # Tier 5: CONTAINS_WORD title (word in title but not first word)
    if query_norm in title_tokens:
        return (5, -popularity, -episode_count)

    # Tier 6: CONTAINS_WORD author
    if author and query_norm in author.split("_"):
        return (6, -popularity, -episode_count)

    # Tier 7: CONTAINS_WORD category
    if any(query_norm in c.split("_") for c in categories):
        return (7, -popularity, -episode_count)

    # Tier 8: CONTAINS_SUBSTRING title
    if query_norm in title_norm:
        return (8, -popularity, -episode_count)

    # Tier 9: CONTAINS_SUBSTRING author/category
    if (author and query_norm in author) or any(query_norm in c for c in categories):
        return (9, -popularity, -episode_count)

    # Tier 10: PREFIX author/category
    if (author and author.startswith(query_norm)) or any(
        c.startswith(query_norm) for c in categories
    ):
        return (10, -popularity, -episode_count)

    # Tier 11: Fallback
    return (11, -popularity, -episode_count)


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


def _extract_work_id_num(doc: dict[str, Any]) -> int:
    """
    Extract numeric work ID from OpenLibrary key for tie-breaking.

    Lower IDs = older/more established works (e.g., OL76837W < OL15290485W).
    Returns a large number if no valid ID found.
    """
    key = doc.get("openlibrary_key") or doc.get("key") or doc.get("id") or ""
    # Handle formats: "/works/OL76837W", "OL76837W", "book_OL76837W"
    if "OL" in key and "W" in key:
        try:
            # Extract just the numeric part between OL and W
            start = key.index("OL") + 2
            end = key.index("W", start)
            return int(key[start:end])
        except (ValueError, IndexError):
            pass
    return 999999999  # Large number for unknown IDs


def score_book_result(query: str, doc: dict[str, Any]) -> tuple[int, float, int]:
    """
    Score a book result against a search query.

    Tiers (lower = better):
        0: EXACT title (raw)
        1: EXACT title (normalized)
        2: TITLE STARTS WITH query
        3: EXACT author (normalized)
        4: EXACT subject (any)
        5: CONTAINS_WORD title
        6: CONTAINS_WORD author
        7: CONTAINS_WORD subject
        8: CONTAINS_SUBSTRING title
        9: CONTAINS_SUBSTRING author/subject
        10: PREFIX author/subject
        11: CONTAINS_WORD description
        12: CONTAINS_SUBSTRING description
        13: Fallback

    Within each tier, sorted by:
    1. -popularity_score (descending)
    2. work_id (ascending) - lower IDs = older/more established works

    Args:
        query: The search query string
        doc: The book document

    Returns:
        Tuple of (tier, -popularity_score, work_id) for use as sort key
    """
    query_lower = query.lower().strip()
    query_norm = normalize_for_match(query)

    # Extract fields ONCE
    title_raw = (doc.get("search_title") or doc.get("title") or "").lower().strip()
    title_norm = normalize_for_match(title_raw)
    author = doc.get("author_normalized") or normalize_for_match(doc.get("author") or "")
    subjects: list[str] = doc.get("subjects_normalized") or []
    description = (doc.get("description") or "").lower()

    popularity_score = float(doc.get("popularity_score") or 0)
    work_id = _extract_work_id_num(doc)

    # Tier 0: EXACT title (raw)
    if query_lower == title_raw:
        return (0, -popularity_score, work_id)

    # Tier 1: EXACT title (normalized)
    if query_norm == title_norm:
        return (1, -popularity_score, work_id)

    # Tier 2: TITLE STARTS WITH query
    title_tokens = title_norm.split("_")
    if title_tokens and title_tokens[0] == query_norm:
        return (2, -popularity_score, work_id)

    # Tier 3: EXACT author
    if author and query_norm == author:
        return (3, -popularity_score, work_id)

    # Tier 4: EXACT subject (any)
    if any(query_norm == s for s in subjects):
        return (4, -popularity_score, work_id)

    # Tier 5: CONTAINS_WORD title (word in title but not first word)
    if query_norm in title_tokens:
        return (5, -popularity_score, work_id)

    # Tier 6: CONTAINS_WORD author
    if author and query_norm in author.split("_"):
        return (6, -popularity_score, work_id)

    # Tier 7: CONTAINS_WORD subject
    if any(query_norm in s.split("_") for s in subjects):
        return (7, -popularity_score, work_id)

    # Tier 8: CONTAINS_SUBSTRING title
    if query_norm in title_norm:
        return (8, -popularity_score, work_id)

    # Tier 9: CONTAINS_SUBSTRING author/subject
    if (author and query_norm in author) or any(query_norm in s for s in subjects):
        return (9, -popularity_score, work_id)

    # Tier 10: PREFIX author/subject
    if (author and author.startswith(query_norm)) or any(
        s.startswith(query_norm) for s in subjects
    ):
        return (10, -popularity_score, work_id)

    # Tier 11: CONTAINS_WORD description (whole word match)
    # Split description into words for exact word matching
    desc_words = set(normalize_for_match(description).split("_"))
    if query_norm in desc_words:
        return (11, -popularity_score, work_id)

    # Tier 12: CONTAINS_SUBSTRING description
    if query_norm in description:
        return (12, -popularity_score, work_id)

    # Tier 13: Fallback
    return (13, -popularity_score, work_id)


# Cross-source priority for exact_match (lower index = higher priority)
EXACT_MATCH_SOURCE_PRIORITY: tuple[str, ...] = (
    "movie",
    "tv",
    "person",
    "podcast",
    "book",
    "author",
)


def is_exact_match(query: str, doc: dict[str, Any], source: str) -> bool:
    """
    Return True if doc is an exact match for the query (tier 0-1 title/name match).

    Used to surface the single best match when the user's query exactly matches
    a known entity (e.g., "the godfather" -> movie "The Godfather").

    Definition of exact:
    - Media (tv, movie): tier 0-1 (exact title raw/normalized)
    - Person: tier 0-1 (exact name)
    - Book: tier 0-1 (exact title)
    - Podcast: tier 0-3 (exact title, title starts with, exact author)
    - Author: exact normalized name match

    Args:
        query: Search query string
        doc: Parsed result document (MCBaseItem-like dict)
        source: Source name (movie, tv, person, podcast, book, author)

    Returns:
        True if doc is an exact match for query
    """
    if not query or not query.strip():
        return False

    q = query.strip()
    if source in ("tv", "movie"):
        tier = score_media_result(q, doc)[0]
        return tier <= 1
    if source == "person":
        tier = score_person_result(q, doc)[0]
        return tier <= 1
    if source == "book":
        tier = score_book_result(q, doc)[0]
        return tier <= 1
    if source == "podcast":
        tier = score_podcast_result(q, doc)[0]
        return tier <= 3
    if source == "author":
        name = (doc.get("search_title") or doc.get("name") or "").strip()
        if not name:
            return False
        query_norm = normalize_for_match(q)
        name_norm = normalize_for_match(name.lower())
        return query_norm == name_norm

    return False
