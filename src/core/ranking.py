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

import math
from datetime import UTC, datetime
from typing import Any

from core.iptc import get_search_aliases

_LEADING_ARTICLES: tuple[str, ...] = ("the_", "a_", "an_")
_THEATRICAL_WINDOW_DAYS = 183
_RECENCY_HALF_LIFE_DAYS = 540.0
_BAYESIAN_MIN_VOTES = 500.0
_BAYESIAN_PRIOR_RATING = 6.5


def strip_leading_article(value_norm: str) -> str:
    """Drop one leading the/a/an word from a normalize_for_match value."""
    for article in _LEADING_ARTICLES:
        if value_norm.startswith(article) and len(value_norm) > len(article):
            return value_norm[len(article) :]
    return value_norm


def _parse_effective_date(doc: dict[str, Any]) -> datetime | None:
    """Parse release_date (movies) or last_air_date/first_air_date (TV) to UTC datetime."""
    mc_type = doc.get("mc_type")
    if mc_type == "tv":
        raw = doc.get("last_air_date") or doc.get("last_aired_date") or doc.get("first_air_date")
    else:
        raw = doc.get("release_date") or doc.get("last_air_date") or doc.get("first_air_date")
    if not raw or not isinstance(raw, str):
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    except (ValueError, TypeError):
        return None


def compute_media_prominence(doc: dict[str, Any]) -> float:
    """
    Compute a composite prominence score for ranking media within the same match tier.

    Combines:
    - Base popularity (0.45 weight, 0-100)
    - Bayesian-shrunk rating + optional RT blend (0.25 weight, 0-100)
    - Uncapped log10 vote footprint up to 10^6 votes (0.15 weight, 0-100)
    - Exponential recency decay with 540-day half-life (0.15 weight, 0-100)
    - Language / US-market affinity modifier (-10 to +10)
    - US watchability / theatrical viability modifier (0 to +12)
    """
    popularity = max(0.0, min(100.0, float(doc.get("popularity") or 0.0)))

    try:
        vote_count = max(0, int(doc.get("vote_count") or 0))
    except (ValueError, TypeError):
        vote_count = 0

    try:
        raw_rating = float(doc.get("rating") or doc.get("vote_average") or 0.0)
    except (ValueError, TypeError):
        raw_rating = 0.0

    # Bayesian-weighted quality (0-100)
    if raw_rating > 0.0:
        r_bayes = (
            vote_count * raw_rating + _BAYESIAN_MIN_VOTES * _BAYESIAN_PRIOR_RATING
        ) / (vote_count + _BAYESIAN_MIN_VOTES)
        q_bayes = max(0.0, min(100.0, r_bayes * 10.0))
    else:
        q_bayes = _BAYESIAN_PRIOR_RATING * 10.0

    rt_scores: list[float] = []
    for rt_key in ("rt_critics_score", "rt_audience_score"):
        rt_val = doc.get(rt_key)
        if isinstance(rt_val, (int, float)) and rt_val > 0:
            rt_scores.append(float(rt_val))
    if rt_scores:
        rt_avg = sum(rt_scores) / len(rt_scores)
        q_bayes = 0.70 * q_bayes + 0.30 * rt_avg

    # Uncapped cultural footprint up to 1,000,000 votes (0-100)
    v_scale = min(100.0, (math.log10(vote_count + 1) / 6.0) * 100.0) if vote_count > 0 else 0.0

    # Bounded recency decay (0-100, 540-day half-life)
    now = datetime.now(tz=UTC)
    eff_dt = _parse_effective_date(doc)
    in_theatrical = False
    if eff_dt is not None:
        days_old = max(0.0, (now - eff_dt).total_seconds() / 86400.0)
        r_decay = 100.0 * math.pow(2.0, -days_old / _RECENCY_HALF_LIFE_DAYS)
        if doc.get("mc_type") == "movie" and days_old <= _THEATRICAL_WINDOW_DAYS:
            in_theatrical = True
    else:
        year = int(doc.get("year") or 0)
        if year > 0:
            approx_days = max(0.0, (now.year - year) * 365.25)
            r_decay = 100.0 * math.pow(2.0, -approx_days / _RECENCY_HALF_LIFE_DAYS)
        else:
            r_decay = 0.0

    # Language & US market affinity (-10 to +10)
    orig_lang = str(doc.get("original_language") or "").lower().strip()
    origin_countries_raw = doc.get("origin_country")
    origin_countries: list[str] = (
        [str(c).upper() for c in origin_countries_raw]
        if isinstance(origin_countries_raw, list)
        else []
    )
    if orig_lang == "en" or "US" in origin_countries:
        b_lang = 10.0
    elif orig_lang and vote_count < 1000:
        b_lang = -10.0
    else:
        b_lang = 0.0

    # Watchability / viability boost (0 to +12)
    wp = doc.get("watch_providers")
    b_viable = 0.0
    if isinstance(wp, dict):
        if wp.get("streaming_platform_ids") or in_theatrical:
            b_viable = 12.0
        elif wp.get("on_demand_platform_ids") or wp.get("primary_provider"):
            b_viable = 6.0
    elif in_theatrical:
        b_viable = 12.0

    score = (
        0.45 * popularity
        + 0.25 * q_bayes
        + 0.15 * v_scale
        + 0.15 * r_decay
        + b_lang
        + b_viable
    )
    return round(score, 3)


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


def score_media_result(query: str, doc: dict[str, Any]) -> tuple[int, float, float]:
    """
    Score a media result against a search query.

    Returns a tuple for sorting: (tier, -prominence, -popularity)
    Lower tier = better match. Negative prominence/popularity for descending sort.

    Args:
        query: The search query string
        doc: The media document with fields like search_title, director_name, etc.

    Returns:
        Tuple of (tier, -prominence, -popularity) for use as sort key
    """
    # Pre-normalize query ONCE
    query_lower = query.lower().strip()
    query_norm = normalize_for_match(query)
    query_compact = query_norm.replace("_", "")

    # Extract fields ONCE
    title_raw = (doc.get("search_title") or doc.get("title") or "").lower().strip()
    title_norm = normalize_for_match(title_raw)
    title_compact = str(doc.get("title_compact") or "") or title_norm.replace("_", "")
    director = doc.get("director_name") or ""
    cast_names: list[str] = doc.get("cast_names") or []
    keywords: list[str] = doc.get("keywords") or []
    genres: list[str] = doc.get("genres") or []

    prominence = compute_media_prominence(doc)
    popularity = float(doc.get("popularity") or 0)

    # Tier 0: EXACT title (raw) - case-insensitive but not normalized
    if query_lower == title_raw:
        return (0, -prominence, -popularity)

    # Tier 1: EXACT title (normalized)
    if query_norm == title_norm:
        return (1, -prominence, -popularity)

    # Tier 1: EXACT title (leading article ignored on either side)
    if strip_leading_article(query_norm) == strip_leading_article(title_norm):
        return (1, -prominence, -popularity)

    # Tier 1: EXACT title (compact) — collapsed-token queries like "goodwillhunting"
    if len(query_compact) >= 4 and query_compact == title_compact:
        return (1, -prominence, -popularity)

    # Tier 2: EXACT director_name
    if query_norm == director:
        return (2, -prominence, -popularity)

    # Tier 3: EXACT cast_names (any match)
    if any(query_norm == c for c in cast_names):
        return (3, -prominence, -popularity)

    # Tier 4: EXACT keywords (any match)
    if any(query_norm == k for k in keywords):
        return (4, -prominence, -popularity)

    # Get IPTC expanded aliases for later checks
    query_aliases = get_search_aliases(query_norm)
    expanded_aliases = [a for a in query_aliases if a != query_norm and len(a) > 3]

    # Tier 4.1: Title contains IPTC alias (e.g., "AI" -> "artificial_intelligence"
    # matches title "A.I. Artificial Intelligence" which normalizes to contain "artificial_intelligence")
    # Title matches rank higher than keyword-only matches
    if expanded_aliases and any(alias in title_norm for alias in expanded_aliases):
        return (4, -prominence, -popularity)

    # Tier 4.2: Keyword starts with IPTC alias (e.g., "AI" -> "artificial_intelligence"
    # matches keyword "artificial_intelligence_a_i")
    # This ranks higher than simple word/substring matches because it indicates
    # the content is specifically ABOUT the topic
    if expanded_aliases and any(
        any(k.startswith(alias) for alias in expanded_aliases) for k in keywords
    ):
        return (5, -prominence, -popularity)

    # Tier 5: EXACT genres (any match)
    if any(query_norm == g for g in genres):
        return (5, -prominence, -popularity)

    # Tier 6: CONTAINS_WORD title (query is complete token in title)
    title_tokens = title_norm.split("_")
    if query_norm in title_tokens:
        return (6, -prominence, -popularity)

    # Tier 7: CONTAINS_WORD director_name
    if director and query_norm in director.split("_"):
        return (7, -prominence, -popularity)

    # Tier 8: CONTAINS_WORD cast_names
    if any(query_norm in c.split("_") for c in cast_names):
        return (8, -prominence, -popularity)

    # Tier 9: CONTAINS_WORD keywords
    if any(query_norm in k.split("_") for k in keywords):
        return (9, -prominence, -popularity)

    # Tier 10: CONTAINS_WORD genres
    if any(query_norm in g.split("_") for g in genres):
        return (10, -prominence, -popularity)

    # Tier 11: CONTAINS_SUBSTRING title (raw query or IPTC alias)
    # e.g., "AI" query with title "A.I. Artificial Intelligence" - check if any IPTC alias
    # like "artificial_intelligence" is in the normalized title
    if query_norm in title_norm:
        return (11, -prominence, -popularity)

    # Compact substring (e.g. "goodwillhunt" in "goodwillhunting")
    if len(query_compact) >= 4 and query_compact in title_compact:
        return (11, -prominence, -popularity)

    # Also check IPTC-expanded aliases in title (e.g., "AI" -> "artificial_intelligence")
    query_aliases = get_search_aliases(query_norm)
    if any(alias in title_norm for alias in query_aliases if alias != query_norm):
        return (11, -prominence, -popularity)

    # Tier 12: CONTAINS_SUBSTRING any TAG field (raw query or IPTC alias)
    if (
        (director and query_norm in director)
        or any(query_norm in c for c in cast_names)
        or any(query_norm in k for k in keywords)
        or any(query_norm in g for g in genres)
    ):
        return (12, -prominence, -popularity)

    # Also check IPTC-expanded aliases in keywords
    # e.g., "AI" -> ["artificial_intelligence"] matches keyword "artificial_intelligence_a_i"
    if any(any(alias in k for alias in query_aliases if alias != query_norm) for k in keywords):
        return (12, -prominence, -popularity)

    # Tier 13: PREFIX title
    if title_norm.startswith(query_norm):
        return (13, -prominence, -popularity)

    # Compact prefix (e.g. "goodwillhunt" -> "goodwillhunting")
    if len(query_compact) >= 4 and title_compact.startswith(query_compact):
        return (13, -prominence, -popularity)

    # Tier 14: PREFIX any TAG field
    if (
        (director and director.startswith(query_norm))
        or any(c.startswith(query_norm) for c in cast_names)
        or any(k.startswith(query_norm) for k in keywords)
        or any(g.startswith(query_norm) for g in genres)
    ):
        return (14, -prominence, -popularity)

    # Tier 15: Fallback (no match found)
    return (15, -prominence, -popularity)


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


def _words_for_exact_title(value: str) -> str:
    """Lowercase and drop punctuation, keeping spaces as word boundaries.

    ``"It's Complicated"`` and ``"Its Complicated"`` both become
    ``"its complicated"``. ``"VisionQuest"`` stays one word and does not
    equal ``"Vision Quest"``.
    """
    words: list[str] = []
    current: list[str] = []
    for char in value.lower():
        if char.isalnum():
            current.append(char)
        elif current:
            words.append("".join(current))
            current = []
    if current:
        words.append("".join(current))
    return " ".join(words)


def media_titles_match_exactly(query: str, doc: dict[str, Any]) -> bool:
    """Return True when the query and media title are the same words.

    Case and punctuation are ignored. Spaces are not. ``"VisionQuest"``
    matches ``VisionQuest`` and does not match ``Vision Quest``. The
    collapsed form remains a lower-scoring compact hit.
    """
    title_raw = (doc.get("search_title") or doc.get("title") or "").strip()
    if not title_raw:
        return False
    query_clean = query.strip()
    if query_clean.lower() == title_raw.lower():
        return True
    return _words_for_exact_title(query_clean) == _words_for_exact_title(title_raw)


def media_titles_match_ignoring_article(query: str, doc: dict[str, Any]) -> bool:
    """Return True when query and title match after stripping a leading the/a/an."""
    title_raw = (doc.get("search_title") or doc.get("title") or "").strip()
    if not title_raw:
        return False
    query_norm = normalize_for_match(query.strip())
    title_norm = normalize_for_match(title_raw)
    return strip_leading_article(query_norm) == strip_leading_article(title_norm)


def is_exact_match(query: str, doc: dict[str, Any], source: str) -> bool:
    """
    Return True if doc is an exact match for the query (tier 0-1 title/name match).

    Used to surface the single best match when the user's query exactly matches
    a known entity (e.g., "the godfather" -> movie "The Godfather").

    Definition of exact:
    - Media (tv, movie): tier 0-1, including article-insensitive title equality
      and space-collapsed compact titles. Word-level exact matches outrank compact-only.
    - Person: tier 0-1 (exact name)
    - Book: tier 0-1 (exact title)
    - Podcast: tier 0-1 (exact title raw/normalized)
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
        return tier <= 1
    if source == "author":
        name = (doc.get("search_title") or doc.get("name") or "").strip()
        if not name:
            return False
        query_norm = normalize_for_match(q)
        name_norm = normalize_for_match(name.lower())
        return query_norm == name_norm

    return False
