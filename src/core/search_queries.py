
# Common stopwords that Redis Search ignores
STOPWORDS = {"the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of", "is", "it"}


def build_autocomplete_query(q: str) -> str:
    """
    Build a prefix search query for autocomplete.
    Handles multi-word queries and filters out stopwords.
    """
    words = q.lower().split()
    # Filter out stopwords and empty strings
    words = [w for w in words if w and w not in STOPWORDS]

    if not words:
        # If only stopwords, return a broad match
        return "*"

    # For multi-word: match documents containing all words (last word as prefix)
    if len(words) == 1:
        return f"@search_title:{words[0]}*"
    else:
        # All words except last should be exact, last word is prefix
        exact_words = " ".join(words[:-1])
        prefix_word = words[-1]
        return f"@search_title:({exact_words} {prefix_word}*)"


def build_fuzzy_fulltext_query(q: str) -> str:
    """Build a fuzzy full-text search query."""
    words = q.lower().split()
    words = [w for w in words if w and w not in STOPWORDS]

    if not words:
        return "*"

    # Fuzzy match on each word
    fuzzy_terms = " ".join(f"%{w}%" for w in words)
    return f"@search_title:({fuzzy_terms})"


def build_filter_query(
    q: str | None = None,
    genre_ids: list[str] | None = None,
    genre_match: str = "any",
    cast_ids: list[str] | None = None,
    cast_match: str = "any",
    year_min: int | None = None,
    year_max: int | None = None,
    rating_min: float | None = None,
    rating_max: float | None = None,
    mc_type: str | None = None,
) -> str:
    """
    Build a RediSearch query combining text search and field filters.

    Supports field-only filtering (no text query required) for browsing/discovery.

    Args:
        q: Optional text search query (searches search_title field)
        genre_ids: List of TMDB genre IDs to filter by
        genre_match: "any" for OR logic (default), "all" for AND logic
        cast_ids: List of TMDB person IDs to filter by
        cast_match: "any" for OR logic (default), "all" for AND logic
        year_min: Minimum release year (inclusive)
        year_max: Maximum release year (inclusive)
        rating_min: Minimum rating 0-10 (inclusive)
        rating_max: Maximum rating 0-10 (inclusive)
        mc_type: Filter by media type (movie, tv)

    Returns:
        RediSearch query string

    Examples:
        # All sci-fi movies from 2020+
        build_filter_query(genre_ids=["878"], year_min=2020, mc_type="movie")
        # Returns: "@genre_ids:{878} @year:[2020 +inf] @mc_type:{movie}"

        # Comedy OR Drama movies
        build_filter_query(genre_ids=["35", "18"], genre_match="any")
        # Returns: "@genre_ids:{35|18}"

        # Comedy AND Drama movies (must have both)
        build_filter_query(genre_ids=["35", "18"], genre_match="all")
        # Returns: "@genre_ids:{35} @genre_ids:{18}"

        # Movies with Brad Pitt (person ID 287)
        build_filter_query(cast_ids=["287"], mc_type="movie")
        # Returns: "@cast_ids:{287} @mc_type:{movie}"

        # Movies with both Brad Pitt AND George Clooney
        build_filter_query(cast_ids=["287", "1461"], cast_match="all")
        # Returns: "@cast_ids:{287} @cast_ids:{1461}"
    """
    parts: list[str] = []

    # Text query component
    if q and len(q.strip()) >= 2:
        text_query = build_autocomplete_query(q)
        # Only add if it's not just "*"
        if text_query != "*":
            parts.append(text_query)

    # Genre filter
    if genre_ids:
        escaped_ids = [gid.replace("-", "\\-") for gid in genre_ids]
        if genre_match == "all":
            # AND logic: separate clauses for each genre
            for gid in escaped_ids:
                parts.append(f"@genre_ids:{{{gid}}}")
        else:
            # OR logic (default): join with |
            genre_filter = "|".join(escaped_ids)
            parts.append(f"@genre_ids:{{{genre_filter}}}")

    # Cast filter
    if cast_ids:
        escaped_ids = [cid.replace("-", "\\-") for cid in cast_ids]
        if cast_match == "all":
            # AND logic: separate clauses for each cast member
            for cid in escaped_ids:
                parts.append(f"@cast_ids:{{{cid}}}")
        else:
            # OR logic (default): join with |
            cast_filter = "|".join(escaped_ids)
            parts.append(f"@cast_ids:{{{cast_filter}}}")

    # Year range filter
    if year_min is not None or year_max is not None:
        min_val = str(year_min) if year_min is not None else "-inf"
        max_val = str(year_max) if year_max is not None else "+inf"
        parts.append(f"@year:[{min_val} {max_val}]")

    # Rating range filter
    if rating_min is not None or rating_max is not None:
        min_val = str(rating_min) if rating_min is not None else "-inf"
        max_val = str(rating_max) if rating_max is not None else "+inf"
        parts.append(f"@rating:[{min_val} {max_val}]")

    # Media type filter
    if mc_type:
        parts.append(f"@mc_type:{{{mc_type}}}")

    # If no filters at all, return match-all
    if not parts:
        return "*"

    return " ".join(parts)
