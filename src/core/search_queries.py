import re

# Common stopwords that Redis Search ignores
STOPWORDS = {"the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of", "is", "it"}


def normalize_for_tag(value: str) -> str:
    """
    Normalize a value for TAG field search (must match how tags are stored).

    - Lowercase
    - Replace spaces and special characters with underscore
    - Remove leading/trailing underscores
    """
    if not value:
        return ""
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def escape_redis_search_term(term: str) -> str:
    """
    Escape special characters in a Redis Search term.

    Redis Search treats certain characters as special. When these appear in search
    terms (after @field:), they need to be escaped with a backslash.

    Args:
        term: The search term to escape

    Returns:
        Escaped search term safe for use in Redis Search queries
    """
    # Characters that need escaping in Redis Search queries
    # Note: We don't escape * since we use it for wildcards
    special_chars = {
        ":",
        ",",
        ".",
        "<",
        ">",
        "{",
        "}",
        "[",
        "]",
        '"',
        "'",
        ";",
        "!",
        "@",
        "#",
        "$",
        "%",
        "^",
        "&",
        "(",
        ")",
        "-",
        "+",
        "=",
        "~",
    }
    result = ""
    for char in term:
        if char in special_chars:
            result += "\\" + char
        else:
            result += char
    return result


def build_autocomplete_query(q: str, include_tag_fields: bool = True) -> str:
    """
    Build a prefix search query for autocomplete.
    Handles multi-word queries and filters out stopwords.
    Also splits on colons to handle titles like "Predator:Badlands".

    When include_tag_fields=True (default), creates a union query that searches:
    - search_title (TEXT field with prefix matching)
    - cast_names (TAG field - matches actor names)
    - director_name (TAG field - matches director name)
    - keywords (TAG field - matches content keywords)
    - genres (TAG field - matches genre names)
    """
    # Split on both spaces and colons, then flatten
    # This handles cases like "Predator:Badlands" or "Predator: Badlands"
    parts = q.replace(":", " : ").split()
    words = [w.lower() for w in parts if w and w != ":"]
    # Filter out stopwords and empty strings
    words = [w for w in words if w and w not in STOPWORDS]

    if not words:
        # If only stopwords, return a broad match
        return "*"

    # Escape special characters in search terms
    escaped_words = [escape_redis_search_term(w) for w in words]

    # Build title query (TEXT field with prefix matching)
    if len(escaped_words) == 1:
        title_query = f"@search_title:{escaped_words[0]}*"
    else:
        # All words except last should be exact, last word is prefix
        exact_words = " ".join(escaped_words[:-1])
        prefix_word = escaped_words[-1]
        title_query = f"@search_title:({exact_words} {prefix_word}*)"

    # If not including TAG fields, return just title query
    if not include_tag_fields:
        return title_query

    # Build TAG field queries for union search
    # Normalize the full query for TAG matching (e.g., "Tom Hanks" -> "tom_hanks")
    normalized_full = normalize_for_tag(q)

    # Build union query parts
    query_parts = [title_query]

    # For TAG fields, we search for the normalized full query
    # TAG fields support prefix matching with * suffix
    if normalized_full and len(normalized_full) >= 2:
        # Cast names - search for actor/actress names
        query_parts.append(f"@cast_names:{{{normalized_full}*}}")

        # Director name - search for director
        query_parts.append(f"@director_name:{{{normalized_full}*}}")

        # Keywords - search for content keywords
        query_parts.append(f"@keywords:{{{normalized_full}*}}")

        # Genres - search for genre names (e.g., "science_fiction")
        query_parts.append(f"@genres:{{{normalized_full}*}}")

    # Combine with OR (union)
    if len(query_parts) == 1:
        return query_parts[0]

    return " | ".join(f"({part})" for part in query_parts)


def build_fuzzy_fulltext_query(q: str) -> str:
    """Build a fuzzy full-text search query."""
    # Split on both spaces and colons, then flatten
    parts = q.replace(":", " : ").split()
    words = [w.lower() for w in parts if w and w != ":"]
    words = [w for w in words if w and w not in STOPWORDS]

    if not words:
        return "*"

    # Escape special characters in search terms
    escaped_words = [escape_redis_search_term(w) for w in words]

    # Fuzzy match on each word
    fuzzy_terms = " ".join(f"%{w}%" for w in escaped_words)
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
    include_tag_fields: bool = True,
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
        include_tag_fields: Include TAG field union search (cast_names, director_name, keywords, genres)

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

    # Text query component (with TAG field union when enabled)
    if q and len(q.strip()) >= 2:
        text_query = build_autocomplete_query(q, include_tag_fields=include_tag_fields)
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
