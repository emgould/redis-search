import re

from core.iptc import get_search_aliases
from utils.get_logger import get_logger

logger = get_logger(__name__)


class RawQueryError(ValueError):
    """Raised when raw mode query fails validation."""


# Backward-compatible alias
RawMediaQueryError = RawQueryError

# Per-index TAG/NUMERIC fields allowed for raw passthrough
ALLOWED_RAW_MEDIA_FIELDS = frozenset(
    {
        "keywords",
        "genres",
        "genre_ids",
        "cast_names",
        "cast_ids",
        "director_name",
        "director_id",
        "mc_type",
        "mc_subtype",
        "source",
        "origin_country",
        "year",
        "rating",
        "popularity",
    }
)

ALLOWED_RAW_PODCAST_FIELDS = frozenset(
    {
        "author_normalized",
        "mc_type",
        "source",
        "id",
        "language",
        "categories",
        "popularity",
        "episode_count",
    }
)

ALLOWED_RAW_PEOPLE_FIELDS = frozenset(
    {
        "mc_type",
        "mc_subtype",
        "source",
        "popularity",
    }
)

ALLOWED_RAW_BOOK_FIELDS = frozenset(
    {
        "mc_type",
        "source",
        "openlibrary_key",
        "primary_isbn13",
        "primary_isbn10",
        "author_olid",
        "cover_available",
        "author_normalized",
        "subjects",
        "first_publish_year",
        "ratings_average",
        "ratings_count",
        "readinglog_count",
        "number_of_pages",
        "popularity_score",
        "edition_count",
    }
)

ALLOWED_RAW_AUTHOR_FIELDS = frozenset(
    {
        "mc_type",
        "mc_subtype",
        "source",
        "wikidata_id",
        "openlibrary_key",
        "work_count",
        "quality_score",
        "birth_year",
    }
)

# Union of all per-index fields for general validation
ALL_ALLOWED_RAW_FIELDS = (
    ALLOWED_RAW_MEDIA_FIELDS
    | ALLOWED_RAW_PODCAST_FIELDS
    | ALLOWED_RAW_PEOPLE_FIELDS
    | ALLOWED_RAW_BOOK_FIELDS
    | ALLOWED_RAW_AUTHOR_FIELDS
)


def validate_raw_query(q: str) -> None:
    """
    Validate that the query is safe RediSearch syntax for any indexed source.

    Checks:
    - Query must start with @ (field syntax)
    - All field names must be in ALL_ALLOWED_RAW_FIELDS (union of all indices)
    - Braces and brackets must be balanced

    Raises:
        RawQueryError: If validation fails
    """
    if not q or not q.strip():
        raise RawQueryError("Raw query cannot be empty")

    s = q.strip()

    # Must start with @ to indicate field syntax
    if not s.startswith("@"):
        raise RawQueryError("Raw query must start with @field:{value} or @field:[range]")

    # Extract all field names: @fieldname: or @fieldname:[
    field_matches = re.findall(r"@(\w+)\s*[:\[\{]", s)
    for field_name in field_matches:
        if field_name.lower() not in ALL_ALLOWED_RAW_FIELDS:
            raise RawQueryError(
                f"Disallowed field '{field_name}' in raw query. "
                f"Allowed: {', '.join(sorted(ALL_ALLOWED_RAW_FIELDS))}"
            )

    # Basic balance check for {} and []
    open_braces = 0
    open_brackets = 0
    for c in s:
        if c == "{":
            open_braces += 1
        elif c == "}":
            open_braces -= 1
        elif c == "[":
            open_brackets += 1
        elif c == "]":
            open_brackets -= 1
        if open_braces < 0 or open_brackets < 0:
            raise RawQueryError("Unbalanced delimiters in raw query")
    if open_braces != 0 or open_brackets != 0:
        raise RawQueryError("Unbalanced delimiters in raw query")


# Backward-compatible alias
validate_raw_media_query = validate_raw_query


def build_media_query_from_user_input(
    q: str, raw: bool = False, include_tag_fields: bool = True
) -> str:
    """
    Build media index query from user input.

    When raw=True, validates and passes through the query. Otherwise uses
    build_autocomplete_query.
    """
    if raw:
        validate_raw_query(q)
        return q.strip()
    return build_autocomplete_query(q, include_tag_fields=include_tag_fields)

# Common stopwords that Redis Search ignores
STOPWORDS = {"the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of", "is", "it"}

# Regex to strip apostrophes from search queries.
# Must match the normalization applied to search_title at index time
# (see normalize_search_title in core.normalize).
_APOSTROPHE_RE = re.compile(r"[\u0027\u2018\u2019\u02BC]")  # ' ' ' ʼ


def strip_query_apostrophes(q: str) -> str:
    """Strip apostrophes from a search query to match indexed titles.

    RediSearch tokenizes apostrophes as word separators, so indexed titles
    have apostrophes removed (e.g. "It's Complicated" → "Its Complicated").
    This function applies the same normalization to user queries.
    """
    return _APOSTROPHE_RE.sub("", q)


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
    # Strip apostrophes to match indexed titles (e.g. "it's" -> "its")
    q = strip_query_apostrophes(q)

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

    # Build title query (TEXT field)
    # Short queries (<=3 chars) use EXACT match to avoid false positives
    # e.g., "AI" shouldn't match "Air Force One", "Airplane", etc.
    # Longer queries use prefix matching for autocomplete behavior
    if len(escaped_words) == 1:
        word = escaped_words[0]
        if len(word) <= 3:
            # Exact match for short single-word queries
            title_query = f"@search_title:{word}"

            # Also search title for IPTC-expanded aliases (e.g., "AI" -> "artificial intelligence")
            # This helps find titles like "A.I. Artificial Intelligence"
            normalized_word = normalize_for_tag(word)
            title_aliases = get_search_aliases(normalized_word)
            # Find expanded aliases (not the raw query) that are longer
            expanded_title_searches = []
            for alias in title_aliases:
                if alias != normalized_word and len(alias) > 3:
                    # Convert normalized alias back to space-separated for title search
                    title_alias = alias.replace("_", " ")
                    expanded_title_searches.append(f"@search_title:({title_alias}*)")

            if expanded_title_searches:
                # Combine exact match with expanded alias searches
                title_query = (
                    f"({title_query})"
                    + " | "
                    + " | ".join(f"({s})" for s in expanded_title_searches)
                )
        else:
            # Prefix match for longer queries
            title_query = f"@search_title:{word}*"
    else:
        # Multi-word: all words except last should be exact, last word is prefix
        exact_words = " ".join(escaped_words[:-1])
        last_word = escaped_words[-1]
        if len(last_word) <= 3:
            # Exact match for short last word
            title_query = f"@search_title:({exact_words} {last_word})"
        else:
            # Prefix match for longer last word
            title_query = f"@search_title:({exact_words} {last_word}*)"

    # If not including TAG fields, return just title query
    if not include_tag_fields:
        return title_query

    # Build TAG field queries for union search
    # Normalize the full query for TAG matching (e.g., "Tom Hanks" -> "tom_hanks")
    normalized_full = normalize_for_tag(q)

    # Build union query parts
    query_parts = [title_query]

    # For TAG fields (cast, director, genres):
    # - Short queries (<=3 chars) use exact matching to avoid false positives
    # - Longer queries use prefix matching for autocomplete behavior
    min_tag_length = 2
    use_prefix = len(normalized_full) > 3  # Only prefix match if > 3 chars

    if normalized_full and len(normalized_full) >= min_tag_length:
        # Pattern for non-keyword TAG fields
        tag_pattern = f"{normalized_full}*" if use_prefix else normalized_full
        match_type = "prefix" if use_prefix else "exact"

        # Log the TAG search
        logger.info(
            f"[Media Index] Query: '{q}' -> TAG: '{tag_pattern}' ({match_type}) "
            f"(searching: cast_names, director_name, genres)"
        )

        # Cast names - search for actor/actress names
        query_parts.append(f"@cast_names:{{{tag_pattern}}}")

        # Director name - search for director
        query_parts.append(f"@director_name:{{{tag_pattern}}}")

        # Genres - same pattern as cast/director
        query_parts.append(f"@genres:{{{tag_pattern}}}")

        # Keywords - use IPTC alias expansion
        # - Raw query alias (e.g., "ai"): EXACT match to avoid "ai*" matching "aircraft_carrier"
        # - Expanded aliases (e.g., "artificial_intelligence"): PREFIX match to find "artificial_intelligence_a_i"
        keyword_aliases = get_search_aliases(normalized_full)
        keyword_patterns = []
        for alias in keyword_aliases:
            if alias == normalized_full:
                # Raw query - exact match to avoid false positives
                keyword_patterns.append(alias)
            else:
                # Expanded alias - prefix match to find variations
                keyword_patterns.append(f"{alias}*")
        keyword_union = "|".join(keyword_patterns)
        query_parts.append(f"@keywords:{{{keyword_union}}}")

        logger.info(
            f"[Media Index] Query: '{q}' -> Keywords: {len(keyword_aliases)} aliases "
            f"(raw=exact, expanded=prefix: {keyword_aliases[:5]}{'...' if len(keyword_aliases) > 5 else ''})"
        )

    # Combine with OR (union)
    if len(query_parts) == 1:
        return query_parts[0]

    return " | ".join(f"({part})" for part in query_parts)


def build_fuzzy_fulltext_query(q: str) -> str:
    """Build a fuzzy full-text search query."""
    # Strip apostrophes to match indexed titles
    q = strip_query_apostrophes(q)

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


def build_podcast_autocomplete_query(q: str, include_tag_fields: bool = True) -> str:
    """
    Build a prefix search query for podcast autocomplete.

    When include_tag_fields=True (default), creates a union query that searches:
    - search_title (TEXT field with prefix matching)
    - author_normalized (TAG field - matches podcast author/creator)
    - categories (TAG field - matches podcast categories)
    """
    # Strip apostrophes to match indexed titles
    q = strip_query_apostrophes(q)

    # Split on both spaces and colons, then flatten
    parts = q.replace(":", " : ").split()
    words = [w.lower() for w in parts if w and w != ":"]
    words = [w for w in words if w and w not in STOPWORDS]

    if not words:
        return "*"

    # Escape special characters in search terms
    escaped_words = [escape_redis_search_term(w) for w in words]

    # Build title query (TEXT field with prefix matching)
    if len(escaped_words) == 1:
        title_query = f"@search_title:{escaped_words[0]}*"
    else:
        exact_words = " ".join(escaped_words[:-1])
        prefix_word = escaped_words[-1]
        title_query = f"@search_title:({exact_words} {prefix_word}*)"

    if not include_tag_fields:
        return title_query

    # Normalize full query for TAG matching
    normalized_full = normalize_for_tag(q)

    query_parts = [title_query]

    # Short queries (<=3 chars) use exact matching to avoid false positives
    min_tag_length = 2
    use_prefix = len(normalized_full) > 3

    if normalized_full and len(normalized_full) >= min_tag_length:
        tag_pattern = f"{normalized_full}*" if use_prefix else normalized_full
        match_type = "prefix" if use_prefix else "exact"

        # Log the TAG aliases being searched
        logger.info(
            f"[Podcast Index] Query: '{q}' -> TAG alias: '{tag_pattern}' ({match_type}) "
            f"(searching: author_normalized, categories)"
        )

        # Author - search for podcast creator
        query_parts.append(f"@author_normalized:{{{tag_pattern}}}")

        # Categories - use IPTC alias expansion with EXACT matches
        # e.g., "ai" -> ["ai", "artificial_intelligence", "machine_intelligence", ...]
        category_aliases = get_search_aliases(normalized_full)
        category_union = "|".join(category_aliases)
        query_parts.append(f"@categories:{{{category_union}}}")

    if len(query_parts) == 1:
        return query_parts[0]

    return " | ".join(f"({part})" for part in query_parts)


def build_books_autocomplete_query(q: str, include_tag_fields: bool = True) -> str:
    """
    Build a search query for books autocomplete.

    Creates a union query that searches:
    - search_title (TEXT field with prefix matching)
    - author (TEXT field with prefix matching)
    - author_normalized (TAG field - matches normalized author name)
    - subjects (TAG field - matches normalized subjects with IPTC expansion)
    - description (TEXT field - lower priority)

    Short query rules (<=3 chars):
    - Title: exact match
    - Skip author/subjects TAGs to avoid false positives

    Args:
        q: Search query string
        include_tag_fields: Whether to include TAG field union search

    Returns:
        RediSearch query string
    """
    # Strip apostrophes to match indexed titles
    q = strip_query_apostrophes(q)

    # Split on both spaces and colons, then flatten
    parts = q.replace(":", " : ").split()
    words = [w.lower() for w in parts if w and w != ":"]
    words = [w for w in words if w and w not in STOPWORDS]

    if not words:
        return "*"

    # Escape special characters in search terms
    escaped_words = [escape_redis_search_term(w) for w in words]

    # Build title query (TEXT field)
    # Short queries (<=3 chars) use exact match
    # Longer queries use prefix matching for autocomplete
    if len(escaped_words) == 1:
        word = escaped_words[0]
        if len(word) <= 3:
            title_query = f"@search_title:{word}"
        else:
            title_query = f"@search_title:{word}*"
    else:
        exact_words = " ".join(escaped_words[:-1])
        last_word = escaped_words[-1]
        if len(last_word) <= 3:
            title_query = f"@search_title:({exact_words} {last_word})"
        else:
            title_query = f"@search_title:({exact_words} {last_word}*)"

    # Build author TEXT query (same pattern as title)
    if len(escaped_words) == 1:
        word = escaped_words[0]
        if len(word) <= 3:
            author_query = f"@author:{word}"
        else:
            author_query = f"@author:{word}*"
    else:
        exact_words = " ".join(escaped_words[:-1])
        last_word = escaped_words[-1]
        if len(last_word) <= 3:
            author_query = f"@author:({exact_words} {last_word})"
        else:
            author_query = f"@author:({exact_words} {last_word}*)"

    # Build description query (TEXT field, lower priority)
    # Description uses same pattern but will rank lower in tiered scoring
    if len(escaped_words) == 1:
        word = escaped_words[0]
        if len(word) <= 3:
            desc_query = f"@description:{word}"
        else:
            desc_query = f"@description:{word}*"
    else:
        exact_words = " ".join(escaped_words[:-1])
        last_word = escaped_words[-1]
        if len(last_word) <= 3:
            desc_query = f"@description:({exact_words} {last_word})"
        else:
            desc_query = f"@description:({exact_words} {last_word}*)"

    query_parts = [title_query, author_query, desc_query]

    if not include_tag_fields:
        return " | ".join(f"({part})" for part in query_parts)

    # Normalize full query for TAG matching
    normalized_full = normalize_for_tag(q)

    # For TAG fields (author_normalized, subjects):
    # - Short queries (<=3 chars) use exact matching
    # - Longer queries use prefix matching
    min_tag_length = 2
    use_prefix = len(normalized_full) > 3

    if normalized_full and len(normalized_full) >= min_tag_length:
        tag_pattern = f"{normalized_full}*" if use_prefix else normalized_full
        match_type = "prefix" if use_prefix else "exact"

        logger.info(
            f"[Book Index] Query: '{q}' -> TAG: '{tag_pattern}' ({match_type}) "
            f"(searching: author_normalized, subjects)"
        )

        # Author normalized - exact/prefix TAG match
        query_parts.append(f"@author_normalized:{{{tag_pattern}}}")

        # Subjects - use IPTC alias expansion with exact matches
        # e.g., "mystery" -> ["mystery", "detective_fiction", ...]
        subject_aliases = get_search_aliases(normalized_full)
        subject_union = "|".join(subject_aliases)
        query_parts.append(f"@subjects:{{{subject_union}}}")

        logger.info(
            f"[Book Index] Query: '{q}' -> Subjects: {len(subject_aliases)} aliases "
            f"({subject_aliases[:5]}{'...' if len(subject_aliases) > 5 else ''})"
        )

    return " | ".join(f"({part})" for part in query_parts)


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
    raw: bool = False,
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
        raw: If True, treat q as validated raw RediSearch syntax (passthrough)

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
    has_filter_params = any(
        [genre_ids, cast_ids, year_min, year_max, rating_min, rating_max, mc_type]
    )

    # Text query component (with TAG field union when enabled, or raw passthrough)
    if q and len(q.strip()) >= 2:
        text_query = build_media_query_from_user_input(
            q, raw=raw, include_tag_fields=include_tag_fields
        )
        # Only add if it's not just "*"
        if text_query != "*":
            # Wrap in parentheses when combining with filters to preserve precedence
            wrap_in_parens = has_filter_params or raw
            parts.append(f"({text_query})" if wrap_in_parens else text_query)

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
