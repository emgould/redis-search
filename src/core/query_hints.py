"""
Source hint parsing for search queries.

When a user types "the godfather movie" or "podcast joe rogan" without explicitly
setting a sources filter, we detect hint tokens and may infer the source scope.
Search runs both the literal query and a hint-stripped query (see SourceHintPlan).

Keywords (and their plurals): podcast, movie, video, book, actor, actress,
          author, artist, album, tv series, tv show(s), tvshow, tvseries (-> tv).
Excluded: news, ratings, standalone tv, standalone show.
"""

from dataclasses import dataclass

# Multi-word keywords (suffix/prefix): (lowercase tuple) -> source
_MULTI_WORD_KEYWORDS: dict[tuple[str, ...], str] = {
    ("tv", "series"): "tv",
    ("tv", "show"): "tv",
    ("tv", "shows"): "tv",
}

# Single-word keywords: lowercase -> source
_SINGLE_WORD_KEYWORDS: dict[str, str] = {
    "podcast": "podcast",
    "podcasts": "podcast",
    "movie": "movie",
    "movies": "movie",
    "video": "video",
    "videos": "video",
    "book": "book",
    "books": "book",
    "actor": "person",
    "actors": "person",
    "actress": "person",
    "actresses": "person",
    "author": "author",
    "authors": "author",
    "artist": "artist",
    "artists": "artist",
    "album": "album",
    "albums": "album",
    "tvshow": "tv",
    "tvseries": "tv",
}

MIN_STRIPPED_LENGTH = 3


def parse_source_hint(query: str) -> tuple[str, set[str] | None]:
    """
    Parse source hint from query suffix or prefix.

    When sources is not specified (global search), a user can type keywords
    like "the godfather movie" or "podcast joe rogan" to restrict the search.
    Rightmost suffix wins; if no suffix match, leftmost prefix wins.

    Returns:
        (cleaned_query, sources) if a hint was found and stripped query is valid,
        (original_query, None) otherwise.

    Examples:
        "godfather movie" -> ("godfather", {"movie"})
        "podcast joe rogan" -> ("joe rogan", {"podcast"})
        "breaking bad tv show" -> ("breaking bad", {"tv"})
        "something movie podcast" -> ("something movie", {"podcast"})
    """
    if not query or not query.strip():
        return (query, None)

    tokens = query.strip().split()
    if not tokens:
        return (query, None)

    lower_tokens = [t.lower() for t in tokens]
    matched_source: str | None = None
    strip_count = 0
    strip_from_end = True  # suffix first

    # Check suffix: multi-word first
    for (kw1, kw2), source in _MULTI_WORD_KEYWORDS.items():
        if len(lower_tokens) >= 2 and (lower_tokens[-2], lower_tokens[-1]) == (kw1, kw2):
            matched_source = source
            strip_count = 2
            strip_from_end = True
            break

    # Check suffix: single-word
    if not matched_source and lower_tokens[-1] in _SINGLE_WORD_KEYWORDS:
        matched_source = _SINGLE_WORD_KEYWORDS[lower_tokens[-1]]
        strip_count = 1
        strip_from_end = True

    # Check prefix: multi-word
    if not matched_source and len(lower_tokens) >= 2:
        for (kw1, kw2), source in _MULTI_WORD_KEYWORDS.items():
            if (lower_tokens[0], lower_tokens[1]) == (kw1, kw2):
                matched_source = source
                strip_count = 2
                strip_from_end = False
                break

    # Check prefix: single-word
    if not matched_source and lower_tokens[0] in _SINGLE_WORD_KEYWORDS:
        matched_source = _SINGLE_WORD_KEYWORDS[lower_tokens[0]]
        strip_count = 1
        strip_from_end = False

    if not matched_source:
        return (query, None)

    if strip_from_end:
        stripped_tokens = tokens[:-strip_count]
    else:
        stripped_tokens = tokens[strip_count:]

    stripped = " ".join(stripped_tokens).strip()
    if len(stripped) < MIN_STRIPPED_LENGTH:
        return (query, None)

    return (stripped, {matched_source})


@dataclass(frozen=True)
class SourceHintPlan:
    """Literal user query plus a hint-stripped variant for union search."""

    literal_q: str
    stripped_q: str
    hinted_sources: frozenset[str]

    @property
    def source_hint_applied(self) -> list[str]:
        return sorted(self.hinted_sources)


def build_source_hint_plan(query: str) -> SourceHintPlan | None:
    """
    Build a dual-query plan when a prefix/suffix source hint is present.

    Returns None when no hint matches or the stripped query is too short.
    """
    if not query or not query.strip():
        return None
    literal = query.strip()
    stripped, hinted = parse_source_hint(literal)
    if not hinted or stripped == literal:
        return None
    return SourceHintPlan(
        literal_q=literal,
        stripped_q=stripped,
        hinted_sources=frozenset(hinted),
    )


def apply_source_hint_to_sources(
    explicit_sources: set[str] | None,
    plan: SourceHintPlan | None,
) -> set[str] | None:
    """Use hinted sources only when the client did not pass ``sources``."""
    if explicit_sources is not None:
        return explicit_sources
    if plan is None:
        return None
    return set(plan.hinted_sources)


def resolve_query_source_hint(
    query: str,
    explicit_sources: set[str] | None,
    *,
    raw: bool = False,
) -> tuple[str, set[str] | None, list[str] | None]:
    """
    HTTP-layer helper: never rewrite ``query``; only infer sources when omitted.

    Returns:
        (query, sources, source_hint_applied)
    """
    if raw or not query or not query.strip():
        return query, explicit_sources, None

    plan = build_source_hint_plan(query)
    if plan is None:
        return query, explicit_sources, None

    sources = apply_source_hint_to_sources(explicit_sources, plan)
    return query, sources, plan.source_hint_applied
