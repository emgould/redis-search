"""
Query builders for the public List discovery index (``idx:public_lists``).

Search modes:
- free text: List name, description, topic summary, and item titles
- ``@username`` owner search: exact/prefix TAG match on ``owner_username``
- topic: TAG match on ``topic_keywords`` / ``topic_labels`` OR free text
- title containment: exact TAG match on ``item_mc_ids`` or item-title text
- sort modes: followers / engagement / freshness (system Lists excluded
  from popularity sorts because they have no follow paradigm)
"""

from core.iptc import normalize_tag
from core.normalize import compact_title
from core.search_queries import (
    escape_redis_search_term,
    normalize_query_separators,
    strip_query_apostrophes,
)

PUBLIC_LIST_SORT_FIELDS = {
    "relevance": None,
    "followers": "followers_count",
    "engagement": "engagement_score",
    "recent": "discovery_updated_at",
    "created": "created_at",
}

# Sorts that rank by community popularity. MediaCircle system-owned Lists
# are implicitly in every user's Lists view (no follow paradigm), so
# follower/engagement numbers are meaningless for them and they are
# excluded from these sort modes.
POPULARITY_SORTS = frozenset({"followers", "engagement"})


class PublicListQueryError(ValueError):
    """Raised when public-list search parameters fail validation."""


def resolve_sort(sort: str | None) -> tuple[str, str | None]:
    """Validate a sort mode name and return ``(mode, redis_sort_field)``."""
    mode = (sort or "relevance").strip().lower()
    if mode not in PUBLIC_LIST_SORT_FIELDS:
        raise PublicListQueryError(
            f"Invalid sort '{sort}'. Allowed: {', '.join(sorted(PUBLIC_LIST_SORT_FIELDS))}"
        )
    return mode, PUBLIC_LIST_SORT_FIELDS[mode]


def _text_clause(q: str) -> str | None:
    """Free-text clause across name, description, topics, and item titles."""
    cleaned = strip_query_apostrophes(q)
    words = [w.lower() for w in normalize_query_separators(cleaned).split() if w]
    if not words:
        return None

    escaped = [escape_redis_search_term(w) for w in words]
    if len(escaped) == 1 and len(escaped[0]) <= 3:
        term = escaped[0]
    elif len(escaped) == 1:
        term = f"{escaped[0]}*"
    else:
        term = f"{' '.join(escaped[:-1])} {escaped[-1]}*"

    text_fields = "search_title|description_text|topic_summary|item_titles_text"
    parts = [f"@{text_fields}:({term})"]

    normalized = normalize_tag(q)
    if normalized and len(normalized) >= 2:
        tag_pattern = f"{normalized}*" if len(normalized) > 3 else normalized
        parts.append(f"@topic_keywords:{{{tag_pattern}}}")
        parts.append(f"@topic_labels:{{{tag_pattern}}}")

    if len(parts) == 1:
        return parts[0]
    return " | ".join(f"({part})" for part in parts)


def build_public_list_query(
    q: str | None = None,
    owner_username: str | None = None,
    owner_id: str | None = None,
    topic: str | None = None,
    item_mc_id: str | None = None,
    item_title: str | None = None,
    exclude_system_owned: bool = False,
) -> str:
    """
    Build the RediSearch query string for public List discovery.

    All provided filters are AND-ed. With no filters, returns match-all
    (browse mode).
    """
    parts: list[str] = []

    if q and q.strip():
        stripped = q.strip()
        # "@username" query syntax routes to owner search
        if stripped.startswith("@") and len(stripped) > 1:
            owner_username = owner_username or stripped[1:]
        else:
            clause = _text_clause(stripped)
            if clause:
                parts.append(f"({clause})")

    if owner_username and owner_username.strip():
        normalized_owner = normalize_tag(owner_username)
        if not normalized_owner:
            raise PublicListQueryError("owner_username produced an empty search value")
        pattern = f"{normalized_owner}*" if len(normalized_owner) > 3 else normalized_owner
        parts.append(f"@owner_username:{{{pattern}}}")

    if owner_id and owner_id.strip():
        parts.append(f"@owner_id:{{{escape_redis_search_term(owner_id.strip())}}}")

    if topic and topic.strip():
        normalized_topic = normalize_tag(topic)
        if not normalized_topic:
            raise PublicListQueryError("topic produced an empty search value")
        topic_pattern = (
            f"{normalized_topic}*" if len(normalized_topic) > 3 else normalized_topic
        )
        topic_clauses = [
            f"@topic_keywords:{{{topic_pattern}}}",
            f"@topic_labels:{{{topic_pattern}}}",
        ]
        text_clause = _text_clause(topic.strip())
        if text_clause:
            topic_clauses.append(text_clause)
        parts.append("(" + " | ".join(f"({clause})" for clause in topic_clauses) + ")")

    if item_mc_id and item_mc_id.strip():
        parts.append(f"@item_mc_ids:{{{escape_redis_search_term(item_mc_id.strip())}}}")

    if item_title and item_title.strip():
        # Exact collapsed-token match first, OR free-text over item titles
        collapsed = compact_title(item_title)
        clauses = []
        if collapsed:
            clauses.append(
                f"@item_title_compacts:{{{escape_redis_search_term(collapsed)}}}"
            )
        cleaned = strip_query_apostrophes(item_title)
        words = [
            escape_redis_search_term(w.lower())
            for w in normalize_query_separators(cleaned).split()
            if w
        ]
        if words:
            clauses.append(f"@item_titles_text:({' '.join(words)})")
        if clauses:
            parts.append("(" + " | ".join(f"({clause})" for clause in clauses) + ")")

    if exclude_system_owned:
        parts.append("-@is_mediacircle_owner:{true}")

    if not parts:
        return "*"
    if all(part.startswith("-") for part in parts):
        parts.insert(0, "*")
    return " ".join(parts)
