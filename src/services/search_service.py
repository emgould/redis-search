import json

from src.adapters.redis_repository import RedisRepository
from src.core.search_queries import (
    build_autocomplete_query,
    build_fuzzy_fulltext_query,
)

# Lazy initialization
_repo = None


def get_repo():
    global _repo
    if _repo is None:
        _repo = RedisRepository()
    return _repo


def parse_doc(doc):
    """Parse Redis Search document, handling JSON documents."""
    result = {"id": doc.id}

    # For JSON documents, parse the 'json' attribute
    if hasattr(doc, "json") and doc.json:
        try:
            parsed = json.loads(doc.json)
            result.update(parsed)
        except (json.JSONDecodeError, TypeError):
            pass

    # Also include any direct attributes
    for key, value in doc.__dict__.items():
        if key not in ("id", "payload", "json") and value is not None:
            result[key] = value

    return result


async def autocomplete(q: str):
    query = build_autocomplete_query(q)
    res = await get_repo().search(query)
    return [parse_doc(doc) for doc in res.docs]


async def full_search(q: str):
    query = build_fuzzy_fulltext_query(q)
    res = await get_repo().search(query)
    return [parse_doc(doc) for doc in res.docs]
