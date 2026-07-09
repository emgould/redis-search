"""
Public List discovery service.

Owns index lifecycle, CRUD, and search execution for
``idx:public_lists``. Documents are runtime-owned by the MediaCircle
projection pipeline; this service never fabricates content fields.
"""

import json
from typing import Any, cast

from redis.asyncio import Redis
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query

from adapters.redis_client import get_redis
from core.public_list_queries import (
    POPULARITY_SORTS,
    build_public_list_query,
    resolve_sort,
)
from core.public_lists import (
    PUBLIC_LIST_INDEX,
    PUBLIC_LIST_PREFIX,
    PublicListUpsertRequest,
    public_list_index_schema,
    public_list_key,
    public_list_to_redis_doc,
)
from utils.get_logger import get_logger

logger = get_logger(__name__)

MAX_PAGE_SIZE = 50
DEFAULT_PAGE_SIZE = 20


async def ensure_public_list_index(redis: Redis | None = None) -> bool:
    """Create ``idx:public_lists`` if missing. Returns True when created."""
    client = redis if redis is not None else get_redis()
    definition = IndexDefinition(prefix=[PUBLIC_LIST_PREFIX], index_type=IndexType.JSON)
    try:
        await client.ft(PUBLIC_LIST_INDEX).create_index(
            list(public_list_index_schema()),
            definition=definition,
            stopwords=[],
        )
        logger.info(f"Created index {PUBLIC_LIST_INDEX}")
        return True
    except Exception as exc:  # noqa: BLE001 - redis raises generic ResponseError
        if "Index already exists" in str(exc):
            return False
        raise


async def get_public_list(
    list_id: str, redis: Redis | None = None
) -> dict[str, object] | None:
    """Fetch one public List document by List id."""
    client = redis if redis is not None else get_redis()
    raw = await client.json().get(public_list_key(list_id))  # type: ignore[misc]
    if raw is None:
        return None
    if isinstance(raw, list):
        first = raw[0] if raw else None
        return cast(dict[str, object] | None, first)
    return cast(dict[str, object], raw)


async def upsert_public_list(
    request: PublicListUpsertRequest, redis: Redis | None = None
) -> dict[str, object]:
    """
    Upsert one public List document.

    Preserves ``created_at`` from any existing document unless the
    request provides one explicitly.
    """
    client = redis if redis is not None else get_redis()
    existing = await get_public_list(request.list_id, redis=client)
    doc = cast(dict[str, object], public_list_to_redis_doc(request, existing=existing))
    await client.execute_command(
        "JSON.SET", public_list_key(request.list_id), "$", json.dumps(doc)
    )
    logger.info(
        f"Upserted public list {request.list_id} "
        f"(items={doc['item_count']}, followers={doc['followers_count']})"
    )
    return doc


async def delete_public_list(list_id: str, redis: Redis | None = None) -> bool:
    """Delete one public List document. Returns True when a key was removed."""
    client = redis if redis is not None else get_redis()
    deleted = await client.delete(public_list_key(list_id))
    if deleted:
        logger.info(f"Deleted public list {list_id}")
    return bool(deleted)


def _parse_search_doc(doc: Any) -> dict | None:
    """Parse one FT.SEARCH result document into a plain dict."""
    raw_json = getattr(doc, "json", None)
    if raw_json:
        try:
            parsed = json.loads(raw_json)
        except (TypeError, ValueError):
            return None
        if isinstance(parsed, list):
            parsed = parsed[0] if parsed else None
        return parsed if isinstance(parsed, dict) else None
    return None


async def search_public_lists(
    q: str | None = None,
    owner_username: str | None = None,
    owner_id: str | None = None,
    topic: str | None = None,
    item_mc_id: str | None = None,
    item_title: str | None = None,
    sort: str | None = None,
    limit: int = DEFAULT_PAGE_SIZE,
    offset: int = 0,
    include_system_owned_in_popularity: bool = False,
    redis: Redis | None = None,
) -> dict:
    """
    Search the public List index.

    Returns ``{"results", "total", "offset", "limit", "sort"}`` where
    ``total`` is the full match count for pagination.
    """
    client = redis if redis is not None else get_redis()
    sort_mode, sort_field = resolve_sort(sort)
    limit = max(1, min(int(limit), MAX_PAGE_SIZE))
    offset = max(0, int(offset))

    exclude_system = (
        sort_mode in POPULARITY_SORTS and not include_system_owned_in_popularity
    )
    query_str = build_public_list_query(
        q=q,
        owner_username=owner_username,
        owner_id=owner_id,
        topic=topic,
        item_mc_id=item_mc_id,
        item_title=item_title,
        exclude_system_owned=exclude_system,
    )

    query = Query(query_str).paging(offset, limit)
    if sort_field:
        query = query.sort_by(sort_field, asc=False)
    elif not any([q, owner_username, owner_id, topic, item_mc_id, item_title]):
        # Browse mode without text relevance: stable freshness ordering
        query = query.sort_by("discovery_updated_at", asc=False)

    result = await client.ft(PUBLIC_LIST_INDEX).search(query)

    results = []
    for doc in result.docs:
        parsed = _parse_search_doc(doc)
        if parsed is not None:
            results.append(parsed)

    return {
        "results": results,
        "total": int(result.total),
        "offset": offset,
        "limit": limit,
        "sort": sort_mode,
        "query": query_str,
    }
