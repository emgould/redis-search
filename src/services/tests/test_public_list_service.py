"""
Integration tests for the public List index (``idx:public_lists``).

Requires a local Redis Stack instance (default localhost:6380). Tests
skip automatically when Redis is unavailable.

Covers the full contract:
- index lifecycle (create if missing, schema fields present)
- CRUD (upsert create, upsert update, get, delete)
- search modes (name, description, @username, owner_id, topic,
  item mc_id containment, item title containment)
- sort modes (followers, engagement, recent) and system-list exclusion
- pagination (stable non-overlapping pages, total counts)

Seed data mirrors real MediaCircle public List shapes: two user-owned
Lists plus one MediaCircle system-owned List.
"""

import asyncio
import os
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from core.public_lists import (
    PUBLIC_LIST_INDEX,
    PublicListItem,
    PublicListUpsertRequest,
    public_list_key,
)
from services.public_list_service import (
    delete_public_list,
    ensure_public_list_index,
    get_public_list,
    search_public_lists,
    upsert_public_list,
)

pytestmark = pytest.mark.integration


def _redis() -> Redis:  # type: ignore[type-arg]
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )


async def _require_redis(redis: Redis) -> None:  # type: ignore[type-arg]
    try:
        await redis.ping()  # type: ignore[misc]
    except Exception as exc:
        pytest.skip(f"Redis is required for public list integration tests: {exc}")


class _Corpus:
    """Uniquely-tagged seed corpus so assertions never touch other docs."""

    def __init__(self) -> None:
        run_id = uuid4().hex[:12]
        self.run_id = run_id
        # Unique topic tag scoping every search/sort assertion to this run
        self.scope_topic = f"testscope_{run_id}"
        self.owner_a = f"test_user_a_{run_id}"
        self.owner_b = f"test_user_b_{run_id}"
        self.username_a = f"docfan_{run_id}"
        self.username_b = f"crimebuff_{run_id}"
        self.doc_list_id = f"test_docs_{run_id}"
        self.crime_list_id = f"test_crime_{run_id}"
        self.system_list_id = f"test_system_{run_id}"
        self.all_ids = [self.doc_list_id, self.crime_list_id, self.system_list_id]

    def requests(self) -> list[PublicListUpsertRequest]:
        return [
            # Seeded from data/public_lists/best_documentary.json shape
            PublicListUpsertRequest(
                list_id=self.doc_list_id,
                name=f"Top Movie Documentaries {self.run_id}",
                description="Top documentaries from around the world",
                circle_type="movies_and_tv",
                owner_id=self.owner_a,
                owner_username=self.username_a,
                owner_display_name="Doc Fan",
                topic_keywords=["documentary", "film", self.scope_topic],
                topic_labels=["documentary.nature"],
                topic_summary="The best documentary films from around the world",
                items=[
                    PublicListItem(mc_id="tmdb_14048", title="Man on Wire", mc_type="movie"),
                    PublicListItem(mc_id="tmdb_776527", title="The Rescue", mc_type="movie"),
                ],
                followers_count=25,
                engagement_score=40.0,
                vote_count=30,
                net_vote_score=22,
                discovery_updated_at=1700000200,
            ),
            PublicListUpsertRequest(
                list_id=self.crime_list_id,
                name=f"Mob Cinema Classics {self.run_id}",
                description="Essential organized crime films",
                circle_type="movies_and_tv",
                owner_id=self.owner_b,
                owner_username=self.username_b,
                owner_display_name="Crime Buff",
                topic_keywords=["crime", "mafia", self.scope_topic],
                topic_labels=["drama.crime"],
                topic_summary="Essential mafia and organized crime cinema",
                items=[
                    PublicListItem(mc_id="tmdb_238", title="The Godfather", mc_type="movie"),
                    PublicListItem(mc_id="tmdb_240", title="The Godfather Part II", mc_type="movie"),
                    PublicListItem(mc_id="tmdb_1396", title="Breaking Bad", mc_type="tv"),
                ],
                followers_count=90,
                engagement_score=75.0,
                vote_count=120,
                net_vote_score=95,
                discovery_updated_at=1700000300,
            ),
            # MediaCircle system-owned catalog List
            PublicListUpsertRequest(
                list_id=self.system_list_id,
                name=f"MediaCircle Staff Picks {self.run_id}",
                description="Curated by MediaCircle",
                circle_type="movies_and_tv",
                owner_id="mediacircle_system_001",
                owner_username="mediacircle",
                owner_display_name="MediaCircle",
                is_mediacircle_owner=True,
                topic_keywords=["staff_picks", "crime", self.scope_topic],
                topic_summary="MediaCircle curated crime picks",
                items=[
                    PublicListItem(mc_id="tmdb_238", title="The Godfather", mc_type="movie"),
                ],
                followers_count=0,
                engagement_score=999.0,
                vote_count=500,
                net_vote_score=400,
                discovery_updated_at=1700000400,
            ),
        ]


async def _seed(redis: Redis, corpus: _Corpus) -> None:  # type: ignore[type-arg]
    await ensure_public_list_index(redis)
    for request in corpus.requests():
        await upsert_public_list(request, redis=redis)


async def _cleanup(redis: Redis, corpus: _Corpus) -> None:  # type: ignore[type-arg]
    for list_id in corpus.all_ids:
        await redis.delete(public_list_key(list_id))


async def _run_with_corpus(test_body) -> None:
    redis = _redis()
    corpus = _Corpus()
    try:
        await _require_redis(redis)
        await _seed(redis, corpus)
        await test_body(redis, corpus)
    finally:
        try:
            await _cleanup(redis, corpus)
        finally:
            await redis.aclose()


def _ids(payload: dict) -> list[str]:
    return [doc["list_id"] for doc in payload["results"]]


# =============================================================================
# Index lifecycle
# =============================================================================


def test_index_lifecycle_and_schema() -> None:
    asyncio.run(_test_index_lifecycle_and_schema())


async def _test_index_lifecycle_and_schema() -> None:
    redis = _redis()
    try:
        await _require_redis(redis)
        await ensure_public_list_index(redis)
        # Second call is a no-op, not an error
        created_again = await ensure_public_list_index(redis)
        assert created_again is False

        info = await redis.ft(PUBLIC_LIST_INDEX).info()
        attributes = info.get("attributes", [])
        field_names = set()
        for attr in attributes:
            # attributes are flat lists: [..., "attribute", <name>, ...]
            for i, token in enumerate(attr):
                if token == "attribute":
                    field_names.add(attr[i + 1])
        expected = {
            "search_title",
            "owner_username",
            "owner_id",
            "is_mediacircle_owner",
            "topic_keywords",
            "item_mc_ids",
            "item_title_compacts",
            "followers_count",
            "engagement_score",
            "discovery_updated_at",
        }
        assert expected.issubset(field_names), f"missing: {expected - field_names}"
    finally:
        await redis.aclose()


# =============================================================================
# CRUD
# =============================================================================


def test_crud_upsert_get_update_delete() -> None:
    asyncio.run(_run_with_corpus(_test_crud_body))


async def _test_crud_body(redis: Redis, corpus: _Corpus) -> None:  # type: ignore[type-arg]
    # Read back a seeded document
    doc = await get_public_list(corpus.doc_list_id, redis=redis)
    assert doc is not None
    assert doc["name"] == f"Top Movie Documentaries {corpus.run_id}"
    assert doc["item_count"] == 2
    created_at = doc["created_at"]

    # Update: change metrics and items; created_at must be preserved
    requests = corpus.requests()
    update = requests[0].model_copy(
        update={
            "followers_count": 26,
            "items": requests[0].items
            + [PublicListItem(mc_id="tmdb_913823", title="Fire of Love", mc_type="movie")],
        }
    )
    await upsert_public_list(update, redis=redis)

    updated = await get_public_list(corpus.doc_list_id, redis=redis)
    assert updated is not None
    assert updated["followers_count"] == 26
    assert updated["item_count"] == 3
    assert "tmdb_913823" in updated["item_mc_ids"]
    assert updated["created_at"] == created_at

    # Delete
    removed = await delete_public_list(corpus.crime_list_id, redis=redis)
    assert removed is True
    assert await get_public_list(corpus.crime_list_id, redis=redis) is None

    # Deleting again reports nothing removed
    removed_again = await delete_public_list(corpus.crime_list_id, redis=redis)
    assert removed_again is False

    # Deleted document no longer appears in search
    payload = await search_public_lists(topic=corpus.scope_topic, redis=redis)
    assert corpus.crime_list_id not in _ids(payload)


# =============================================================================
# Search modes
# =============================================================================


def test_search_by_list_name() -> None:
    asyncio.run(_run_with_corpus(_test_search_by_list_name_body))


async def _test_search_by_list_name_body(redis: Redis, corpus: _Corpus) -> None:  # type: ignore[type-arg]
    payload = await search_public_lists(q=f"Mob Cinema {corpus.run_id}", redis=redis)
    assert corpus.crime_list_id in _ids(payload)


def test_search_by_description_text() -> None:
    asyncio.run(_run_with_corpus(_test_search_by_description_body))


async def _test_search_by_description_body(redis: Redis, corpus: _Corpus) -> None:  # type: ignore[type-arg]
    payload = await search_public_lists(q="organized crime films", redis=redis)
    assert corpus.crime_list_id in _ids(payload)


def test_search_by_at_username() -> None:
    asyncio.run(_run_with_corpus(_test_search_by_at_username_body))


async def _test_search_by_at_username_body(redis: Redis, corpus: _Corpus) -> None:  # type: ignore[type-arg]
    # Full "@username" query string routes to owner search
    payload = await search_public_lists(q=f"@{corpus.username_a}", redis=redis)
    assert _ids(payload) == [corpus.doc_list_id]

    # Prefix matching: "@docfan_<partial>" still resolves
    prefix = corpus.username_a[:-4]
    payload = await search_public_lists(q=f"@{prefix}", redis=redis)
    assert corpus.doc_list_id in _ids(payload)

    # Explicit owner_username param behaves identically
    payload = await search_public_lists(owner_username=corpus.username_b, redis=redis)
    assert _ids(payload) == [corpus.crime_list_id]


def test_search_by_owner_id() -> None:
    asyncio.run(_run_with_corpus(_test_search_by_owner_id_body))


async def _test_search_by_owner_id_body(redis: Redis, corpus: _Corpus) -> None:  # type: ignore[type-arg]
    payload = await search_public_lists(owner_id=corpus.owner_b, redis=redis)
    assert _ids(payload) == [corpus.crime_list_id]


def test_search_by_topic() -> None:
    asyncio.run(_run_with_corpus(_test_search_by_topic_body))


async def _test_search_by_topic_body(redis: Redis, corpus: _Corpus) -> None:  # type: ignore[type-arg]
    # Both the user crime list and the system crime list carry the crime keyword
    payload = await search_public_lists(
        topic="crime", owner_id=corpus.owner_b, redis=redis
    )
    assert corpus.crime_list_id in _ids(payload)

    # Scope topic returns exactly the seeded corpus
    payload = await search_public_lists(topic=corpus.scope_topic, redis=redis)
    assert set(_ids(payload)) == set(corpus.all_ids)


def test_search_by_contained_item_mc_id() -> None:
    asyncio.run(_run_with_corpus(_test_search_by_item_mc_id_body))


async def _test_search_by_item_mc_id_body(redis: Redis, corpus: _Corpus) -> None:  # type: ignore[type-arg]
    # "Find me all lists that have the movie The Godfather" (resolved mc_id)
    payload = await search_public_lists(
        item_mc_id="tmdb_238", topic=corpus.scope_topic, redis=redis
    )
    assert set(_ids(payload)) == {corpus.crime_list_id, corpus.system_list_id}


def test_search_by_contained_item_title() -> None:
    asyncio.run(_run_with_corpus(_test_search_by_item_title_body))


async def _test_search_by_item_title_body(redis: Redis, corpus: _Corpus) -> None:  # type: ignore[type-arg]
    # Unresolved title fallback path
    payload = await search_public_lists(
        item_title="The Godfather", topic=corpus.scope_topic, redis=redis
    )
    assert corpus.crime_list_id in _ids(payload)
    assert corpus.system_list_id in _ids(payload)
    assert corpus.doc_list_id not in _ids(payload)


# =============================================================================
# Sort modes and system-list exclusion
# =============================================================================


def test_sort_by_followers_excludes_system_lists() -> None:
    asyncio.run(_run_with_corpus(_test_sort_followers_body))


async def _test_sort_followers_body(redis: Redis, corpus: _Corpus) -> None:  # type: ignore[type-arg]
    payload = await search_public_lists(
        topic=corpus.scope_topic, sort="followers", redis=redis
    )
    # System list excluded; user lists ordered by followers desc (90 > 25)
    assert _ids(payload) == [corpus.crime_list_id, corpus.doc_list_id]


def test_sort_by_engagement_excludes_system_lists() -> None:
    asyncio.run(_run_with_corpus(_test_sort_engagement_body))


async def _test_sort_engagement_body(redis: Redis, corpus: _Corpus) -> None:  # type: ignore[type-arg]
    payload = await search_public_lists(
        topic=corpus.scope_topic, sort="engagement", redis=redis
    )
    # System list has the highest engagement score but is excluded
    assert _ids(payload) == [corpus.crime_list_id, corpus.doc_list_id]


def test_browse_mode_popularity_sort_without_filters() -> None:
    asyncio.run(_run_with_corpus(_test_browse_popularity_sort_body))


async def _test_browse_popularity_sort_body(redis: Redis, corpus: _Corpus) -> None:  # type: ignore[type-arg]
    # Regression: followers/engagement sort with NO other filters must build
    # a valid standalone query (`* -@tag:{...}` is rejected by Redis 7.4).
    payload = await search_public_lists(sort="followers", limit=50, redis=redis)
    ids = _ids(payload)
    assert corpus.system_list_id not in ids
    assert corpus.crime_list_id in ids
    assert corpus.doc_list_id in ids
    # Seeded user lists must be ordered by followers desc relative to each other
    assert ids.index(corpus.crime_list_id) < ids.index(corpus.doc_list_id)


def test_sort_by_recent_includes_system_lists() -> None:
    asyncio.run(_run_with_corpus(_test_sort_recent_body))


async def _test_sort_recent_body(redis: Redis, corpus: _Corpus) -> None:  # type: ignore[type-arg]
    payload = await search_public_lists(
        topic=corpus.scope_topic, sort="recent", redis=redis
    )
    # Freshness sort keeps system lists and orders by discovery_updated_at desc
    assert _ids(payload) == [
        corpus.system_list_id,
        corpus.crime_list_id,
        corpus.doc_list_id,
    ]


def test_system_lists_included_in_text_search() -> None:
    asyncio.run(_run_with_corpus(_test_system_in_text_search_body))


async def _test_system_in_text_search_body(redis: Redis, corpus: _Corpus) -> None:  # type: ignore[type-arg]
    payload = await search_public_lists(
        q=f"Staff Picks {corpus.run_id}", redis=redis
    )
    assert corpus.system_list_id in _ids(payload)


# =============================================================================
# Pagination
# =============================================================================


def test_pagination_stable_non_overlapping_pages() -> None:
    asyncio.run(_run_with_corpus(_test_pagination_body))


async def _test_pagination_body(redis: Redis, corpus: _Corpus) -> None:  # type: ignore[type-arg]
    page1 = await search_public_lists(
        topic=corpus.scope_topic, sort="recent", limit=2, offset=0, redis=redis
    )
    page2 = await search_public_lists(
        topic=corpus.scope_topic, sort="recent", limit=2, offset=2, redis=redis
    )

    assert page1["total"] == 3
    assert page2["total"] == 3
    assert len(page1["results"]) == 2
    assert len(page2["results"]) == 1
    assert not set(_ids(page1)) & set(_ids(page2))
    assert set(_ids(page1)) | set(_ids(page2)) == set(corpus.all_ids)
