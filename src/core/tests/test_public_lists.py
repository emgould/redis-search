"""
Unit tests for the public List document contract and query builders.

These tests are hermetic (no Redis required). Full search/CRUD coverage
against a live Redis index lives in
``src/services/tests/test_public_list_service.py``.
"""

import pytest

from core.public_list_queries import (
    POPULARITY_SORTS,
    PublicListQueryError,
    build_public_list_query,
    resolve_sort,
)
from core.public_lists import (
    PUBLIC_LIST_INDEX,
    PUBLIC_LIST_PREFIX,
    RUNTIME_OWNED_INDEXES,
    RUNTIME_OWNED_PREFIXES,
    PublicListItem,
    PublicListUpsertRequest,
    public_list_index_schema,
    public_list_key,
    public_list_to_redis_doc,
)

pytestmark = pytest.mark.unit


def _request(**overrides) -> PublicListUpsertRequest:
    base = {
        "list_id": "public_best_documentary",
        "name": "Top Movie Documentaries",
        "description": "Top documentaries from around the world",
        "owner_id": "user_abc123",
        "owner_username": "DocFan99",
        "owner_display_name": "Doc Fan",
        "topic_keywords": ["Documentary", "True Crime"],
        "topic_labels": ["documentary.nature"],
        "items": [
            PublicListItem(mc_id="tmdb_14048", title="Man on Wire", mc_type="movie"),
            PublicListItem(mc_id="tmdb_1396", title="Breaking Bad", mc_type="tv"),
        ],
        "followers_count": 12,
        "engagement_score": 34.5,
        "vote_count": 20,
        "net_vote_score": 15,
    }
    base.update(overrides)
    return PublicListUpsertRequest(**base)


class TestPublicListDocument:
    def test_key_format(self):
        assert public_list_key("abc") == "public_list:abc"
        assert public_list_key("abc").startswith(PUBLIC_LIST_PREFIX)

    def test_doc_derives_search_fields(self):
        doc = public_list_to_redis_doc(_request())

        assert doc["list_id"] == "public_best_documentary"
        assert doc["search_title"] == "Top Movie Documentaries"
        assert doc["title_compact"] == "topmoviedocumentaries"
        assert doc["owner_username"] == "docfan99"
        assert doc["owner_username_display"] == "DocFan99"
        assert doc["topic_keywords"] == ["documentary", "true_crime"]
        assert doc["topic_labels"] == ["documentary_nature"]
        assert doc["item_mc_ids"] == ["tmdb_14048", "tmdb_1396"]
        assert doc["item_titles"] == ["Man on Wire", "Breaking Bad"]
        assert doc["item_title_compacts"] == ["manonwire", "breakingbad"]
        assert doc["item_types"] == ["movie", "tv"]
        assert doc["item_count"] == 2
        assert doc["is_mediacircle_owner"] == "false"
        assert doc["followers_count"] == 12
        assert doc["engagement_score"] == 34.5
        assert doc["_source"] == "mediacircle_projection"

    def test_apostrophes_stripped_from_search_text(self):
        doc = public_list_to_redis_doc(
            _request(
                name="It's a Wonderful List",
                items=[PublicListItem(mc_id="tmdb_1585", title="It's a Wonderful Life")],
            )
        )
        assert doc["search_title"] == "Its a Wonderful List"
        assert "Its a Wonderful Life" in doc["item_titles_text"]
        assert doc["item_title_compacts"] == ["itsawonderfullife"]

    def test_system_owner_flag(self):
        doc = public_list_to_redis_doc(_request(is_mediacircle_owner=True))
        assert doc["is_mediacircle_owner"] == "true"

    def test_created_at_preserved_from_existing(self):
        existing = {"created_at": 1700000000}
        doc = public_list_to_redis_doc(_request(), existing=existing)
        assert doc["created_at"] == 1700000000

    def test_created_at_explicit_wins(self):
        existing = {"created_at": 1700000000}
        doc = public_list_to_redis_doc(_request(created_at=1650000000), existing=existing)
        assert doc["created_at"] == 1650000000

    def test_schema_covers_search_sort_fields(self):
        field_names = {f.as_name for f in public_list_index_schema()}
        expected = {
            "search_title",
            "description_text",
            "topic_summary",
            "item_titles_text",
            "owner_id",
            "owner_username",
            "is_mediacircle_owner",
            "topic_keywords",
            "topic_labels",
            "item_mc_ids",
            "item_title_compacts",
            "followers_count",
            "engagement_score",
            "net_vote_score",
            "discovery_updated_at",
            "created_at",
        }
        assert expected.issubset(field_names)

    def test_runtime_owned_registry(self):
        assert PUBLIC_LIST_INDEX in RUNTIME_OWNED_INDEXES
        assert PUBLIC_LIST_PREFIX in RUNTIME_OWNED_PREFIXES


class TestResolveSort:
    def test_valid_sorts(self):
        assert resolve_sort(None) == ("relevance", None)
        assert resolve_sort("relevance") == ("relevance", None)
        assert resolve_sort("followers") == ("followers", "followers_count")
        assert resolve_sort("engagement") == ("engagement", "engagement_score")
        assert resolve_sort("recent") == ("recent", "discovery_updated_at")
        assert resolve_sort("created") == ("created", "created_at")

    def test_invalid_sort_raises(self):
        with pytest.raises(PublicListQueryError):
            resolve_sort("popularity")

    def test_popularity_sorts_set(self):
        assert POPULARITY_SORTS == {"followers", "engagement"}


class TestBuildPublicListQuery:
    def test_browse_mode_matches_all(self):
        assert build_public_list_query() == "*"

    def test_free_text_covers_text_fields_and_topics(self):
        query = build_public_list_query(q="documentary")
        assert "search_title|description_text|topic_summary|item_titles_text" in query
        assert "@topic_keywords:{documentary*}" in query
        assert "@topic_labels:{documentary*}" in query

    def test_at_username_routes_to_owner_search(self):
        query = build_public_list_query(q="@DocFan99")
        assert "@owner_username:{docfan99*}" in query
        assert "search_title" not in query

    def test_owner_username_param(self):
        query = build_public_list_query(owner_username="DocFan99")
        assert query == "@owner_username:{docfan99*}"

    def test_short_username_uses_exact_match(self):
        query = build_public_list_query(owner_username="Bob")
        assert query == "@owner_username:{bob}"

    def test_owner_id_exact_filter(self):
        query = build_public_list_query(owner_id="user_abc123")
        assert query == "@owner_id:{user_abc123}"

    def test_topic_filter_unions_tags_and_text(self):
        query = build_public_list_query(topic="true crime")
        assert "@topic_keywords:{true_crime*}" in query
        assert "@topic_labels:{true_crime*}" in query

    def test_item_mc_id_containment(self):
        query = build_public_list_query(item_mc_id="tmdb_238")
        assert query == "@item_mc_ids:{tmdb_238}"

    def test_item_title_containment(self):
        query = build_public_list_query(item_title="The Godfather")
        assert "@item_title_compacts:{thegodfather}" in query
        assert "@item_titles_text:(the godfather)" in query

    def test_exclude_system_owned(self):
        query = build_public_list_query(q="documentary", exclude_system_owned=True)
        assert "-@is_mediacircle_owner:{true}" in query

    def test_exclude_system_owned_alone_prepends_match_all(self):
        query = build_public_list_query(exclude_system_owned=True)
        assert query == "* -@is_mediacircle_owner:{true}"

    def test_combined_filters_are_anded(self):
        query = build_public_list_query(q="crime", owner_id="user_1")
        assert "@owner_id:{user_1}" in query
        assert "search_title" in query

    def test_empty_owner_username_raises(self):
        with pytest.raises(PublicListQueryError):
            build_public_list_query(owner_username="!!!")
