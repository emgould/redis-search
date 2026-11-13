"""Unit tests for Media Manager zero-result search fallback."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from services.media_manager_search_fallback import (
    SemanticFallbackQuerySpec,
    apply_media_manager_semantic_fallback,
    extra_queries_for_source_hint,
    map_media_manager_hit_to_redis_doc,
    semantic_fallback_eligible,
)


def test_semantic_fallback_eligible_requires_empty_bucket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MEDIA_MANAGER_API_URL", "https://mm.example")
    assert semantic_fallback_eligible(
        query_text="holiday heist",
        skip=0,
        raw=False,
        has_filters=False,
        media_sort="popularity",
        mc_type=None,
        requested_sources={"movie", "tv"},
        full_results={"movie": [], "tv": [{"mc_id": "x"}]},
    )
    assert not semantic_fallback_eligible(
        query_text="holiday heist",
        skip=0,
        raw=False,
        has_filters=False,
        media_sort="popularity",
        mc_type=None,
        requested_sources={"movie"},
        full_results={"movie": [{"mc_id": "existing"}]},
    )


def test_semantic_fallback_ineligible_on_skip_or_filters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MEDIA_MANAGER_API_URL", "https://mm.example")
    base = dict(
        query_text="foo bar",
        media_sort="popularity",
        mc_type=None,
        requested_sources={"movie"},
        full_results={"movie": []},
    )
    assert not semantic_fallback_eligible(
        skip=10, has_filters=False, raw=False, **base
    )
    assert not semantic_fallback_eligible(
        skip=0, has_filters=True, raw=False, **base
    )
    assert not semantic_fallback_eligible(skip=0, has_filters=False, raw=True, **base)


def test_map_falls_back_to_normalized_metadata() -> None:
    row = {
        "title": "The Thing",
        "normalized_metadata": {"mc_id": "tmdb_movie_1", "title": "The Thing"},
    }
    doc = map_media_manager_hit_to_redis_doc(row, "movie")
    assert doc is not None
    assert doc["mc_id"] == "tmdb_movie_1"
    assert doc.get("_semantic_fallback") is True


def test_map_uses_index_metadata_not_normalized() -> None:
    row = {
        "metadata": {
            "index_metadata": {
                "mc_id": "tmdb_movie_1",
                "title": "Redis Title",
                "image": "https://image.tmdb.org/t/p/w185/x.jpg",
            }
        },
        "normalized_metadata": {"mc_id": "tmdb_movie_1", "title": "MC Model Title"},
    }
    doc = map_media_manager_hit_to_redis_doc(row, "movie")
    assert doc is not None
    assert doc["title"] == "Redis Title"
    assert doc["search_title"] == "Redis Title"


def test_map_sets_image_from_poster_path() -> None:
    row = {
        "title": "A Christmas Story",
        "metadata": {
            "index_metadata": {
                "mc_id": "tmdb_movie_998",
                "title": "A Christmas Story",
                "poster_path": "/7fEpQkQibcwoCk83gpP6Uj7QeEv.jpg",
            }
        },
    }
    doc = map_media_manager_hit_to_redis_doc(row, "movie")
    assert doc is not None
    assert doc["image"] == (
        "https://image.tmdb.org/t/p/w185/7fEpQkQibcwoCk83gpP6Uj7QeEv.jpg"
    )


def test_map_prefers_poster_image_url() -> None:
    row = {
        "poster_image_url": "https://cdn.example/poster.jpg",
        "metadata": {
            "index_metadata": {"mc_id": "tmdb_tv_1", "title": "Show"},
        },
    }
    doc = map_media_manager_hit_to_redis_doc(row, "tv")
    assert doc is not None
    assert doc["image"] == "https://cdn.example/poster.jpg"


def test_map_media_manager_hit_coerces_keyword_dicts() -> None:
    row = {
        "metadata": {
            "index_metadata": {
                "mc_id": "tmdb_movie_2",
                "title": "Holiday",
                "keywords": [{"name": "christmas"}, {"id": 99, "name": "romance"}],
                "genres": [{"name": "Romance"}],
            }
        },
    }
    doc = map_media_manager_hit_to_redis_doc(row, "movie")
    assert doc is not None
    assert doc["keywords"] == ["christmas", "romance"]
    assert doc["genres"] == ["Romance"]


def test_map_media_manager_hit_index_metadata() -> None:
    row = {
        "metadata": {"index_metadata": {"mc_id": "tmdb_tv_9", "search_title": "Severance"}},
    }
    doc = map_media_manager_hit_to_redis_doc(row, "tv")
    assert doc is not None
    assert doc["mc_type"] == "tv"


def test_extra_queries_for_source_hint() -> None:
    assert extra_queries_for_source_hint(stripped_q="Holiday", primary="Holiday Movies") == (
        "Holiday",
    )
    assert extra_queries_for_source_hint(stripped_q="Holiday Movies", primary="Holiday Movies") == ()


@pytest.mark.asyncio
async def test_apply_fallback_timeout_leaves_bucket_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MEDIA_MANAGER_API_URL", "https://mm.example")

    async def _fail(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise TimeoutError("boom")

    monkeypatch.setattr(
        "services.media_manager_search_fallback.MediaManagerClient.search_index",
        _fail,
    )
    full: dict[str, list[dict[str, Any]]] = {"movie": []}
    meta = await apply_media_manager_semantic_fallback(
        full_results=full,
        query_spec=SemanticFallbackQuerySpec(primary="obscure query"),
        requested_sources={"movie"},
        limit=5,
        skip=0,
        raw=False,
        has_filters=False,
        media_sort="popularity",
        mc_type=None,
    )
    assert meta is not None
    assert full["movie"] == []
    assert meta["movie"]["applied"] is False


@pytest.mark.asyncio
async def test_apply_fallback_fills_movie_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MEDIA_MANAGER_API_URL", "https://mm.example")

    async def _search_index(
        self: object,
        *,
        query: str,
        index_name: str,
        limit: int = 10,
        re_rank: bool = False,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        assert re_rank is True
        return {
            "results": [
                {
                    "metadata": {
                        "index_metadata": {
                            "mc_id": "tmdb_movie_42",
                            "title": "Example",
                            "image": "https://example/poster.jpg",
                        }
                    }
                }
            ],
            "re_rank_applied": True,
        }

    monkeypatch.setattr(
        "services.media_manager_search_fallback.MediaManagerClient.search_index",
        _search_index,
    )
    close_mock = AsyncMock()
    monkeypatch.setattr(
        "services.media_manager_search_fallback.MediaManagerClient.close",
        close_mock,
    )
    full: dict[str, list[dict[str, Any]]] = {"movie": []}
    meta = await apply_media_manager_semantic_fallback(
        full_results=full,
        query_spec=SemanticFallbackQuerySpec(primary="semantic only title"),
        requested_sources={"movie"},
        limit=5,
        skip=0,
        raw=False,
        has_filters=False,
        media_sort="popularity",
        mc_type=None,
    )
    assert meta is not None
    assert len(full["movie"]) == 1
    assert meta["movie"]["applied"] is True
    close_mock.assert_awaited_once()
