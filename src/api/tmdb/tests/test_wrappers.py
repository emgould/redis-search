"""Unit tests for TMDB wrapper helpers."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from api.tmdb.core import TMDBContentRatingCache
from api.tmdb.wrappers import get_content_rating_async

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_get_content_rating_async_returns_rating():
    """Test the wrapper returns the service rating."""
    with (
        patch("api.tmdb.wrappers.TMDBService") as service_class,
        patch.object(TMDBContentRatingCache, "read", return_value=None),
        patch.object(TMDBContentRatingCache._redis, "set") as mock_set,
    ):
        service_instance = service_class.return_value
        service_instance.get_content_rating = AsyncMock(
            return_value={"rating": "TV-MA", "release_date": None}
        )

        result = await get_content_rating_async(1396, "US", "tv")

    assert result == {"rating": "TV-MA", "release_date": None}
    mock_set.assert_called_once()


@pytest.mark.asyncio
async def test_get_content_rating_async_handles_exception():
    """Test the wrapper returns None on service exceptions."""
    with (
        patch("api.tmdb.wrappers.TMDBService") as service_class,
        patch.object(TMDBContentRatingCache, "read", return_value=None),
        patch.object(TMDBContentRatingCache._redis, "set") as mock_set,
    ):
        service_instance = service_class.return_value
        service_instance.get_content_rating = AsyncMock(side_effect=RuntimeError("boom"))

        result = await get_content_rating_async(1396, "US", "tv")

    assert result is None
    mock_set.assert_not_called()


@pytest.mark.asyncio
async def test_get_content_rating_async_returns_movie_rating_and_release_date():
    """Test the wrapper returns movie certification and release date."""
    with (
        patch("api.tmdb.wrappers.TMDBService") as service_class,
        patch.object(TMDBContentRatingCache, "read", return_value=None),
        patch.object(TMDBContentRatingCache._redis, "set") as mock_set,
    ):
        service_instance = service_class.return_value
        service_instance.get_content_rating = AsyncMock(
            return_value={"rating": "R", "release_date": "1999-11-13T00:00:00.000Z"}
        )

        result = await get_content_rating_async(238, "US", "movie")

    assert result == {"rating": "R", "release_date": "1999-11-13T00:00:00.000Z"}
    mock_set.assert_called_once()
