"""
Unit tests for FlixPatrol Wrappers.
Tests async wrapper functions for Firebase Functions compatibility.
"""

import json
from unittest.mock import AsyncMock, patch

import pytest

from api.subapi.flixpatrol.models import (
    FlixPatrolMediaItem,
    FlixPatrolMetadata,
    FlixPatrolResponse,
)
from api.subapi.flixpatrol.wrappers import flixpatrol_wrapper
from utils.pytest_utils import write_snapshot

pytestmark = pytest.mark.unit


class TestGetFlixPatrolDataAsync:
    """Tests for flixpatrol_wrapper.get_flixpatrol_data wrapper."""

    @pytest.mark.asyncio
    async def test_get_flixpatrol_data_async_success(self):
        """Test successful async wrapper call."""
        mock_response = FlixPatrolResponse(
            date="2025-01-15",
            shows={
                "netflix": [
                    FlixPatrolMediaItem(
                        id="netflix:Test Show:tv",
                        rank=1,
                        title="Test Show",
                        score=1000,
                        platform="netflix",
                        content_type="tv",
                    )
                ]
            },
            movies={},
            top_trending_tv_shows=[
                FlixPatrolMediaItem(
                    id="netflix:Test Show:tv",
                    rank=1,
                    title="Test Show",
                    score=1000,
                    platform="netflix",
                    content_type="tv",
                )
            ],
            top_trending_movies=[],
            metadata=FlixPatrolMetadata(
                source="FlixPatrol",
                total_shows=1,
                total_movies=0,
                platforms=["netflix"],
            ),
        )

        with patch.object(
            flixpatrol_wrapper.service, "get_flixpatrol_data", new_callable=AsyncMock
        ) as mock_get:
            mock_get.return_value = mock_response

            result = await flixpatrol_wrapper.get_flixpatrol_data()

            assert isinstance(result, FlixPatrolResponse)
            assert result.status_code == 200
            assert result.error is None
            assert len(result.top_trending_tv_shows) == 1
            write_snapshot(
                json.dumps(result.model_dump(), indent=4),
                "get_flixpatrol_data_async_success.json",
            )

    @pytest.mark.asyncio
    async def test_get_flixpatrol_data_with_providers(self):
        """Test wrapper with custom providers."""
        mock_response = FlixPatrolResponse(
            date="2025-01-15",
            shows={
                "netflix": [
                    FlixPatrolMediaItem(
                        id="netflix:Test Show:tv",
                        rank=1,
                        title="Test Show",
                        score=1000,
                        platform="netflix",
                        content_type="tv",
                    )
                ]
            },
            movies={},
            top_trending_tv_shows=[],
            top_trending_movies=[],
            metadata=FlixPatrolMetadata(
                source="FlixPatrol",
                total_shows=1,
                total_movies=0,
                platforms=["netflix"],
            ),
        )

        with patch.object(
            flixpatrol_wrapper.service, "get_flixpatrol_data", new_callable=AsyncMock
        ) as mock_get:
            mock_get.return_value = mock_response

            result = await flixpatrol_wrapper.get_flixpatrol_data(providers=["netflix"])

            assert isinstance(result, FlixPatrolResponse)
            assert result.status_code == 200
            mock_get.assert_called_once_with(providers=["netflix"])
            assert result.date == "2025-01-15"

            # Verify MCBaseItem fields are present
            assert len(result.shows["netflix"]) > 0
            show_item = result.shows["netflix"][0]
            assert show_item.mc_id is not None
            assert show_item.mc_type is not None
            assert show_item.source is not None

    @pytest.mark.asyncio
    async def test_get_flixpatrol_data_async_failure(self):
        """Test async wrapper with service failure."""
        with patch.object(
            flixpatrol_wrapper.service, "get_flixpatrol_data", new_callable=AsyncMock
        ) as mock_get:
            mock_get.return_value = None

            result = await flixpatrol_wrapper.get_flixpatrol_data()

            assert isinstance(result, FlixPatrolResponse)
            assert result.status_code == 500
            assert result.error is not None
            assert "Failed to fetch FlixPatrol data" in result.error

    @pytest.mark.asyncio
    async def test_get_flixpatrol_data_async_exception(self):
        """Test async wrapper with exception."""
        with patch.object(
            flixpatrol_wrapper.service, "get_flixpatrol_data", new_callable=AsyncMock
        ) as mock_get:
            mock_get.side_effect = Exception("Test error")

            result = await flixpatrol_wrapper.get_flixpatrol_data()

            assert isinstance(result, FlixPatrolResponse)
            assert result.status_code == 500
            assert result.error is not None
            assert "Test error" in result.error
