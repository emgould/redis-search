"""
Unit tests for Comscore Wrappers.
Tests async wrapper functions for Firebase Functions compatibility.
"""

import json
from unittest.mock import AsyncMock, patch

import pytest

from api.subapi.comscore.models import BoxOfficeData
from api.subapi.comscore.tests.conftest import load_fixture
from api.subapi.comscore.wrappers import comscore_wrapper
from utils.pytest_utils import write_snapshot

pytestmark = pytest.mark.unit


class TestGetDomesticRankingsAsync:
    """Tests for comscore_wrapper.get_domestic_rankings wrapper."""

    @pytest.mark.asyncio
    async def test_get_domestic_rankings_async_success(self):
        """Test successful async wrapper call."""
        mock_box_office_data = load_fixture("models/box_office_data.json")
        expected_data = BoxOfficeData.model_validate(mock_box_office_data)

        with patch.object(
            comscore_wrapper.service, "get_domestic_rankings", new_callable=AsyncMock
        ) as mock_get:
            mock_get.return_value = expected_data

            result = await comscore_wrapper.get_domestic_rankings()

            assert isinstance(result, BoxOfficeData)
            assert result.status_code == 200
            assert result.error is None
            assert len(result.rankings) == 3
            write_snapshot(
                json.dumps(result.model_dump(), indent=4),
                "get_domestic_rankings_async_success.json",
            )

    @pytest.mark.asyncio
    async def test_get_domestic_rankings_async_failure(self):
        """Test async wrapper with service failure."""
        with patch.object(
            comscore_wrapper.service, "get_domestic_rankings", new_callable=AsyncMock
        ) as mock_get:
            mock_get.return_value = None

            result = await comscore_wrapper.get_domestic_rankings()

            assert isinstance(result, BoxOfficeData)
            assert result.status_code == 500
            assert result.error is not None
            assert "Failed to fetch box office rankings" in result.error

    @pytest.mark.asyncio
    async def test_get_domestic_rankings_async_exception(self):
        """Test async wrapper with exception."""
        with patch.object(
            comscore_wrapper.service, "get_domestic_rankings", new_callable=AsyncMock
        ) as mock_get:
            mock_get.side_effect = Exception("Test error")

            result = await comscore_wrapper.get_domestic_rankings()

            assert isinstance(result, BoxOfficeData)
            assert result.status_code == 500
            assert result.error is not None
            assert "Test error" in result.error
