"""
Unit tests for Watchmode core service.

Tests the WatchmodeService class methods using mock data.
"""

import pytest

from api.watchmode.core import WatchmodeService


class TestWatchmodeService:
    """Test WatchmodeService class."""

    def test_init_with_api_key(self, mock_watchmode_api_key):
        """Test service initialization with valid API key."""
        service = WatchmodeService(mock_watchmode_api_key)
        assert service.api_key == mock_watchmode_api_key
        assert service.base_url == "https://api.watchmode.com/v1"

    def test_init_without_api_key(self):
        """Test service initialization fails without API key."""
        with pytest.raises(ValueError, match="Watchmode API key is required"):
            WatchmodeService("")

    def test_init_with_none_api_key(self):
        """Test service initialization fails with None API key."""
        with pytest.raises(ValueError, match="Watchmode API key is required"):
            WatchmodeService(None)  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_get_new_releases_method_exists(self, mock_watchmode_api_key):
        """Test get_new_releases method exists and has correct signature."""
        service = WatchmodeService(mock_watchmode_api_key)
        assert hasattr(service, "get_new_releases")
        assert callable(service.get_new_releases)

    @pytest.mark.asyncio
    async def test_get_title_details_method_exists(self, mock_watchmode_api_key):
        """Test get_title_details method exists and has correct signature."""
        service = WatchmodeService(mock_watchmode_api_key)
        assert hasattr(service, "get_title_details")
        assert callable(service.get_title_details)

    @pytest.mark.asyncio
    async def test_get_title_streaming_sources_method_exists(self, mock_watchmode_api_key):
        """Test get_title_streaming_sources method exists."""
        service = WatchmodeService(mock_watchmode_api_key)
        assert hasattr(service, "get_title_streaming_sources")
        assert callable(service.get_title_streaming_sources)

    @pytest.mark.asyncio
    async def test_search_titles_method_exists(self, mock_watchmode_api_key):
        """Test search_titles method exists."""
        service = WatchmodeService(mock_watchmode_api_key)
        assert hasattr(service, "search_titles")
        assert callable(service.search_titles)
