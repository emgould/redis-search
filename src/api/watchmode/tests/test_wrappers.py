"""
Unit tests for Watchmode async wrapper functions.

Tests the async wrapper functions for Firebase Functions compatibility.
"""

import os
from unittest.mock import AsyncMock, patch

import pytest
from contracts.models import MCSources, MCType

from api.tmdb.models import MCMovieItem, MCTvItem
from api.watchmode.models import (
    WatchmodeSearchResponse,
    WatchmodeTitleDetailsResponse,
    WatchmodeWhatsNewResponse,
)
from api.watchmode.wrappers import watchmode_wrapper


class TestGetWhatsNew:
    """Test get_whats_new wrapper method."""

    @pytest.mark.asyncio
    async def test_missing_api_key(self):
        """Test wrapper returns error when API key is missing."""
        from api.watchmode.auth import watchmode_auth

        # Clear API keys from auth instance
        original_watchmode = watchmode_auth._watchmode_api_key
        original_tmdb = watchmode_auth._tmdb_read_token
        watchmode_auth._watchmode_api_key = None
        watchmode_auth._tmdb_read_token = None

        # Clear environment
        original_env_watchmode = os.environ.pop("WATCHMODE_API_KEY", None)
        original_env_tmdb = os.environ.pop("TMDB_READ_TOKEN", None)

        try:
            result = await watchmode_wrapper.get_whats_new()
            # Should get 400 for missing API key
            assert result.status_code in (400, 500)  # Could be 500 if ValueError raised
            assert result.error is not None
            assert "API key" in result.error or "not available" in result.error
        finally:
            # Restore
            watchmode_auth._watchmode_api_key = original_watchmode
            watchmode_auth._tmdb_read_token = original_tmdb
            if original_env_watchmode:
                os.environ["WATCHMODE_API_KEY"] = original_env_watchmode
            if original_env_tmdb:
                os.environ["TMDB_READ_TOKEN"] = original_env_tmdb

    @pytest.mark.asyncio
    async def test_missing_tmdb_token(self, mock_watchmode_api_key, mock_tmdb_token):
        """Test wrapper returns error when TMDB token is missing."""
        from api.watchmode.auth import watchmode_auth

        # Set watchmode key but clear TMDB token
        original_watchmode = watchmode_auth._watchmode_api_key
        original_tmdb = watchmode_auth._tmdb_read_token
        watchmode_auth._watchmode_api_key = mock_watchmode_api_key
        watchmode_auth._tmdb_read_token = None

        # Clear environment
        original_env_tmdb = os.environ.pop("TMDB_READ_TOKEN", None)

        try:
            result = await watchmode_wrapper.get_whats_new(limit=1)
            assert result.status_code == 400
            assert result.error is not None
            assert result.error == "TMDB token is required for enhanced data"
        finally:
            # Restore
            watchmode_auth._watchmode_api_key = original_watchmode
            watchmode_auth._tmdb_read_token = original_tmdb
            if original_env_tmdb:
                os.environ["TMDB_READ_TOKEN"] = original_env_tmdb


class TestGetWatchmodeTitleDetails:
    """Test get_watchmode_title_details wrapper method."""

    @pytest.mark.asyncio
    async def test_missing_api_key(self):
        """Test wrapper returns error when API key is missing."""
        from api.watchmode.auth import watchmode_auth

        # Clear API key from auth instance
        original_watchmode = watchmode_auth._watchmode_api_key
        watchmode_auth._watchmode_api_key = None

        # Clear environment
        original_env_watchmode = os.environ.pop("WATCHMODE_API_KEY", None)

        try:
            result = await watchmode_wrapper.get_watchmode_title_details(watchmode_id=123)
            assert result.status_code == 400
            assert result.error is not None
            assert result.error == "Watchmode API key is required"
        finally:
            # Restore
            watchmode_auth._watchmode_api_key = original_watchmode
            if original_env_watchmode:
                os.environ["WATCHMODE_API_KEY"] = original_env_watchmode


class TestWrapperRequiredFields:
    """Tests for required fields in wrapper responses."""

    @pytest.mark.asyncio
    async def test_get_whats_new_has_required_fields(self, mock_watchmode_api_key, mock_tmdb_token):
        """Test that get_whats_new results have mc_id, mc_type, source, and source_id."""
        from api.watchmode.auth import watchmode_auth

        # Create mock movie and TV items with all required fields
        mock_movie = MCMovieItem(
            tmdb_id=12345,
            title="Test Movie",
            poster_path="/test.jpg",
            source_id="12345",
        )
        mock_tv = MCTvItem(
            tmdb_id=67890,
            name="Test TV Show",
            poster_path="/test2.jpg",
            source_id="67890",
        )
        mock_response = WatchmodeWhatsNewResponse(
            results=[mock_movie, mock_tv],
            total_results=2,
            region="US",
            generated_at="2024-01-01T00:00:00",
        )

        # Set API keys
        original_watchmode = watchmode_auth._watchmode_api_key
        original_tmdb = watchmode_auth._tmdb_read_token
        watchmode_auth._watchmode_api_key = mock_watchmode_api_key
        watchmode_auth._tmdb_read_token = mock_tmdb_token

        try:
            # Mock the wrapper's internal implementation
            with (
                patch("api.watchmode.wrappers.WatchmodeService") as mock_service_class,
                patch("api.watchmode.wrappers.TMDBService") as mock_tmdb_class,
            ):
                # Setup mocks
                mock_service = AsyncMock()
                mock_service.get_new_releases.return_value = {
                    "releases": [
                        {"id": 1, "tmdb_id": 12345, "type": "movie"},
                        {"id": 2, "tmdb_id": 67890, "type": "tv_series"},
                    ]
                }
                mock_service_class.return_value = mock_service

                mock_tmdb = AsyncMock()
                mock_tmdb.image_base_url = "https://image.tmdb.org/t/p/w500"
                mock_tmdb._make_request.side_effect = [
                    {
                        "id": 12345,
                        "title": "Test Movie",
                        "original_title": "Test Movie",
                        "original_language": "en",
                        "poster_path": "/test.jpg",
                        "genres": [],
                        "overview": "Test overview",
                    },
                    {
                        "id": 67890,
                        "name": "Test TV Show",
                        "original_name": "Test TV Show",
                        "original_language": "en",
                        "poster_path": "/test2.jpg",
                        "genres": [],
                        "overview": "Test overview",
                    },
                ]
                mock_tmdb.enhance_media_item.side_effect = lambda x: x
                mock_tmdb_class.return_value = mock_tmdb

                result = await watchmode_wrapper.get_whats_new(limit=2)

            assert result.status_code == 200
            assert len(result.results) == 2

            # Verify required fields for each result
            for item in result.results:
                # Verify required fields are present and not None/empty
                item_name = item.title if hasattr(item, "title") else item.name
                assert item.mc_id, f"mc_id is missing or empty for item: {item_name}"
                assert item.mc_type, f"mc_type is missing or empty for item: {item_name}"
                assert item.source, f"source is missing or empty for item: {item_name}"
                assert item.source_id, f"source_id is missing or empty for item: {item_name}"

                # Verify correct values
                assert item.mc_type in [MCType.MOVIE, MCType.TV_SERIES]
                assert item.source == MCSources.TMDB

        finally:
            # Restore
            watchmode_auth._watchmode_api_key = original_watchmode
            watchmode_auth._tmdb_read_token = original_tmdb

    @pytest.mark.asyncio
    async def test_get_watchmode_title_details_has_required_fields(self, mock_watchmode_api_key):
        """Test that get_watchmode_title_details result has mc_id, mc_type, source, and source_id."""
        from api.watchmode.auth import watchmode_auth

        # Set API key
        original_watchmode = watchmode_auth._watchmode_api_key
        watchmode_auth._watchmode_api_key = mock_watchmode_api_key

        try:
            # Mock the wrapper's internal implementation
            with patch("api.watchmode.wrappers.WatchmodeService") as mock_service_class:
                mock_service = AsyncMock()
                mock_service.get_title_details.return_value = {
                    "id": 123,
                    "title": "Test Title",
                    "type": "movie",
                }
                mock_service.get_title_streaming_sources.return_value = []
                mock_service_class.return_value = mock_service

                result = await watchmode_wrapper.get_watchmode_title_details(watchmode_id=123)

            assert result.status_code == 200

            # Verify required fields are present and not None/empty
            assert result.mc_id, "mc_id is missing or empty"
            assert result.mc_type, "mc_type is missing or empty"
            assert result.source, "source is missing or empty"
            assert result.source_id, "source_id is missing or empty"

            # Verify correct values
            assert result.mc_type == MCType.MIXED
            assert result.source == MCSources.TMDB
            assert result.source_id == "123"

        finally:
            # Restore
            watchmode_auth._watchmode_api_key = original_watchmode


class TestSearchTitles:
    """Test search_titles wrapper method."""

    @pytest.mark.asyncio
    async def test_missing_api_key(self):
        """Test wrapper returns error when API key is missing."""
        from api.watchmode.auth import watchmode_auth

        # Clear API key from auth instance
        original_watchmode = watchmode_auth._watchmode_api_key
        watchmode_auth._watchmode_api_key = None

        # Clear environment
        original_env_watchmode = os.environ.pop("WATCHMODE_API_KEY", None)

        try:
            result = await watchmode_wrapper.search_titles(query="Breaking Bad")
            assert result.status_code == 400
            assert result.error is not None
            assert result.error == "Watchmode API key is required"
        finally:
            # Restore
            watchmode_auth._watchmode_api_key = original_watchmode
            if original_env_watchmode:
                os.environ["WATCHMODE_API_KEY"] = original_env_watchmode

    @pytest.mark.asyncio
    async def test_empty_query(self, mock_watchmode_api_key):
        """Test wrapper returns error when query is empty."""
        from api.watchmode.auth import watchmode_auth

        # Set API key
        original_watchmode = watchmode_auth._watchmode_api_key
        watchmode_auth._watchmode_api_key = mock_watchmode_api_key

        try:
            result = await watchmode_wrapper.search_titles(query="")
            assert result.status_code == 400
            assert result.error is not None
            assert result.error == "Search query is required"
        finally:
            # Restore
            watchmode_auth._watchmode_api_key = original_watchmode

    @pytest.mark.asyncio
    async def test_search_titles_has_required_fields(self, mock_watchmode_api_key):
        """Test that search_titles result has mc_id, mc_type, source, and source_id."""
        from api.watchmode.auth import watchmode_auth

        # Set API key
        original_watchmode = watchmode_auth._watchmode_api_key
        watchmode_auth._watchmode_api_key = mock_watchmode_api_key

        try:
            # Mock the wrapper's internal implementation
            with patch("api.watchmode.wrappers.WatchmodeService") as mock_service_class:
                mock_service = AsyncMock()
                mock_service.search_titles.return_value = {
                    "title_results": [
                        {
                            "id": 3173903,
                            "name": "Breaking Bad",
                            "type": "tv_series",
                            "year": 2008,
                            "imdb_id": "tt0903747",
                            "tmdb_id": 1396,
                            "tmdb_type": "tv",
                        },
                        {
                            "id": 4146033,
                            "name": "Breaking Bad Wolf",
                            "type": "tv_movie",
                            "year": 2018,
                            "imdb_id": "tt9746510",
                            "tmdb_id": 635602,
                            "tmdb_type": "movie",
                        },
                    ],
                    "people_results": [],
                }
                mock_service_class.return_value = mock_service

                result = await watchmode_wrapper.search_titles(query="Breaking Bad")

            assert result.status_code == 200
            assert result.total_results == 2
            assert result.query == "Breaking Bad"

            # Verify required fields are present and not None/empty
            assert result.mc_id, "mc_id is missing or empty"
            assert result.mc_type, "mc_type is missing or empty"
            assert result.source, "source is missing or empty"
            assert result.source_id, "source_id is missing or empty"

            # Verify correct values
            assert result.mc_type == MCType.MIXED
            assert result.source == MCSources.WATCHMODE

            # Verify search results have watchmode IDs
            assert len(result.results) == 2
            assert result.results[0].id == 3173903
            assert result.results[0].name == "Breaking Bad"
            assert result.results[0].tmdb_id == 1396
            assert result.results[1].id == 4146033
            assert result.results[1].name == "Breaking Bad Wolf"
            assert result.results[1].tmdb_id == 635602

        finally:
            # Restore
            watchmode_auth._watchmode_api_key = original_watchmode
