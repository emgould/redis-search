"""
Integration tests for Watchmode service.

These tests hit the actual Watchmode API and require:
- WATCHMODE_API_KEY environment variable
- TMDB_READ_TOKEN environment variable (for wrapper tests)
- Internet connection

Run with: pytest test_integration.py -m integration
"""

import json
import os

import pytest

from api.watchmode.core import WatchmodeService
from api.watchmode.wrappers import watchmode_wrapper
from utils.pytest_utils import write_snapshot

# Skip all integration tests if API key is not available
pytestmark = pytest.mark.integration

WATCHMODE_API_KEY = os.getenv("WATCHMODE_API_KEY")
TMDB_READ_TOKEN = os.getenv("TMDB_READ_TOKEN")

# Test IDs will be fetched dynamically from new releases
# These are set as module-level variables after fetching
MOVIE_TEST_ID = None
TV_TEST_ID = None


async def get_test_ids():
    """Fetch valid Watchmode IDs from new releases.

    Returns:
        Tuple of (movie_id, tv_id) or (None, None) if not found
    """
    global MOVIE_TEST_ID, TV_TEST_ID

    # If already fetched, return cached values
    if MOVIE_TEST_ID is not None and TV_TEST_ID is not None:
        return MOVIE_TEST_ID, TV_TEST_ID

    if not WATCHMODE_API_KEY:
        return None, None

    try:
        service = WatchmodeService(WATCHMODE_API_KEY)
        result = await service.get_new_releases(limit=50)

        if not result or "releases" not in result:
            return None, None

        releases = result["releases"]

        for release in releases:
            if MOVIE_TEST_ID is None and release.get("type") == "movie":
                MOVIE_TEST_ID = release.get("id")
            if TV_TEST_ID is None and release.get("type") in ["tv_series", "tv_special"]:
                TV_TEST_ID = release.get("id")

            if MOVIE_TEST_ID and TV_TEST_ID:
                break

        return MOVIE_TEST_ID, TV_TEST_ID
    except Exception as e:
        print(f"Warning: Could not fetch test IDs: {e}")
        return None, None


@pytest.mark.skipif(not WATCHMODE_API_KEY, reason="WATCHMODE_API_KEY not set")
class TestWatchmodeServiceIntegration:
    """Integration tests for WatchmodeService."""

    @pytest.mark.asyncio
    async def test_get_new_releases(self):
        """Test getting new releases from API."""
        service = WatchmodeService(WATCHMODE_API_KEY)
        result = await service.get_new_releases(limit=10)

        assert result is not None
        assert "releases" in result
        assert isinstance(result["releases"], list)

    @pytest.mark.asyncio
    async def test_get_title_details_movie(self):
        """Test getting movie title details from API."""
        movie_id, _ = await get_test_ids()

        if movie_id is None:
            pytest.skip("No movie ID available from new releases")

        service = WatchmodeService(WATCHMODE_API_KEY)
        result = await service.get_title_details(movie_id)

        assert result is not None
        assert "id" in result
        assert "title" in result

    @pytest.mark.asyncio
    async def test_get_title_details_tv(self):
        """Test getting TV title details from API."""
        _, tv_id = await get_test_ids()

        if tv_id is None:
            pytest.skip("No TV ID available from new releases")

        service = WatchmodeService(WATCHMODE_API_KEY)
        result = await service.get_title_details(tv_id)

        assert result is not None
        assert "id" in result
        assert "title" in result

    @pytest.mark.asyncio
    async def test_get_title_streaming_sources(self):
        """Test getting streaming sources from API."""
        movie_id, _ = await get_test_ids()

        if movie_id is None:
            pytest.skip("No movie ID available from new releases")

        service = WatchmodeService(WATCHMODE_API_KEY)
        result = await service.get_title_streaming_sources(movie_id)

        assert result is not None
        # Watchmode API always returns a list for the sources endpoint
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_search_titles(self):
        """Test searching titles from API."""
        service = WatchmodeService(WATCHMODE_API_KEY)
        result = await service.search_titles("Breaking Bad")

        assert result is not None
        # Should have some structure
        assert isinstance(result, dict)


@pytest.mark.skipif(
    not WATCHMODE_API_KEY or not TMDB_READ_TOKEN,
    reason="WATCHMODE_API_KEY or TMDB_READ_TOKEN not set",
)
class TestWatchmodeWrappersIntegration:
    """Integration tests for Watchmode wrapper functions."""

    @pytest.mark.asyncio
    async def test_get_whats_new(self):
        """Test get_whats_new wrapper with real APIs."""
        # Set API keys via environment (wrapper loads from env/SecretParam)
        import os

        from contracts.models import MCSources, MCType

        from api.watchmode.auth import watchmode_auth

        original_watchmode = os.environ.get("WATCHMODE_API_KEY")
        original_tmdb = os.environ.get("TMDB_READ_TOKEN")
        os.environ["WATCHMODE_API_KEY"] = WATCHMODE_API_KEY
        os.environ["TMDB_READ_TOKEN"] = TMDB_READ_TOKEN

        # Reset auth's cached keys
        watchmode_auth._watchmode_api_key = None
        watchmode_auth._tmdb_read_token = None

        try:
            result = await watchmode_wrapper.get_whats_new(
                limit=5,
            )
        finally:
            # Restore environment
            if original_watchmode:
                os.environ["WATCHMODE_API_KEY"] = original_watchmode
            elif "WATCHMODE_API_KEY" in os.environ:
                del os.environ["WATCHMODE_API_KEY"]
            if original_tmdb:
                os.environ["TMDB_READ_TOKEN"] = original_tmdb
            elif "TMDB_READ_TOKEN" in os.environ:
                del os.environ["TMDB_READ_TOKEN"]
            watchmode_auth._watchmode_api_key = None
            watchmode_auth._tmdb_read_token = None

        assert result.status_code == 200
        assert result.error is None
        assert result.results is not None
        assert isinstance(result.results, list)
        assert result.total_results is not None
        assert result.data_source is not None

        # Verify required fields for all results
        for item in result.results:
            item_name = item.title if hasattr(item, "title") else item.name
            assert item.mc_id, f"mc_id is missing or empty for item: {item_name}"
            assert item.mc_type, f"mc_type is missing or empty for item: {item_name}"
            assert item.source, f"source is missing or empty for item: {item_name}"
            assert item.source_id, f"source_id is missing or empty for item: {item_name}"

            # Verify correct values
            assert item.mc_type in [MCType.MOVIE, MCType.TV_SERIES]
            assert item.source == MCSources.TMDB

        # Write snapshot for integration test
        write_snapshot(result.model_dump(), "whats_new_integration.json")

    @pytest.mark.asyncio
    async def test_get_watchmode_title_details(self):
        """Test get_watchmode_title_details wrapper with real API."""
        movie_id, _ = await get_test_ids()

        if movie_id is None:
            pytest.skip("No movie ID available from new releases")

        # Set API key via environment (wrapper loads from env/SecretParam)
        import os

        from contracts.models import MCSources, MCType

        from api.watchmode.auth import watchmode_auth

        original_watchmode = os.environ.get("WATCHMODE_API_KEY")
        os.environ["WATCHMODE_API_KEY"] = WATCHMODE_API_KEY

        # Reset auth's cached key
        watchmode_auth._watchmode_api_key = None

        try:
            result = await watchmode_wrapper.get_watchmode_title_details(
                watchmode_id=movie_id,
            )
        finally:
            # Restore environment
            if original_watchmode:
                os.environ["WATCHMODE_API_KEY"] = original_watchmode
            elif "WATCHMODE_API_KEY" in os.environ:
                del os.environ["WATCHMODE_API_KEY"]
            watchmode_auth._watchmode_api_key = None

        assert result.status_code == 200
        assert result.error is None
        assert result.id is not None
        assert result.title is not None
        assert result.streaming_sources is not None

        # Verify required fields
        assert result.mc_id, "mc_id is missing or empty"
        assert result.mc_type, "mc_type is missing or empty"
        assert result.source, "source is missing or empty"
        assert result.source_id, "source_id is missing or empty"

        # Verify correct values
        assert result.mc_type == MCType.MIXED
        assert result.source == MCSources.TMDB

        # Write snapshot for integration test
        write_snapshot(result.model_dump(), "title_details_integration.json")
