"""
Integration tests for Spotify API.
These tests make real API calls and should be run sparingly.
"""

import pytest

from api.subapi.spotify.wrappers import spotify_wrapper
from utils.pytest_utils import write_snapshot

pytestmark = pytest.mark.integration


class TestSpotifyIntegration:
    """Integration tests for Spotify API."""

    @pytest.mark.asyncio
    async def test_search_albums_integration(self):
        """Integration test for album search."""
        result_dict, status_code = await spotify_wrapper.search_albums(query="Pink Floyd", limit=5)

        assert status_code == 200
        assert result_dict.get("error") is None
        assert "results" in result_dict
        assert len(result_dict["results"]) > 0

        # Verify MCBaseItem fields
        if result_dict["results"]:
            assert "mc_id" in result_dict["results"][0]
            assert "mc_type" in result_dict["results"][0]
            assert result_dict["results"][0]["mc_type"] == "music_album"

        # Write snapshot
        write_snapshot(result_dict, "spotify_search_albums_integration.json")

    @pytest.mark.asyncio
    async def test_search_artists_integration(self):
        """Integration test for artist search."""
        result_dict, status_code = await spotify_wrapper.search_artists(query="The Weeknd", limit=5)

        assert status_code == 200
        assert result_dict.get("error") is None
        assert "results" in result_dict
        assert len(result_dict["results"]) > 0

        # Verify MCBaseItem fields
        if result_dict["results"]:
            assert "mc_id" in result_dict["results"][0]
            assert "mc_type" in result_dict["results"][0]
            assert result_dict["results"][0]["mc_type"] == "music_artist"

        # Write snapshot
        write_snapshot(result_dict, "spotify_search_artists_integration.json")

    @pytest.mark.asyncio
    async def test_search_by_genre_integration(self):
        """Integration test for genre search."""
        result_dict, status_code = await spotify_wrapper.search_by_genre(genre="rock", limit=10)

        assert status_code == 200
        assert result_dict.get("error") is None
        assert "results" in result_dict
        assert len(result_dict["results"]) > 0

        # Verify MCBaseItem fields
        if result_dict["results"]:
            assert "mc_id" in result_dict["results"][0]
            assert "mc_type" in result_dict["results"][0]
            assert result_dict["results"][0]["mc_type"] == "music_artist"

        # Write snapshot
        write_snapshot(result_dict, "spotify_search_by_genre_integration.json")

    @pytest.mark.asyncio
    async def test_search_by_keyword_integration(self):
        """Integration test for keyword search."""
        result_dict, status_code = await spotify_wrapper.search_by_keyword(keyword="jazz", limit=10)

        assert status_code == 200
        assert result_dict.get("error") is None
        assert "results" in result_dict
        assert len(result_dict["results"]) > 0

        # Verify MCBaseItem fields
        if result_dict["results"]:
            assert "mc_id" in result_dict["results"][0]
            assert "mc_type" in result_dict["results"][0]

        # Write snapshot
        write_snapshot(result_dict, "spotify_search_by_keyword_integration.json")

    @pytest.mark.asyncio
    async def test_search_albums_with_track_type_integration(self):
        """Integration test for album search using track query - should extract albums from track results."""
        result_dict, status_code = await spotify_wrapper.search_albums(
            query="track:wish you were here artist:pink floyd", type="track", limit=5
        )

        assert status_code == 200
        assert result_dict.get("error") is None
        assert "results" in result_dict
        assert len(result_dict["results"]) > 0

        # Verify that albums were extracted from tracks
        # Verify MCBaseItem fields
        if result_dict["results"]:
            assert "mc_id" in result_dict["results"][0]
            assert "mc_type" in result_dict["results"][0]
            assert result_dict["results"][0]["mc_type"] == "music_album"
            # Verify album data is present
            assert "title" in result_dict["results"][0]
            assert "artist" in result_dict["results"][0]

        # Write snapshot
        write_snapshot(result_dict, "spotify_search_albums_track_integration.json")
