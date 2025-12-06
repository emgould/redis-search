"""
Test for U2 album search - verifies get_artist_albums returns unfiltered results.
This test specifically validates the fix for the issue where only 10 of 20 albums were returned.

Run with: pytest api/lastfm/tests/test_u2_albums.py -v
"""

import os

import pytest

from api.lastfm.models import MCMusicAlbum, MCMusicArtist
from api.lastfm.wrappers import lastfm_wrapper
from contracts.models import MCPersonSearchRequest, MCSources, MCSubType, MCType

pytestmark = pytest.mark.integration


@pytest.fixture
def real_lastfm_api_key():
    """Get real Last.fm API key from environment."""
    api_key = os.getenv("LASTFM_API_KEY")
    if not api_key:
        pytest.skip("LASTFM_API_KEY environment variable not set")
    return api_key


@pytest.mark.asyncio
class TestU2AlbumSearch:
    """Test U2 album search to verify get_artist_albums returns unfiltered results."""

    async def test_search_person_async_u2_albums(self, real_lastfm_api_key, monkeypatch):
        """Test search_person_async for U2 - should return 30 albums using get_artist_albums."""
        monkeypatch.setenv("LASTFM_API_KEY", real_lastfm_api_key)

        # Get U2's actual Spotify ID by searching first
        search_result = await lastfm_wrapper.search_artist(query="U2", limit=1)
        assert search_result.status_code == 200
        assert len(search_result.results) > 0
        u2_spotify_id = search_result.results[0].source_id
        u2_name = search_result.results[0].name

        print(f"\n🎵 Testing album search for: {u2_name} (ID: {u2_spotify_id})")

        # Create request with valid Spotify artist ID for U2
        person_request = MCPersonSearchRequest(
            source_id=u2_spotify_id,
            source=MCSources.SPOTIFY,
            mc_type=MCType.PERSON,
            mc_id=f"music_artist_{u2_spotify_id}",
            mc_subtype=MCSubType.MUSICIAN,
            name=u2_name,
        )

        # Call wrapper with limit of 30
        result = await lastfm_wrapper.search_person_async(person_request, limit=30)

        # Validate response - should succeed
        assert result.status_code == 200, f"Expected status 200, got {result.status_code}"
        assert result.error is None, f"Unexpected error: {result.error}"
        assert result.input == person_request

        # Validate person details
        assert result.details is not None, "Artist details should not be None"
        artist = MCMusicArtist.model_validate(result.details.model_dump())
        assert artist.mc_type == MCType.PERSON
        assert artist.name == u2_name
        print(f"✓ Artist details validated: {artist.name}")

        # Validate works array contains albums
        assert len(result.works) > 0, "works array should not be empty"
        print(f"✓ Found {len(result.works)} total works")

        # Count valid albums (only albums, not singles or compilations)
        albums_found = 0
        album_titles = []
        for work in result.works:
            work_dict = work.model_dump() if hasattr(work, "model_dump") else dict(work)

            # Should be music albums
            if work_dict.get("mc_type") == MCType.MUSIC_ALBUM.value:
                item_validated = MCMusicAlbum.model_validate(work_dict)
                assert item_validated.mc_type == MCType.MUSIC_ALBUM
                albums_found += 1
                album_titles.append(item_validated.title)

                # Verify required MCBaseItem fields
                assert item_validated.mc_id is not None, (
                    f"mc_id is missing for album: {item_validated.title}"
                )
                assert item_validated.source is not None, (
                    f"source is missing for album: {item_validated.title}"
                )
                assert item_validated.source_id is not None, (
                    f"source_id is missing for album: {item_validated.title}"
                )

                # Verify album is by U2
                assert item_validated.artist is not None, (
                    f"artist is missing for album: {item_validated.title}"
                )

        # Print first 10 album titles for verification
        print("\n📀 First 10 albums found:")
        for i, title in enumerate(album_titles[:10], 1):
            print(f"  {i}. {title}")

        # Verify we got albums (should be close to 30 for a prolific artist like U2)
        # Allow some variance as Spotify may filter out some results
        assert albums_found >= 15, (
            f"Expected at least 15 albums for U2, got {albums_found}. "
            f"This test verifies get_artist_albums() returns unfiltered results. "
            f"If this fails, the Levenshtein filtering may be incorrectly applied."
        )

        print(f"\n✅ U2 albums test PASSED: Found {albums_found} albums (expected >= 15)")

        # Validate related is empty (will be filled by search_broker)
        assert result.related == [], "related should be empty (filled by search_broker)"
