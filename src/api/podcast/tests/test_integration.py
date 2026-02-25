"""
Integration tests for Podcast service.
These tests hit actual PodcastIndex API endpoints with no mocks.

Requirements:
- PODCASTINDEX_API_KEY environment variable must be set
- PODCASTINDEX_API_SECRET environment variable must be set
- Internet connection required
- Tests may be slower due to actual API calls

Run with: pytest services/podcast/tests/test_integration.py -v
"""

import pytest

from api.podcast.core import PodcastService
from api.podcast.models import (
    EpisodeListResponse,
    MCEpisodeItem,
    MCPodcaster,
    MCPodcastItem,
    PersonSearchResponse,
    PodcasterSearchResponse,
    PodcastSearchResponse,
    PodcastTrendingResponse,
    PodcastWithLatestEpisode,
)
from api.podcast.search import PodcastSearchService
from api.podcast.wrappers import podcast_wrapper
from contracts.models import MCSources, MCSubType, MCType
from utils.pytest_utils import write_snapshot

pytestmark = pytest.mark.integration


@pytest.fixture
def podcast_service(podcast_credentials, monkeypatch):
    """Create PodcastService instance with real API credentials."""
    # Set environment variables for Auth to use
    monkeypatch.setenv("PODCASTINDEX_API_KEY", podcast_credentials["api_key"])
    monkeypatch.setenv("PODCASTINDEX_API_SECRET", podcast_credentials["api_secret"])
    return PodcastService()


@pytest.fixture
def podcast_search_service(podcast_credentials, monkeypatch):
    """Create PodcastSearchService instance with real API credentials."""
    # Set environment variables for Auth to use
    monkeypatch.setenv("PODCASTINDEX_API_KEY", podcast_credentials["api_key"])
    monkeypatch.setenv("PODCASTINDEX_API_SECRET", podcast_credentials["api_secret"])
    return PodcastSearchService()


def check_podcast_result(podcast: MCPodcastItem):
    """Validate MCPodcastItem model structure and data quality."""
    assert isinstance(podcast, MCPodcastItem)

    # Core fields
    assert podcast.id is not None
    assert podcast.id > 0
    assert podcast.title is not None
    assert len(podcast.title) > 0
    assert podcast.url is not None
    assert len(podcast.url) > 0

    # MediaCircle standardized fields - REQUIRED
    assert podcast.mc_id is not None, "mc_id is required"
    assert len(podcast.mc_id) > 0, "mc_id cannot be empty"
    assert podcast.mc_type == "podcast", "mc_type must be 'podcast'"
    assert podcast.source is not None, "source is required"
    assert podcast.source.value == "podcastindex", "source must be 'podcastindex'"
    assert podcast.source_id is not None, "source_id is required"
    assert len(podcast.source_id) > 0, "source_id cannot be empty"

    # Image should be present (critical for UI)
    assert podcast.image is not None or podcast.artwork is not None
    if podcast.image:
        assert podcast.image.startswith("http")
    if podcast.artwork:
        assert podcast.artwork.startswith("http")


def check_episode_result(episode: MCEpisodeItem):
    """Validate MCEpisodeItem model structure and data quality."""
    assert isinstance(episode, MCEpisodeItem)

    # Core fields
    assert episode.id is not None
    assert episode.id > 0
    assert episode.title is not None
    assert len(episode.title) > 0

    # MediaCircle standardized fields - REQUIRED
    assert episode.mc_id is not None, "mc_id is required"
    assert len(episode.mc_id) > 0, "mc_id cannot be empty"
    assert episode.mc_type == "podcast_episode", "mc_type must be 'podcast_episode'"
    assert episode.source is not None, "source is required"
    assert episode.source.value == "podcastindex", "source must be 'podcastindex'"
    assert episode.source_id is not None, "source_id is required"
    assert len(episode.source_id) > 0, "source_id cannot be empty"

    # Playback URL should be present
    if episode.enclosure_url:
        assert episode.enclosure_url.startswith("http")


class TestPodcastSearchServiceIntegration:
    """Integration tests for PodcastSearchService."""

    @pytest.mark.asyncio
    async def test_get_trending_podcasts(self, podcast_search_service):
        """Test getting trending podcasts."""
        result = await podcast_search_service.get_trending_podcasts(max_results=5)

        assert isinstance(result, PodcastTrendingResponse)
        assert result.total_results > 0
        assert len(result.results) > 0
        assert len(result.results) <= 5

        # Check each podcast
        for podcast in result.results:
            check_podcast_result(podcast)

    @pytest.mark.asyncio
    async def test_search_podcasts(self, podcast_search_service):
        """Test searching for podcasts."""
        result = await podcast_search_service.search_podcasts(query="true crime", max_results=10)

        assert isinstance(result, PodcastSearchResponse)
        assert result.query == "true crime"
        assert result.total_results > 0
        assert len(result.results) > 0

        # Check first result
        first_podcast = result.results[0]
        check_podcast_result(first_podcast)

        # Should have relevancy score
        assert first_podcast.relevancy_score is not None

    @pytest.mark.asyncio
    async def test_get_podcast_by_id(self, podcast_search_service):
        """Test getting a specific podcast by ID."""
        # Use a known podcast ID (e.g., Joe Rogan Experience)
        feed_id = 360084

        result = await podcast_search_service.get_podcast_by_id(feed_id)

        assert result is not None
        assert isinstance(result, MCPodcastItem)
        check_podcast_result(result)
        assert result.id == feed_id

    @pytest.mark.asyncio
    async def test_get_podcast_episodes(self, podcast_search_service):
        """Test getting episodes for a podcast."""
        # Use a known podcast ID
        feed_id = 360084

        result = await podcast_search_service.get_podcast_episodes(feed_id=feed_id, max_results=5)

        assert isinstance(result, EpisodeListResponse)
        assert result.feed_id == feed_id
        assert result.total_results > 0
        assert len(result.results) > 0
        assert len(result.results) <= 5

        # Check each episode
        for episode in result.results:
            check_episode_result(episode)

    @pytest.mark.asyncio
    async def test_get_podcast_with_latest_episode(self, podcast_search_service):
        """Test getting a podcast with its latest episode."""
        # Use a known podcast ID
        feed_id = 360084

        result = await podcast_search_service.get_podcast_with_latest_episode(feed_id)

        assert result is not None
        assert isinstance(result, PodcastWithLatestEpisode)
        check_podcast_result(result)
        assert result.id == feed_id

        # Check latest episode if present
        if result.latest_episode:
            check_episode_result(result.latest_episode)


class TestPodcastEdgeCases:
    """Integration tests for edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_search_with_empty_query(self, podcast_search_service):
        """Test search with empty query."""
        result = await podcast_search_service.search_podcasts(query="", max_results=10)

        assert isinstance(result, PodcastSearchResponse)
        # Should handle empty query gracefully
        assert result.total_results >= 0

    @pytest.mark.asyncio
    async def test_get_nonexistent_podcast(self, podcast_search_service):
        """Test getting a podcast that doesn't exist."""
        # Use an invalid feed ID
        feed_id = 999999999

        result = await podcast_search_service.get_podcast_by_id(feed_id)

        # Should return MCPodcastItem with error for nonexistent podcast
        assert isinstance(result, MCPodcastItem)
        assert result.status_code == 404
        assert result.error == "Podcast not found"

    @pytest.mark.asyncio
    async def test_trending_with_large_limit(self, podcast_search_service):
        """Test getting trending podcasts with large limit."""
        result = await podcast_search_service.get_trending_podcasts(max_results=50)

        assert isinstance(result, PodcastTrendingResponse)
        assert result.total_results > 0
        # Should handle large limits (may be capped internally)
        assert len(result.results) > 0


class TestPodcastDataQuality:
    """Integration tests to verify data quality and completeness."""

    @pytest.mark.asyncio
    async def test_podcast_data_completeness(self, podcast_search_service):
        """Test that podcast data contains all expected fields."""
        result = await podcast_search_service.search_podcasts(query="technology", max_results=5)

        assert len(result.results) > 0
        podcast = result.results[0]

        # Required fields
        required_fields = [
            "id",
            "title",
            "url",
            "mc_id",
            "mc_type",
        ]

        for field in required_fields:
            assert hasattr(podcast, field), f"Missing required field: {field}"
            assert getattr(podcast, field) is not None, f"Field {field} is None"

    @pytest.mark.asyncio
    async def test_episode_data_completeness(self, podcast_search_service):
        """Test that episode data contains all expected fields."""
        # Use a known podcast ID
        feed_id = 360084

        result = await podcast_search_service.get_podcast_episodes(feed_id=feed_id, max_results=5)

        assert len(result.results) > 0
        episode = result.results[0]

        # Required fields
        required_fields = [
            "id",
            "title",
            "mc_id",
            "mc_type",
        ]

        for field in required_fields:
            assert hasattr(episode, field), f"Missing required field: {field}"
            assert getattr(episode, field) is not None, f"Field {field} is None"

    @pytest.mark.asyncio
    async def test_mc_id_generation(self, podcast_search_service):
        """Test that mc_id is properly generated for all results."""
        result = await podcast_search_service.get_trending_podcasts(max_results=5)

        for podcast in result.results:
            assert podcast.mc_id is not None
            assert len(podcast.mc_id) > 0
            assert podcast.mc_type == "podcast"

            # mc_id should be unique and consistent
            # Format: podcast_{id}
            assert "podcast_" in podcast.mc_id

    @pytest.mark.asyncio
    async def test_search_by_person(self, podcast_search_service):
        """Test searching for podcasts and episodes by person."""
        # Use a well-known podcast host
        result = await podcast_search_service.search_by_person(
            person_name="Joe Rogan", max_results=10
        )

        assert isinstance(result, PersonSearchResponse)
        assert result.person_name == "Joe Rogan"
        assert result.total_podcasts + result.total_episodes > 0

        # Should have at least one podcast (Joe Rogan Experience)
        if result.total_podcasts > 0:
            podcast = result.podcasts[0]
            check_podcast_result(podcast)
            # Verify it's actually Joe Rogan's podcast
            assert "rogan" in podcast.title.lower() or "rogan" in (podcast.author or "").lower()

        # May have guest episodes
        if result.total_episodes > 0:
            episode = result.episodes[0]
            check_episode_result(episode)

    @pytest.mark.asyncio
    async def test_search_person(self, podcast_search_service):
        """Test searching for podcasters by person name."""
        # Use a well-known podcast host
        result = await podcast_search_service.search_person(person_name="Joe Rogan", max_results=10)

        assert isinstance(result, PodcasterSearchResponse)
        assert result.query == "Joe Rogan"
        assert result.total_results >= 0

        # If results found, check structure
        if result.total_results > 0:
            podcaster = result.results[0]
            assert isinstance(podcaster, MCPodcaster)
            assert podcaster.name == "Joe Rogan"
            assert podcaster.mc_type == "person"
            assert podcaster.mc_subtype == "podcaster"
            assert podcaster.podcast_count > 0
            assert len(podcaster.podcasts) > 0
            assert podcaster.total_episodes > 0

            # Verify required fields on podcaster
            assert podcaster.mc_id is not None, "mc_id is required"
            assert len(podcaster.mc_id) > 0, "mc_id cannot be empty"
            assert podcaster.source is not None, "source is required"
            assert podcaster.source.value == "podcastindex", "source must be 'podcastindex'"
            assert podcaster.source_id is not None, "source_id is required"
            assert len(podcaster.source_id) > 0, "source_id cannot be empty"

            # Verify composite source_id format (comma-delimited feed IDs)
            source_id_parts = podcaster.source_id.split(",")
            assert len(source_id_parts) == len(podcaster.podcasts), (
                f"source_id should contain {len(podcaster.podcasts)} feed IDs, got {len(source_id_parts)}"
            )
            # Verify each part is a valid feed ID (numeric string)
            for part in source_id_parts:
                assert part.strip().isdigit(), (
                    f"source_id part '{part}' should be a numeric feed ID"
                )

            # Check podcasts
            for podcast in podcaster.podcasts:
                check_podcast_result(podcast)
                # Verify podcast source_id is included in podcaster's composite source_id
                assert podcast.source_id in podcaster.source_id, (
                    f"Podcast source_id '{podcast.source_id}' should be in podcaster source_id '{podcaster.source_id}'"
                )

            # Check primary podcast fields
            if podcaster.primary_podcast_title:
                assert podcaster.primary_podcast_id is not None
                assert len(podcaster.primary_podcast_title) > 0

    @pytest.mark.asyncio
    async def test_search_person_async(self, podcast_credentials, monkeypatch):
        """Test search_person_async: should fetch podcasts by feed ID (Joe Rogan Experience)"""
        from contracts.models import MCPersonSearchRequest

        monkeypatch.setenv("PODCASTINDEX_API_KEY", podcast_credentials["api_key"])
        monkeypatch.setenv("PODCASTINDEX_API_SECRET", podcast_credentials["api_secret"])

        # Create a person search request using feed ID (Joe Rogan Experience feed ID: 360084)
        person_request = MCPersonSearchRequest(
            source_id="360084",  # Feed ID for Joe Rogan Experience
            source=MCSources.PODCASTINDEX,
            mc_type=MCType.PERSON,
            mc_id="podcaster_joe_rogan",
            mc_subtype=MCSubType.PODCASTER,
            name="Joe Rogan",
        )

        # Call the wrapper function
        result = await podcast_wrapper.search_person_async(person_request, limit=20)

        # Validate response structure
        assert result.status_code == 200
        assert result.error is None
        assert result.input == person_request

        # When using feed IDs, details should be None (no podcaster details available)
        assert result.details is None, "details should be None when using feed IDs directly"

        # Validate works array contains podcasts
        assert len(result.works) > 0, "works array should not be empty"

        # Verify we got the correct podcast
        found_podcast = False
        for work in result.works:
            work_dict = work.model_dump() if hasattr(work, "model_dump") else dict(work)
            item_validated = MCPodcastItem.model_validate(work_dict)
            assert item_validated.mc_type == MCType.PODCAST
            # Verify required MCBaseItem fields
            assert item_validated.mc_id is not None, (
                f"mc_id is missing for podcast: {item_validated.title}"
            )
            assert item_validated.mc_type == MCType.PODCAST, (
                f"mc_type is wrong for podcast: {item_validated.title}"
            )
            assert item_validated.source is not None, (
                f"source is missing for podcast: {item_validated.title}"
            )
            assert item_validated.source_id is not None, (
                f"source_id is missing for podcast: {item_validated.title}"
            )
            # Check if this is the Joe Rogan Experience podcast
            if item_validated.id == 360084:
                found_podcast = True
                check_podcast_result(item_validated)

        assert found_podcast, "Should have found the Joe Rogan Experience podcast (feed ID 360084)"

        # Validate related is empty (will be filled by search_broker)
        assert result.related == [], "related should be empty (filled by search_broker)"

        # Write snapshot
        write_snapshot(result.model_dump(), "search_person_works_podcast.json")

    @pytest.mark.asyncio
    async def test_search_person_async_peter_attia(self, podcast_credentials, monkeypatch):
        """Test search_person_async wrapper should fetch podcasts by feed ID."""
        from contracts.models import MCPersonSearchRequest

        monkeypatch.setenv("PODCASTINDEX_API_KEY", podcast_credentials["api_key"])
        monkeypatch.setenv("PODCASTINDEX_API_SECRET", podcast_credentials["api_secret"])

        # Use a known podcast feed ID (The Drive with Peter Attia: 75075)
        person_request = MCPersonSearchRequest(
            source_id="75075",  # Feed ID for The Drive with Peter Attia
            source=MCSources.PODCASTINDEX,
            mc_type=MCType.PERSON,
            mc_id="podcaster_peter_attia",
            mc_subtype=MCSubType.PODCASTER,
            name="Peter Attia",
        )

        # Call the wrapper function
        result = await podcast_wrapper.search_person_async(person_request, limit=20)

        # Validate response structure
        assert result.status_code == 200
        assert result.error is None
        assert result.input == person_request

        # When using feed IDs, details should be None
        assert result.details is None, "details should be None when using feed IDs directly"

        # Validate works array contains podcasts
        assert len(result.works) > 0, "works array should not be empty"

        # Verify we got the correct podcast
        found_podcast = False
        for work in result.works:
            work_dict = work.model_dump() if hasattr(work, "model_dump") else dict(work)
            item_validated = MCPodcastItem.model_validate(work_dict)
            assert item_validated.mc_type == MCType.PODCAST
            # Verify required MCBaseItem fields
            assert item_validated.mc_id is not None, (
                f"mc_id is missing for podcast: {item_validated.title}"
            )
            assert item_validated.mc_type == MCType.PODCAST, (
                f"mc_type is wrong for podcast: {item_validated.title}"
            )
            assert item_validated.source is not None, (
                f"source is missing for podcast: {item_validated.title}"
            )
            assert item_validated.source_id is not None, (
                f"source_id is missing for podcast: {item_validated.title}"
            )
            # Check if this is The Drive podcast
            if item_validated.id == 75075:
                found_podcast = True
                check_podcast_result(item_validated)

        assert found_podcast, "Should have found The Drive podcast (feed ID 75075)"

        # Validate related is empty (will be filled by search_broker)
        assert result.related == [], "related should be empty (filled by search_broker)"

        # Write snapshot
        write_snapshot(result.model_dump(), "search_person_works_podcast_peter_attia.json")

    @pytest.mark.asyncio
    async def test_search_person_async_invalid_feed_id(self, podcast_credentials, monkeypatch):
        """Test search_person_async with non-existent feed ID."""
        from contracts.models import MCPersonSearchRequest

        monkeypatch.setenv("PODCASTINDEX_API_KEY", podcast_credentials["api_key"])
        monkeypatch.setenv("PODCASTINDEX_API_SECRET", podcast_credentials["api_secret"])

        # Create a person search request with non-existent feed ID
        person_request = MCPersonSearchRequest(
            source_id="999999999",  # Non-existent feed ID
            source=MCSources.PODCASTINDEX,
            mc_type=MCType.PERSON,
            mc_id="podcaster_nonexistent",
            mc_subtype=MCSubType.PODCASTER,
            name="Test Podcaster",
        )

        # Call the wrapper function
        result = await podcast_wrapper.search_person_async(person_request, limit=20)

        # Validate error response
        assert result.status_code == 404
        assert result.error is not None
        assert result.details is None
        assert result.works == []
        assert result.related == []

        # Write snapshot
        write_snapshot(result.model_dump(), "search_person_works_podcast_invalid_feed_id.json")

    @pytest.mark.asyncio
    async def test_search_person_async_invalid_source(self, podcast_credentials, monkeypatch):
        """Test search_person_async with invalid source."""
        from contracts.models import MCPersonSearchRequest

        monkeypatch.setenv("PODCASTINDEX_API_KEY", podcast_credentials["api_key"])
        monkeypatch.setenv("PODCASTINDEX_API_SECRET", podcast_credentials["api_secret"])

        # Create a person search request with invalid source (not PodcastIndex)
        person_request = MCPersonSearchRequest(
            source_id="123",
            source=MCSources.TMDB,  # Invalid for PodcastIndex wrapper
            mc_type=MCType.PERSON,
            mc_id="podcaster_123",
            mc_subtype=MCSubType.PODCASTER,
            name="Test Podcaster",
        )

        # Call the wrapper function
        result = await podcast_wrapper.search_person_async(person_request)

        # Validate error response
        assert result.status_code == 400
        assert result.error is not None
        assert "Invalid source" in result.error
        assert result.details is None
        assert result.works == []
        assert result.related == []

    @pytest.mark.asyncio
    async def test_search_person_async_missing_source_id(self, podcast_credentials, monkeypatch):
        """Test search_person_async with missing source_id."""
        from contracts.models import MCPersonSearchRequest

        monkeypatch.setenv("PODCASTINDEX_API_KEY", podcast_credentials["api_key"])
        monkeypatch.setenv("PODCASTINDEX_API_SECRET", podcast_credentials["api_secret"])

        # Create a person search request without source_id
        person_request = MCPersonSearchRequest(
            source_id="",  # Empty source_id
            source=MCSources.PODCASTINDEX,
            mc_type=MCType.PERSON,
            mc_id="podcaster_123",
            mc_subtype=MCSubType.PODCASTER,
            name="Test Podcaster",
        )

        # Call the wrapper function
        result = await podcast_wrapper.search_person_async(person_request)

        # Validate error response
        assert result.status_code == 400
        assert result.error is not None
        assert "source_id" in result.error.lower()
        assert result.details is None
        assert result.works == []
        assert result.related == []

    @pytest.mark.asyncio
    async def test_search_person_async_invalid_source_id_format(
        self, podcast_credentials, monkeypatch
    ):
        """Test search_person_async with invalid source_id format."""
        from contracts.models import MCPersonSearchRequest

        monkeypatch.setenv("PODCASTINDEX_API_KEY", podcast_credentials["api_key"])
        monkeypatch.setenv("PODCASTINDEX_API_SECRET", podcast_credentials["api_secret"])

        # Create a person search request with invalid source_id (not a number)
        person_request = MCPersonSearchRequest(
            source_id="not_a_number",
            source=MCSources.PODCASTINDEX,
            mc_type=MCType.PERSON,
            mc_id="podcaster_123",
            mc_subtype=MCSubType.PODCASTER,
            name="Test Podcaster",
        )

        # Call the wrapper function
        result = await podcast_wrapper.search_person_async(person_request)

        # Validate error response
        assert result.status_code == 400
        assert result.error is not None
        assert result.details is None
        assert result.works == []
        assert result.related == []

    @pytest.mark.asyncio
    async def test_search_person_async_comma_delimited_feed_ids(
        self, podcast_credentials, monkeypatch
    ):
        """Test search_person_async with comma-delimited feed IDs."""
        from contracts.models import MCPersonSearchRequest

        monkeypatch.setenv("PODCASTINDEX_API_KEY", podcast_credentials["api_key"])
        monkeypatch.setenv("PODCASTINDEX_API_SECRET", podcast_credentials["api_secret"])

        # Create a person search request with multiple feed IDs (comma-delimited)
        # Joe Rogan Experience: 360084, The Drive: 75075
        person_request = MCPersonSearchRequest(
            source_id="360084,75075",
            source=MCSources.PODCASTINDEX,
            mc_type=MCType.PERSON,
            mc_id="podcaster_multiple",
            mc_subtype=MCSubType.PODCASTER,
            name="Multiple Podcasters",
        )

        # Call the wrapper function
        result = await podcast_wrapper.search_person_async(person_request, limit=20)

        # Validate response structure
        assert result.status_code == 200
        assert result.error is None
        assert result.input == person_request

        # When using feed IDs, details should be None
        assert result.details is None, "details should be None when using feed IDs directly"

        # Validate works array contains podcasts (should have at least 2)
        assert len(result.works) >= 2, "works array should contain at least 2 podcasts"

        # Verify we got both podcasts
        feed_ids_found = set()
        for work in result.works:
            work_dict = work.model_dump() if hasattr(work, "model_dump") else dict(work)
            item_validated = MCPodcastItem.model_validate(work_dict)
            assert item_validated.mc_type == MCType.PODCAST
            check_podcast_result(item_validated)
            feed_ids_found.add(item_validated.id)

        # Should have found both feed IDs
        assert 360084 in feed_ids_found, "Should have found Joe Rogan Experience (feed ID 360084)"
        assert 75075 in feed_ids_found, "Should have found The Drive (feed ID 75075)"

        # Validate related is empty (will be filled by search_broker)
        assert result.related == [], "related should be empty (filled by search_broker)"

        # Write snapshot
        write_snapshot(result.model_dump(), "search_person_works_podcast_comma_delimited.json")

    @pytest.mark.asyncio
    async def test_get_podcasts_by_ids(self, podcast_search_service):
        """Test get_podcasts_by_ids method to fetch multiple podcasts by feed IDs."""
        # Use known podcast feed IDs
        feed_ids = [360084, 75075]  # Joe Rogan Experience, The Drive

        result = await podcast_search_service.get_podcasts_by_ids(feed_ids)

        # Should return list of MCPodcastItem
        assert isinstance(result, list)
        assert len(result) == 2, "Should have fetched 2 podcasts"

        # Verify each podcast
        feed_ids_found = set()
        for podcast in result:
            assert isinstance(podcast, MCPodcastItem)
            check_podcast_result(podcast)
            feed_ids_found.add(podcast.id)

        # Should have found both feed IDs
        assert 360084 in feed_ids_found, "Should have found Joe Rogan Experience (feed ID 360084)"
        assert 75075 in feed_ids_found, "Should have found The Drive (feed ID 75075)"

    @pytest.mark.asyncio
    async def test_get_podcasts_by_ids_with_invalid_id(self, podcast_search_service):
        """Test get_podcasts_by_ids with mix of valid and invalid feed IDs."""
        # Mix of valid and invalid feed IDs
        feed_ids = [360084, 999999999, 75075]  # Valid, Invalid, Valid

        result = await podcast_search_service.get_podcasts_by_ids(feed_ids)

        # Should return list with only valid podcasts
        assert isinstance(result, list)
        assert len(result) == 2, "Should have fetched 2 valid podcasts (invalid one skipped)"

        # Verify we got the valid ones
        feed_ids_found = set()
        for podcast in result:
            assert isinstance(podcast, MCPodcastItem)
            check_podcast_result(podcast)
            feed_ids_found.add(podcast.id)

        # Should have found both valid feed IDs
        assert 360084 in feed_ids_found, "Should have found Joe Rogan Experience (feed ID 360084)"
        assert 75075 in feed_ids_found, "Should have found The Drive (feed ID 75075)"
        assert 999999999 not in feed_ids_found, "Should not have found invalid feed ID"
