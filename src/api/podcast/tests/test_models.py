"""
Tests for Podcast Pydantic models.
"""

import pytest

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

pytestmark = pytest.mark.unit


class TestMCPodcastItem:
    """Tests for MCPodcastItem model."""

    def test_podcast_result_creation(self):
        """Test creating a MCPodcastItem with all fields."""
        podcast = MCPodcastItem(
            id=360084,
            title="The Joe Rogan Experience",
            url="https://example.com/feed.xml",
            site="https://example.com",
            description="A test podcast",
            author="Joe Rogan",
            owner_name="Joe Rogan",
            image="https://example.com/image.jpg",
            artwork="https://example.com/artwork.jpg",
            trend_score=85.5,
            language="en",
            episode_count=2000,
            itunes_id=123456,
            spotify_url="https://open.spotify.com/show/123",
        )

        assert podcast.id == 360084
        assert podcast.title == "The Joe Rogan Experience"
        assert podcast.url == "https://example.com/feed.xml"
        assert podcast.episode_count == 2000
        assert podcast.mc_type == "podcast"

    def test_podcast_result_auto_generates_mc_id(self):
        """Test that mc_id is auto-generated if not provided."""
        podcast = MCPodcastItem(
            id=12345,
            title="Test Podcast",
            url="https://example.com/feed.xml",
        )

        assert podcast.mc_id is not None
        assert "podcast_" in podcast.mc_id

    def test_podcast_result_auto_generates_mc_type(self):
        """Test that mc_type is auto-generated if not provided."""
        podcast = MCPodcastItem(
            id=12345,
            title="Test Podcast",
            url="https://example.com/feed.xml",
        )

        assert podcast.mc_type == "podcast"

    def test_podcast_result_with_minimal_fields(self):
        """Test creating a MCPodcastItem with only required fields."""
        podcast = MCPodcastItem(
            id=12345,
            title="Minimal Podcast",
            url="https://example.com/feed.xml",
        )

        assert podcast.id == 12345
        assert podcast.title == "Minimal Podcast"
        assert podcast.url == "https://example.com/feed.xml"
        assert podcast.episode_count == 0
        assert podcast.site is None
        assert podcast.spotify_url is None

    def test_podcast_result_with_relevancy_score(self):
        """Test MCPodcastItem with relevancy score for search results."""
        podcast = MCPodcastItem(
            id=12345,
            title="Search Result",
            url="https://example.com/feed.xml",
            trend_score=75.0,
            episode_count=100,
            relevancy_score=82.5,
        )

        assert podcast.relevancy_score == 82.5
        assert podcast.trend_score == 75.0


class TestMCEpisodeItem:
    """Tests for MCEpisodeItem model."""

    def test_episode_result_creation(self):
        """Test creating an MCEpisodeItem with all fields."""
        episode = MCEpisodeItem(
            id=54321,
            title="Test Episode",
            description="Episode description",
            link="https://example.com/episode",
            guid="episode-guid-123",
            date_published="2024-01-01T00:00:00",
            enclosure_url="https://example.com/audio.mp3",
            enclosure_type="audio/mpeg",
            enclosure_length=50000000,
            duration_seconds=3600,
            explicit=False,
            episode_type="full",
            season=1,
            episode=1,
            feed_id=360084,
            feed_title="The Joe Rogan Experience",
            image="https://example.com/episode-image.jpg",
        )

        assert episode.id == 54321
        assert episode.title == "Test Episode"
        assert episode.enclosure_url == "https://example.com/audio.mp3"
        assert episode.duration_seconds == 3600
        assert episode.mc_type == "podcast_episode"

    def test_episode_result_auto_generates_mc_id(self):
        """Test that mc_id is auto-generated if not provided."""
        episode = MCEpisodeItem(
            id=54321,
            title="Test Episode",
        )

        assert episode.mc_id is not None
        assert episode.mc_id == "episode_54321"  # Format is "episode_{id}"

    def test_episode_result_auto_generates_mc_type(self):
        """Test that mc_type is auto-generated if not provided."""
        episode = MCEpisodeItem(
            id=54321,
            title="Test Episode",
        )

        assert episode.mc_type == "podcast_episode"

    def test_episode_result_with_minimal_fields(self):
        """Test creating an MCEpisodeItem with only required fields."""
        episode = MCEpisodeItem(
            id=54321,
            title="Minimal Episode",
        )

        assert episode.id == 54321
        assert episode.title == "Minimal Episode"
        assert episode.description is None
        assert episode.enclosure_url is None
        assert episode.duration_seconds is None

    def test_episode_result_playback_url(self):
        """Test that enclosure_url is the playback URL."""
        episode = MCEpisodeItem(
            id=54321,
            title="Playback Test",
            enclosure_url="https://cdn.example.com/audio/episode.mp3",
            enclosure_type="audio/mpeg",
        )

        assert episode.enclosure_url == "https://cdn.example.com/audio/episode.mp3"
        assert episode.enclosure_type == "audio/mpeg"


class TestPodcastWithLatestEpisode:
    """Tests for PodcastWithLatestEpisode model."""

    def test_podcast_with_latest_episode_creation(self):
        """Test creating a PodcastWithLatestEpisode."""
        episode = MCEpisodeItem(
            id=54321,
            title="Latest Episode",
            enclosure_url="https://example.com/audio.mp3",
        )

        podcast = PodcastWithLatestEpisode(
            id=360084,
            title="Test Podcast",
            url="https://example.com/feed.xml",
            latest_episode=episode,
        )

        assert podcast.id == 360084
        assert podcast.title == "Test Podcast"
        assert podcast.latest_episode is not None
        assert podcast.latest_episode.id == 54321
        assert podcast.latest_episode.title == "Latest Episode"

    def test_podcast_with_latest_episode_without_episode(self):
        """Test PodcastWithLatestEpisode without latest episode."""
        podcast = PodcastWithLatestEpisode(
            id=360084,
            title="Test Podcast",
            url="https://example.com/feed.xml",
            latest_episode=None,
        )

        assert podcast.id == 360084
        assert podcast.latest_episode is None

    def test_podcast_with_latest_episode_inherits_podcast_fields(self):
        """Test that PodcastWithLatestEpisode inherits all MCPodcastItem fields."""
        podcast = PodcastWithLatestEpisode(
            id=360084,
            title="Test Podcast",
            url="https://example.com/feed.xml",
            author="Test Author",
            episode_count=100,
            trend_score=85.0,
        )

        assert podcast.author == "Test Author"
        assert podcast.episode_count == 100
        assert podcast.trend_score == 85.0
        assert podcast.mc_type == "podcast"


class TestPodcastTrendingResponse:
    """Tests for PodcastTrendingResponse model."""

    def test_podcast_trending_response_creation(self):
        """Test creating a PodcastTrendingResponse."""
        podcasts = [
            MCPodcastItem(id=1, title="Podcast 1", url="https://example.com/1"),
            MCPodcastItem(id=2, title="Podcast 2", url="https://example.com/2"),
        ]

        response = PodcastTrendingResponse(
            date="2025-01-15",
            results=podcasts,
            total_results=2,
        )

        assert len(response.results) == 2
        assert response.total_results == 2
        assert response.data_source == "PodcastIndex Trending"

    def test_podcast_trending_response_empty(self):
        """Test PodcastTrendingResponse with no results."""
        response = PodcastTrendingResponse(
            date="2025-01-15",
            results=[],
            total_results=0,
        )

        assert len(response.results) == 0
        assert response.total_results == 0


class TestPodcastSearchResponse:
    """Tests for PodcastSearchResponse model."""

    def test_podcast_search_response_creation(self):
        """Test creating a PodcastSearchResponse."""
        podcasts = [
            MCPodcastItem(id=1, title="Result 1", url="https://example.com/1"),
            MCPodcastItem(id=2, title="Result 2", url="https://example.com/2"),
        ]

        response = PodcastSearchResponse(
            date="2025-01-15",
            results=podcasts,
            total_results=2,
            query="true crime",
        )

        assert len(response.results) == 2
        assert response.total_results == 2
        assert response.query == "true crime"
        assert response.data_source == "PodcastIndex Search"

    def test_podcast_search_response_empty(self):
        """Test PodcastSearchResponse with no results."""
        response = PodcastSearchResponse(
            date="2025-01-15",
            results=[],
            total_results=0,
            query="nonexistent",
        )

        assert len(response.results) == 0
        assert response.total_results == 0
        assert response.query == "nonexistent"


class TestEpisodeListResponse:
    """Tests for EpisodeListResponse model."""

    def test_episode_list_response_creation(self):
        """Test creating an EpisodeListResponse."""
        episodes = [
            MCEpisodeItem(id=1, title="Episode 1"),
            MCEpisodeItem(id=2, title="Episode 2"),
            MCEpisodeItem(id=3, title="Episode 3"),
        ]

        response = EpisodeListResponse(
            date="2025-01-15",
            results=episodes,
            total_results=3,
            feed_id=360084,
        )

        assert len(response.results) == 3
        assert response.total_results == 3
        assert response.feed_id == 360084
        assert response.data_source == "PodcastIndex Episodes"

    def test_episode_list_response_empty(self):
        """Test EpisodeListResponse with no episodes."""
        response = EpisodeListResponse(
            date="2025-01-15",
            results=[],
            total_results=0,
            feed_id=360084,
        )

        assert len(response.results) == 0
        assert response.total_results == 0
        assert response.feed_id == 360084


class TestModelSerialization:
    """Tests for model serialization."""

    def test_podcast_result_to_dict(self):
        """Test MCPodcastItem serialization to dict."""
        podcast = MCPodcastItem(
            id=12345,
            title="Test Podcast",
            url="https://example.com/feed.xml",
        )

        data = podcast.model_dump()

        assert isinstance(data, dict)
        assert data["id"] == 12345
        assert data["title"] == "Test Podcast"
        assert "mc_id" in data
        assert "mc_type" in data

    def test_episode_result_to_dict(self):
        """Test MCEpisodeItem serialization to dict."""
        episode = MCEpisodeItem(
            id=54321,
            title="Test Episode",
        )

        data = episode.model_dump()

        assert isinstance(data, dict)
        assert data["id"] == 54321
        assert data["title"] == "Test Episode"
        assert "mc_id" in data
        assert "mc_type" in data

    def test_response_models_to_dict(self):
        """Test response models serialization to dict."""
        podcasts = [
            MCPodcastItem(id=1, title="Podcast 1", url="https://example.com/1"),
        ]
        response = PodcastTrendingResponse(
            date="2025-01-15",
            results=podcasts,
            total_results=1,
        )

        data = response.model_dump()

        assert isinstance(data, dict)
        assert "results" in data
        assert "total_results" in data
        assert isinstance(data["results"], list)
        assert isinstance(data["results"][0], dict)


class TestMCPodcaster:
    """Tests for MCPodcaster model."""

    def test_podcaster_creation(self):
        """Test creating an MCPodcaster with all fields."""
        podcasts = [
            MCPodcastItem(
                id=360084,
                title="The Joe Rogan Experience",
                url="https://example.com/feed.xml",
                author="Joe Rogan",
                episode_count=2000,
                image="https://example.com/image.jpg",
            ),
            MCPodcastItem(
                id=500000,
                title="JRE Clips",
                url="https://example.com/clips.xml",
                author="Joe Rogan",
                episode_count=500,
            ),
        ]

        podcaster = MCPodcaster(
            name="Joe Rogan",
            id="joe_rogan",
            podcasts=podcasts,
            total_episodes=2500,
            image="https://example.com/image.jpg",
            bio="Comedian and podcast host",
            website="https://joerogan.com",
            primary_podcast_title="The Joe Rogan Experience",
            primary_podcast_id=360084,
        )

        assert podcaster.name == "Joe Rogan"
        assert podcaster.id == "joe_rogan"
        assert len(podcaster.podcasts) == 2
        assert podcaster.total_episodes == 2500
        assert podcaster.podcast_count == 2  # Auto-calculated by validator
        assert podcaster.image == "https://example.com/image.jpg"
        assert podcaster.bio == "Comedian and podcast host"
        assert podcaster.primary_podcast_title == "The Joe Rogan Experience"
        assert podcaster.mc_type == "person"
        assert podcaster.mc_subtype == "podcaster"

    def test_podcaster_auto_generates_mc_id(self):
        """Test that mc_id is auto-generated if not provided."""
        podcaster = MCPodcaster(
            name="Test Podcaster",
            id="test_podcaster",
            podcasts=[],
        )

        assert podcaster.mc_id is not None
        assert len(podcaster.mc_id) > 0

    def test_podcaster_auto_calculates_podcast_count(self):
        """Test that podcast_count is auto-calculated from podcasts list."""
        podcasts = [
            MCPodcastItem(id=1, title="Podcast 1", url="https://example.com/1"),
            MCPodcastItem(id=2, title="Podcast 2", url="https://example.com/2"),
            MCPodcastItem(id=3, title="Podcast 3", url="https://example.com/3"),
        ]

        podcaster = MCPodcaster(
            name="Test Podcaster",
            id="test",
            podcasts=podcasts,
            total_episodes=100,
        )

        assert podcaster.podcast_count == 3
        assert len(podcaster.podcasts) == 3

    def test_podcaster_with_minimal_fields(self):
        """Test creating an MCPodcaster with minimal fields."""
        podcaster = MCPodcaster(
            name="Minimal Podcaster",
            id="minimal",
            podcasts=[],
        )

        assert podcaster.name == "Minimal Podcaster"
        assert podcaster.podcast_count == 0
        assert podcaster.total_episodes == 0
        assert podcaster.image is None
        assert podcaster.bio is None
        assert podcaster.website is None

    def test_podcaster_serialization(self):
        """Test MCPodcaster serialization to dict."""
        podcasts = [
            MCPodcastItem(id=1, title="Podcast 1", url="https://example.com/1"),
        ]

        podcaster = MCPodcaster(
            name="Test Podcaster",
            id="test",
            podcasts=podcasts,
            total_episodes=100,
        )

        data = podcaster.model_dump()

        assert isinstance(data, dict)
        assert data["name"] == "Test Podcaster"
        assert data["id"] == "test"
        assert "mc_id" in data
        assert "mc_type" in data
        assert "mc_subtype" in data
        assert "podcasts" in data
        assert isinstance(data["podcasts"], list)


class TestPodcasterSearchResponse:
    """Tests for PodcasterSearchResponse model."""

    def test_podcaster_search_response_creation(self):
        """Test creating a PodcasterSearchResponse."""
        podcasts1 = [
            MCPodcastItem(id=1, title="Podcast 1", url="https://example.com/1"),
        ]
        podcasts2 = [
            MCPodcastItem(id=2, title="Podcast 2", url="https://example.com/2"),
        ]

        podcasters = [
            MCPodcaster(name="Joe Rogan", id="joe_rogan", podcasts=podcasts1, total_episodes=100),
            MCPodcaster(
                name="Lex Fridman", id="lex_fridman", podcasts=podcasts2, total_episodes=200
            ),
        ]

        response = PodcasterSearchResponse(
            date="2025-01-15",
            results=podcasters,
            total_results=2,
            query="podcaster",
        )

        assert len(response.results) == 2
        assert response.total_results == 2
        assert response.query == "podcaster"
        assert response.data_source == "PodcastIndex Podcaster Search"
        assert response.data_type == "person"

    def test_podcaster_search_response_empty(self):
        """Test PodcasterSearchResponse with no results."""
        response = PodcasterSearchResponse(
            date="2025-01-15",
            results=[],
            total_results=0,
            query="nonexistent",
        )

        assert len(response.results) == 0
        assert response.total_results == 0
        assert response.query == "nonexistent"
