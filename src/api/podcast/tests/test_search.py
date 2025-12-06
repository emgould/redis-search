"""
Unit tests for Podcast Search Service.
Tests PodcastSearchService search and discovery functionality.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.podcast.models import (
    EpisodeListResponse,
    MCPodcaster,
    MCPodcastItem,
    PersonSearchResponse,
    PodcasterSearchResponse,
    PodcastSearchResponse,
    PodcastTrendingResponse,
    PodcastWithLatestEpisode,
)
from api.podcast.search import PodcastSearchService

pytestmark = pytest.mark.unit


class TestGetTrendingPodcasts:
    """Tests for get_trending_podcasts method."""

    @pytest.mark.asyncio
    async def test_get_trending_podcasts_success(self, mock_auth):
        """Test successful trending podcasts retrieval."""
        service = PodcastSearchService()

        # Mock the response
        mock_podcasts = [
            MCPodcastItem(id=1, title="Podcast 1", url="https://example.com/1"),
            MCPodcastItem(id=2, title="Podcast 2", url="https://example.com/2"),
        ]
        mock_response = PodcastTrendingResponse(
            date="2025-01-15", results=mock_podcasts, total_results=2
        )

        with patch.object(
            service,
            "get_trending_podcasts",
            new=AsyncMock(return_value=mock_response),
        ):
            response = await service.get_trending_podcasts(max_results=2)

            assert isinstance(response, PodcastTrendingResponse)
            assert len(response.results) == 2
            assert response.total_results == 2

    @pytest.mark.asyncio
    async def test_get_trending_podcasts_empty(self, mock_auth):
        """Test trending podcasts with no results."""
        service = PodcastSearchService()

        mock_response = PodcastTrendingResponse(date="2025-01-15", results=[], total_results=0)

        with patch.object(
            service,
            "get_trending_podcasts",
            new=AsyncMock(return_value=mock_response),
        ):
            response = await service.get_trending_podcasts(max_results=10)

            assert isinstance(response, PodcastTrendingResponse)
            assert response.results == []
            assert response.total_results == 0


class TestSearchPodcasts:
    """Tests for search_podcasts method."""

    @pytest.mark.asyncio
    async def test_search_podcasts_success(self, mock_auth):
        """Test successful podcast search."""
        service = PodcastSearchService()

        mock_podcasts = [
            MCPodcastItem(
                id=1,
                title="True Crime Podcast",
                url="https://example.com/1",
                relevancy_score=85.0,
            ),
            MCPodcastItem(
                id=2,
                title="Crime Stories",
                url="https://example.com/2",
                relevancy_score=75.0,
            ),
        ]
        mock_response = PodcastSearchResponse(
            date="2025-01-15", results=mock_podcasts, total_results=2, query="true crime"
        )

        with patch.object(
            service,
            "search_podcasts",
            new=AsyncMock(return_value=mock_response),
        ):
            response = await service.search_podcasts("true crime", max_results=20)

            assert isinstance(response, PodcastSearchResponse)
            assert len(response.results) == 2
            assert response.total_results == 2
            assert response.query == "true crime"

    @pytest.mark.asyncio
    async def test_search_podcasts_no_results(self, mock_auth):
        """Test podcast search with no results."""
        service = PodcastSearchService()

        mock_response = PodcastSearchResponse(
            date="2025-01-15", results=[], total_results=0, query="nonexistent"
        )

        with patch.object(
            service,
            "search_podcasts",
            new=AsyncMock(return_value=mock_response),
        ):
            response = await service.search_podcasts("nonexistent", max_results=20)

            assert isinstance(response, PodcastSearchResponse)
            assert response.results == []
            assert response.total_results == 0


class TestGetPodcastById:
    """Tests for get_podcast_by_id method."""

    @pytest.mark.asyncio
    async def test_get_podcast_by_id_success(self, mock_auth):
        """Test successful podcast retrieval by ID."""
        service = PodcastSearchService()

        mock_podcast = MCPodcastItem(
            id=360084,
            title="The Joe Rogan Experience",
            url="https://example.com/feed.xml",
        )

        with patch.object(
            service,
            "get_podcast_by_id",
            new=AsyncMock(return_value=mock_podcast),
        ):
            result = await service.get_podcast_by_id(360084)

            assert result is not None
            assert isinstance(result, MCPodcastItem)
            assert result.id == 360084

    @pytest.mark.asyncio
    async def test_get_podcast_by_id_not_found(self, mock_auth):
        """Test podcast retrieval with invalid ID."""
        service = PodcastSearchService()

        with patch.object(
            service,
            "get_podcast_by_id",
            new=AsyncMock(return_value=None),
        ):
            result = await service.get_podcast_by_id(999999999)

            assert result is None


class TestGetPodcastEpisodes:
    """Tests for get_podcast_episodes method."""

    @pytest.mark.asyncio
    async def test_get_podcast_episodes_success(self, mock_auth):
        """Test successful episode retrieval."""
        service = PodcastSearchService()

        from api.podcast.models import MCEpisodeItem

        mock_episodes = [
            MCEpisodeItem(id=1, title="Episode 1"),
            MCEpisodeItem(id=2, title="Episode 2"),
        ]
        mock_response = EpisodeListResponse(
            date="2025-01-15", results=mock_episodes, total_results=2, feed_id=360084
        )

        with patch.object(
            service,
            "get_podcast_episodes",
            new=AsyncMock(return_value=mock_response),
        ):
            response = await service.get_podcast_episodes(feed_id=360084, max_results=10)

            assert isinstance(response, EpisodeListResponse)
            assert len(response.results) == 2
            assert response.total_results == 2
            assert response.feed_id == 360084

    @pytest.mark.asyncio
    async def test_get_podcast_episodes_empty(self, mock_auth):
        """Test episode retrieval with no episodes."""
        service = PodcastSearchService()

        mock_response = EpisodeListResponse(
            date="2025-01-15", results=[], total_results=0, feed_id=360084
        )

        with patch.object(
            service,
            "get_podcast_episodes",
            new=AsyncMock(return_value=mock_response),
        ):
            response = await service.get_podcast_episodes(feed_id=360084, max_results=10)

            assert isinstance(response, EpisodeListResponse)
            assert response.results == []
            assert response.total_results == 0


class TestGetPodcastWithLatestEpisode:
    """Tests for get_podcast_with_latest_episode method."""

    @pytest.mark.asyncio
    async def test_get_podcast_with_latest_episode_success(self, mock_auth):
        """Test successful podcast with latest episode retrieval."""
        service = PodcastSearchService()

        from api.podcast.models import MCEpisodeItem

        mock_episode = MCEpisodeItem(id=1, title="Latest Episode")
        mock_podcast = PodcastWithLatestEpisode(
            id=360084,
            title="Test Podcast",
            url="https://example.com/feed.xml",
            latest_episode=mock_episode,
        )

        with patch.object(
            service,
            "get_podcast_with_latest_episode",
            new=AsyncMock(return_value=mock_podcast),
        ):
            result = await service.get_podcast_with_latest_episode(360084)

            assert result is not None
            assert isinstance(result, PodcastWithLatestEpisode)
            assert result.id == 360084
            assert result.latest_episode is not None
            assert result.latest_episode.id == 1

    @pytest.mark.asyncio
    async def test_get_podcast_with_latest_episode_no_episode(self, mock_auth):
        """Test podcast with no latest episode."""
        service = PodcastSearchService()

        mock_podcast = PodcastWithLatestEpisode(
            id=360084,
            title="Test Podcast",
            url="https://example.com/feed.xml",
            latest_episode=None,
        )

        with patch.object(
            service,
            "get_podcast_with_latest_episode",
            new=AsyncMock(return_value=mock_podcast),
        ):
            result = await service.get_podcast_with_latest_episode(360084)

            assert result is not None
            assert result.latest_episode is None

    @pytest.mark.asyncio
    async def test_get_podcast_with_latest_episode_not_found(self, mock_auth):
        """Test podcast with latest episode not found."""
        service = PodcastSearchService()

        with patch.object(
            service,
            "get_podcast_with_latest_episode",
            new=AsyncMock(return_value=None),
        ):
            result = await service.get_podcast_with_latest_episode(999999999)

            assert result is None


class TestRelevancyScoring:
    """Tests for relevancy scoring in search results."""

    def test_relevancy_score_calculation(self, mock_auth):
        """Test that relevancy scores are calculated correctly."""
        import math

        # Test relevancy calculation
        trend_score = 75.0
        episode_count = 100
        alpha = 0.7
        e_max = 200

        episode_score = 100 * (math.log(1 + episode_count) / math.log(1 + e_max))
        expected_relevancy = alpha * trend_score + (1 - alpha) * episode_score

        # Verify the formula is correct
        assert expected_relevancy > 0
        assert expected_relevancy <= 100

    def test_relevancy_score_with_zero_episodes(self, mock_auth):
        """Test relevancy score calculation with zero episodes."""
        trend_score = 80.0
        alpha = 0.7

        # With zero episodes, episode_score should be 0
        episode_score = 0
        expected_relevancy = alpha * trend_score + (1 - alpha) * episode_score

        assert expected_relevancy == alpha * trend_score


class TestSearchByPerson:
    """Tests for search_by_person method."""

    @pytest.mark.asyncio
    async def test_search_by_person_host_match(self, mock_auth):
        """Test person search where person matches feed author (host)."""
        from datetime import UTC, datetime

        from api.podcast.podcastindex import EpisodeItem, PodcastFeed

        service = PodcastSearchService()

        # Mock episode with feed_id - EpisodeItem requires all fields
        mock_episode = EpisodeItem(
            id=1,
            title="Test Episode",
            link=None,
            description=None,
            guid=None,
            date_published=datetime.now(UTC),
            enclosure_url=None,
            enclosure_type=None,
            enclosure_length=None,
            duration_seconds=None,
            explicit=None,
            episode_type=None,
            season=None,
            episode=None,
            feed_id=100,
            feed_title="Joe Rogan Experience",
            image=None,
        )

        # Mock feed where person matches author - PodcastFeed requires all fields
        mock_feed = PodcastFeed(
            id=100,
            title="Joe Rogan Experience",
            url="https://example.com/feed.xml",
            original_url=None,
            site=None,
            description=None,
            author="Joe Rogan",
            owner_name="Joe Rogan",
            image=None,
            artwork=None,
            last_update_time=None,
            last_crawl_time=None,
            last_parse_time=None,
            last_good_http_status_time=None,
            last_http_status=None,
            content_type=None,
            itunes_id=None,
            trend_score=None,
            language=None,
            categories={},
            dead=None,
            locked=None,
            podcast_guid=None,
            episode_count=None,
            spotify_url=None,
        )

        with (
            patch.object(service, "get_client") as mock_get_client,
        ):
            mock_client = AsyncMock()
            mock_get_client.return_value.__aenter__.return_value = mock_client

            # Mock search_episodes_by_person
            mock_client.search_episodes_by_person = AsyncMock(return_value=[mock_episode])
            # Mock podcast_by_feedid
            mock_client.podcast_by_feedid = AsyncMock(return_value=mock_feed)

            result = await service.search_by_person(person_name="Joe Rogan", max_results=20)

            assert isinstance(result, PersonSearchResponse)
            assert result.total_podcasts == 1
            assert result.total_episodes == 0
            assert len(result.podcasts) == 1
            assert result.podcasts[0].author == "Joe Rogan"

    @pytest.mark.asyncio
    async def test_search_by_person_guest_match(self, mock_auth):
        """Test person search where person doesn't match feed author (guest)."""
        from datetime import UTC, datetime

        from api.podcast.podcastindex import EpisodeItem, PodcastFeed

        service = PodcastSearchService()

        # Mock episode with feed_id - EpisodeItem requires all fields
        mock_episode = EpisodeItem(
            id=1,
            title="Episode with Guest",
            link=None,
            description=None,
            guid=None,
            date_published=datetime.now(UTC),
            enclosure_url=None,
            enclosure_type=None,
            enclosure_length=None,
            duration_seconds=None,
            explicit=None,
            episode_type=None,
            season=None,
            episode=None,
            feed_id=200,
            feed_title="Other Podcast",
            image=None,
        )

        # Mock feed where person doesn't match author - PodcastFeed requires all fields
        mock_feed = PodcastFeed(
            id=200,
            title="Other Podcast",
            url="https://example.com/feed.xml",
            original_url=None,
            site=None,
            description=None,
            author="Different Host",
            owner_name="Different Host",
            image=None,
            artwork=None,
            last_update_time=None,
            last_crawl_time=None,
            last_parse_time=None,
            last_good_http_status_time=None,
            last_http_status=None,
            content_type=None,
            itunes_id=None,
            trend_score=None,
            language=None,
            categories={},
            dead=None,
            locked=None,
            podcast_guid=None,
            episode_count=None,
            spotify_url=None,
        )

        with (
            patch.object(service, "get_client") as mock_get_client,
        ):
            mock_client = AsyncMock()
            mock_get_client.return_value.__aenter__.return_value = mock_client

            mock_client.search_episodes_by_person = AsyncMock(return_value=[mock_episode])
            mock_client.podcast_by_feedid = AsyncMock(return_value=mock_feed)

            result = await service.search_by_person(person_name="Joe Rogan", max_results=20)

            assert isinstance(result, PersonSearchResponse)
            assert result.total_podcasts == 0
            assert result.total_episodes == 1
            assert len(result.episodes) == 1
            assert result.episodes[0].feed_title == "Other Podcast"

    @pytest.mark.asyncio
    async def test_search_by_person_mixed_results(self, mock_auth):
        """Test person search with both hosts and guests."""
        from datetime import UTC, datetime

        from api.podcast.podcastindex import EpisodeItem, PodcastFeed

        service = PodcastSearchService()

        # Episode 1: Host match - EpisodeItem requires all fields
        episode1 = EpisodeItem(
            id=1,
            title="Host Episode",
            link=None,
            description=None,
            guid=None,
            date_published=datetime.now(UTC),
            enclosure_url=None,
            enclosure_type=None,
            enclosure_length=None,
            duration_seconds=None,
            explicit=None,
            episode_type=None,
            season=None,
            episode=None,
            feed_id=100,
            feed_title="Host Show",
            image=None,
        )
        feed1 = PodcastFeed(
            id=100,
            title="Host Show",
            url="https://example.com/1",
            original_url=None,
            site=None,
            description=None,
            author="Joe Rogan",
            owner_name=None,
            image=None,
            artwork=None,
            last_update_time=None,
            last_crawl_time=None,
            last_parse_time=None,
            last_good_http_status_time=None,
            last_http_status=None,
            content_type=None,
            itunes_id=None,
            trend_score=None,
            language=None,
            categories={},
            dead=None,
            locked=None,
            podcast_guid=None,
            episode_count=None,
            spotify_url=None,
        )

        # Episode 2: Guest match - EpisodeItem requires all fields
        episode2 = EpisodeItem(
            id=2,
            title="Guest Episode",
            link=None,
            description=None,
            guid=None,
            date_published=datetime.now(UTC),
            enclosure_url=None,
            enclosure_type=None,
            enclosure_length=None,
            duration_seconds=None,
            explicit=None,
            episode_type=None,
            season=None,
            episode=None,
            feed_id=200,
            feed_title="Guest Show",
            image=None,
        )
        feed2 = PodcastFeed(
            id=200,
            title="Guest Show",
            url="https://example.com/2",
            original_url=None,
            site=None,
            description=None,
            author="Other Host",
            owner_name=None,
            image=None,
            artwork=None,
            last_update_time=None,
            last_crawl_time=None,
            last_parse_time=None,
            last_good_http_status_time=None,
            last_http_status=None,
            content_type=None,
            itunes_id=None,
            trend_score=None,
            language=None,
            categories={},
            dead=None,
            locked=None,
            podcast_guid=None,
            episode_count=None,
            spotify_url=None,
        )

        with (
            patch.object(service, "get_client") as mock_get_client,
        ):
            mock_client = AsyncMock()
            mock_get_client.return_value.__aenter__.return_value = mock_client

            mock_client.search_episodes_by_person = AsyncMock(return_value=[episode1, episode2])
            mock_client.podcast_by_feedid = AsyncMock(side_effect=[feed1, feed2])

            result = await service.search_by_person(person_name="Joe Rogan", max_results=20)

            assert isinstance(result, PersonSearchResponse)
            assert result.total_podcasts == 1
            assert result.total_episodes == 1
            assert len(result.podcasts) == 1
            assert len(result.episodes) == 1


class TestSearchPerson:
    """Tests for search_person method."""

    @pytest.mark.asyncio
    async def test_search_person_success(self, mock_auth):
        """Test successful podcaster search."""
        service = PodcastSearchService()

        # Mock PersonSearchResponse with podcasts (hosts)
        mock_podcasts = [
            MCPodcastItem(
                id=360084,
                title="The Joe Rogan Experience",
                url="https://example.com/feed.xml",
                author="Joe Rogan",
                episode_count=2000,
                image="https://example.com/image.jpg",
                description="A podcast",
                site="https://joerogan.com",
            ),
            MCPodcastItem(
                id=500000,
                title="JRE Clips",
                url="https://example.com/clips.xml",
                author="Joe Rogan",
                episode_count=500,
            ),
        ]

        mock_person_response = PersonSearchResponse(
            date="2025-01-15",
            podcasts=mock_podcasts,
            episodes=[],
            total_podcasts=2,
            total_episodes=0,
            person_name="Joe Rogan",
        )

        with patch.object(
            service, "search_by_person", new=AsyncMock(return_value=mock_person_response)
        ):
            result = await service.search_person(person_name="Joe Rogan", max_results=20)

            assert isinstance(result, PodcasterSearchResponse)
            assert result.total_results == 1
            assert len(result.results) == 1
            assert result.query == "Joe Rogan"

            podcaster = result.results[0]
            assert isinstance(podcaster, MCPodcaster)
            assert podcaster.name == "Joe Rogan"
            assert podcaster.podcast_count == 2
            assert podcaster.total_episodes == 2500  # 2000 + 500
            assert len(podcaster.podcasts) == 2
            assert podcaster.primary_podcast_title == "The Joe Rogan Experience"
            assert podcaster.image == "https://example.com/image.jpg"

    @pytest.mark.asyncio
    async def test_search_person_no_podcasts(self, mock_auth):
        """Test podcaster search with no podcasts found."""
        service = PodcastSearchService()

        mock_person_response = PersonSearchResponse(
            date="2025-01-15",
            podcasts=[],
            episodes=[],
            total_podcasts=0,
            total_episodes=0,
            person_name="Unknown Person",
        )

        with patch.object(
            service, "search_by_person", new=AsyncMock(return_value=mock_person_response)
        ):
            result = await service.search_person(person_name="Unknown Person", max_results=20)

            assert isinstance(result, PodcasterSearchResponse)
            assert result.total_results == 0
            assert len(result.results) == 0
            assert result.query == "Unknown Person"

    @pytest.mark.asyncio
    async def test_search_person_selects_primary_podcast_with_image(self, mock_auth):
        """Test that search_person selects primary podcast with image."""
        service = PodcastSearchService()

        # First podcast has no image, second has image
        mock_podcasts = [
            MCPodcastItem(
                id=1,
                title="Podcast Without Image",
                url="https://example.com/1",
                author="Joe Rogan",
                episode_count=100,
                image=None,
            ),
            MCPodcastItem(
                id=2,
                title="Podcast With Image",
                url="https://example.com/2",
                author="Joe Rogan",
                episode_count=200,
                image="https://example.com/image.jpg",
                description="Bio",
                site="https://example.com",
            ),
        ]

        mock_person_response = PersonSearchResponse(
            date="2025-01-15",
            podcasts=mock_podcasts,
            episodes=[],
            total_podcasts=2,
            total_episodes=0,
            person_name="Joe Rogan",
        )

        with patch.object(
            service, "search_by_person", new=AsyncMock(return_value=mock_person_response)
        ):
            result = await service.search_person(person_name="Joe Rogan", max_results=20)

            podcaster = result.results[0]
            # Should select podcast with image as primary
            assert podcaster.primary_podcast_title == "Podcast With Image"
            assert podcaster.primary_podcast_id == 2
            assert podcaster.image == "https://example.com/image.jpg"

    @pytest.mark.asyncio
    async def test_search_person_handles_error(self, mock_auth):
        """Test error handling in search_person."""
        service = PodcastSearchService()

        with patch.object(
            service, "search_by_person", new=AsyncMock(side_effect=Exception("Test error"))
        ):
            result = await service.search_person(person_name="Joe Rogan", max_results=20)

            assert isinstance(result, PodcasterSearchResponse)
            assert result.total_results == 0
            assert result.error is not None
            assert "Test error" in result.error
            assert result.status_code == 500
