"""
Tests for Podcast async wrapper functions.
"""

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

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
from api.podcast.wrappers import podcast_wrapper
from contracts.models import MCSources, MCSubType, MCType
from utils.pytest_utils import write_snapshot

pytestmark = pytest.mark.unit


class TestGetTrendingPodcasts:
    """Tests for podcast_wrapper.get_trending_podcasts wrapper."""

    @pytest.mark.asyncio
    async def test_get_trending_podcasts_success(self, mock_auth):
        """Test successful trending podcasts wrapper."""
        mock_podcasts = [
            MCPodcastItem(id=1, title="Podcast 1", url="https://example.com/1", source_id="1"),
            MCPodcastItem(id=2, title="Podcast 2", url="https://example.com/2", source_id="2"),
        ]
        mock_response = PodcastTrendingResponse(
            date=datetime.now(UTC).strftime("%Y-%m-%d"),
            results=mock_podcasts,
            total_results=2,
        )

        # Ensure cache is disabled for this test
        from api.podcast.wrappers import PodcastWrapperCache

        original_disable = PodcastWrapperCache.disableCache
        PodcastWrapperCache.disableCache = True

        try:
            with patch.object(
                podcast_wrapper.service, "get_trending_podcasts", new_callable=AsyncMock
            ) as mock_method:
                mock_method.return_value = mock_response

                result = await podcast_wrapper.get_trending_podcasts(max_results=10)

                assert isinstance(result, PodcastTrendingResponse)
                assert result.status_code == 200
                assert result.total_results == 2
                assert len(result.results) == 2
                assert result.error is None

                # Verify MCSearchResponse fields are present
                assert result.data_source is not None
                assert result.data_type == MCType.PODCAST

                # Verify required fields on all items
                for item in result.results:
                    assert item.mc_id is not None, f"mc_id missing for podcast: {item.title}"
                    assert item.mc_type == MCType.PODCAST, (
                        f"mc_type incorrect for podcast: {item.title}"
                    )
                    assert item.source is not None, f"source missing for podcast: {item.title}"
                    assert item.source == MCSources.PODCASTINDEX, (
                        f"source incorrect for podcast: {item.title}"
                    )
                    assert item.source_id is not None, (
                        f"source_id missing for podcast: {item.title}"
                    )

                mock_method.assert_called_once_with(max_results=10, lang="en")
                write_snapshot(
                    json.dumps(result.model_dump(), indent=4), "get_trending_podcasts_success.json"
                )
        finally:
            PodcastWrapperCache.disableCache = original_disable

    @pytest.mark.asyncio
    async def test_get_trending_podcasts_handles_error(self, mock_auth):
        """Test error handling in trending podcasts wrapper."""
        # Ensure cache is disabled for this test
        from api.podcast.wrappers import PodcastWrapperCache

        original_disable = PodcastWrapperCache.disableCache
        PodcastWrapperCache.disableCache = True

        try:
            with patch.object(
                podcast_wrapper.service, "get_trending_podcasts", new_callable=AsyncMock
            ) as mock_method:
                mock_method.side_effect = Exception("Test error")

                result = await podcast_wrapper.get_trending_podcasts(max_results=10)

                assert isinstance(result, PodcastTrendingResponse)
                assert result.status_code == 500
                assert result.error == "Test error"
                assert result.results == []
                assert result.total_results == 0
        finally:
            PodcastWrapperCache.disableCache = original_disable

    @pytest.mark.asyncio
    async def test_get_trending_podcasts_with_error_in_response(self, mock_auth):
        """Test wrapper handles error field in response."""
        mock_response = PodcastTrendingResponse(
            date=datetime.now(UTC).strftime("%Y-%m-%d"),
            results=[],
            total_results=0,
            error="Failed to fetch",
        )

        # Ensure cache is disabled for this test
        from api.podcast.wrappers import PodcastWrapperCache

        original_disable = PodcastWrapperCache.disableCache
        PodcastWrapperCache.disableCache = True

        try:
            with patch.object(
                podcast_wrapper.service, "get_trending_podcasts", new_callable=AsyncMock
            ) as mock_method:
                mock_method.return_value = mock_response

                result = await podcast_wrapper.get_trending_podcasts(max_results=10)

                assert isinstance(result, PodcastTrendingResponse)
                assert result.status_code == 500
                assert result.error == "Failed to fetch"
        finally:
            PodcastWrapperCache.disableCache = original_disable


class TestSearchPodcasts:
    """Tests for podcast_wrapper.search_podcasts wrapper."""

    @pytest.mark.asyncio
    async def test_search_podcasts_success(self, mock_auth):
        """Test successful podcast search wrapper."""
        mock_podcasts = [
            MCPodcastItem(id=1, title="True Crime", url="https://example.com/1", source_id="1"),
            MCPodcastItem(id=2, title="Crime Stories", url="https://example.com/2", source_id="2"),
        ]
        mock_response = PodcastSearchResponse(
            date=datetime.now(UTC).strftime("%Y-%m-%d"),
            results=mock_podcasts,
            total_results=2,
            query="true crime",
        )

        # Ensure cache is disabled for this test
        from api.podcast.wrappers import PodcastWrapperCache

        original_disable = PodcastWrapperCache.disableCache
        PodcastWrapperCache.disableCache = True

        try:
            with patch.object(
                podcast_wrapper.service, "search_podcasts", new_callable=AsyncMock
            ) as mock_method:
                mock_method.return_value = mock_response

                result = await podcast_wrapper.search_podcasts(query="true crime", max_results=20)

                assert isinstance(result, PodcastSearchResponse)
                assert result.status_code == 200
                assert result.total_results == 2
                assert len(result.results) == 2
                assert result.query == "true crime"
                assert result.error is None

                # Verify MCSearchResponse fields are present
                assert result.data_source is not None
                assert result.data_type == MCType.PODCAST

                # Verify required fields on all items
                for item in result.results:
                    assert item.mc_id is not None, f"mc_id missing for podcast: {item.title}"
                    assert item.mc_type == MCType.PODCAST, (
                        f"mc_type incorrect for podcast: {item.title}"
                    )
                    assert item.source is not None, f"source missing for podcast: {item.title}"
                    assert item.source == MCSources.PODCASTINDEX, (
                        f"source incorrect for podcast: {item.title}"
                    )
                    assert item.source_id is not None, (
                        f"source_id missing for podcast: {item.title}"
                    )

                mock_method.assert_called_once_with(query="true crime", max_results=20)
                write_snapshot(
                    json.dumps(result.model_dump(), indent=4), "search_podcasts_success.json"
                )
        finally:
            PodcastWrapperCache.disableCache = original_disable

    @pytest.mark.asyncio
    async def test_search_podcasts_handles_error(self, mock_auth):
        """Test error handling in podcast search wrapper."""
        with patch.object(
            podcast_wrapper.service, "search_podcasts", new_callable=AsyncMock
        ) as mock_method:
            mock_method.side_effect = Exception("Test error")

            result = await podcast_wrapper.search_podcasts(query="test", max_results=20)

            assert isinstance(result, PodcastSearchResponse)
            assert result.status_code == 500
            assert result.error == "Test error"
            assert result.results == []
            assert result.total_results == 0


class TestGetPodcastById:
    """Tests for podcast_wrapper.get_podcast_by_id wrapper."""

    @pytest.mark.asyncio
    async def test_get_podcast_by_id_success(self, mock_auth):
        """Test successful podcast by id wrapper."""
        mock_podcast = MCPodcastItem(
            id=360084,
            title="The Joe Rogan Experience",
            url="https://example.com/feed.xml",
            source_id="360084",
        )

        with patch.object(
            podcast_wrapper.service, "get_podcast_by_id", new_callable=AsyncMock
        ) as mock_method:
            mock_method.return_value = mock_podcast

            result = await podcast_wrapper.get_podcast_by_id(feed_id=360084)

            assert isinstance(result, MCPodcastItem)
            assert result.status_code == 200
            assert result.id == 360084
            assert result.title == "The Joe Rogan Experience"
            assert result.error is None

            # Verify MCBaseItem fields are present
            assert result.mc_id is not None, "mc_id missing for podcast"
            assert result.mc_type == MCType.PODCAST, "mc_type incorrect for podcast"
            assert result.source is not None, "source missing for podcast"
            assert result.source == MCSources.PODCASTINDEX, "source incorrect for podcast"
            assert result.source_id is not None, "source_id missing for podcast"

            mock_method.assert_called_once_with(feed_id=360084)
            write_snapshot(
                json.dumps(result.model_dump(), indent=4), "get_podcast_by_id_success.json"
            )

    @pytest.mark.asyncio
    async def test_get_podcast_by_id_not_found(self, mock_auth):
        """Test podcast by id wrapper when podcast not found."""
        with patch.object(
            podcast_wrapper.service, "get_podcast_by_id", new_callable=AsyncMock
        ) as mock_method:
            mock_method.return_value = None

            result = await podcast_wrapper.get_podcast_by_id(feed_id=999999999)

            assert isinstance(result, MCPodcastItem)
            assert result.status_code == 404
            assert result.error == "Podcast not found"

    @pytest.mark.asyncio
    async def test_get_podcast_by_id_handles_error(self, mock_auth):
        """Test error handling in podcast by id wrapper."""
        with patch.object(
            podcast_wrapper.service, "get_podcast_by_id", new_callable=AsyncMock
        ) as mock_method:
            mock_method.side_effect = Exception("Test error")

            result = await podcast_wrapper.get_podcast_by_id(feed_id=360084)

            assert isinstance(result, MCPodcastItem)
            assert result.status_code == 500
            assert result.error == "Test error"


class TestGetPodcastEpisodes:
    """Tests for podcast_wrapper.get_podcast_episodes wrapper."""

    @pytest.mark.asyncio
    async def test_get_podcast_episodes_success(self, mock_auth):
        """Test successful podcast episodes wrapper."""
        mock_episodes = [
            MCEpisodeItem(id=1, title="Episode 1", source_id="1"),
            MCEpisodeItem(id=2, title="Episode 2", source_id="2"),
        ]
        mock_response = EpisodeListResponse(
            date=datetime.now(UTC).strftime("%Y-%m-%d"),
            results=mock_episodes,
            total_results=2,
            feed_id=360084,
        )

        # Ensure cache is disabled for this test
        from api.podcast.wrappers import PodcastWrapperCache

        original_disable = PodcastWrapperCache.disableCache
        PodcastWrapperCache.disableCache = True

        try:
            with patch.object(
                podcast_wrapper.service, "get_podcast_episodes", new_callable=AsyncMock
            ) as mock_method:
                mock_method.return_value = mock_response

                result = await podcast_wrapper.get_podcast_episodes(feed_id=360084, max_results=10)

                assert isinstance(result, EpisodeListResponse)
                assert result.status_code == 200
                assert result.total_results == 2
                assert len(result.results) == 2
                assert result.feed_id == 360084
                assert result.error is None

                # Verify MCSearchResponse fields are present
                assert result.data_source is not None
                assert result.data_type == MCType.PODCAST_EPISODE

                # Verify required fields on all items
                for item in result.results:
                    assert item.mc_id is not None, f"mc_id missing for episode: {item.title}"
                    assert item.mc_type == MCType.PODCAST_EPISODE, (
                        f"mc_type incorrect for episode: {item.title}"
                    )
                    assert item.source is not None, f"source missing for episode: {item.title}"
                    assert item.source == MCSources.PODCASTINDEX, (
                        f"source incorrect for episode: {item.title}"
                    )
                    assert item.source_id is not None, (
                        f"source_id missing for episode: {item.title}"
                    )

                mock_method.assert_called_once_with(feed_id=360084, max_results=10, since=None)
                write_snapshot(
                    json.dumps(result.model_dump(), indent=4), "get_podcast_episodes_success.json"
                )
        finally:
            PodcastWrapperCache.disableCache = original_disable

    @pytest.mark.asyncio
    async def test_get_podcast_episodes_handles_error(self, mock_auth):
        """Test error handling in podcast episodes wrapper."""
        # Ensure cache is disabled for this test
        from api.podcast.wrappers import PodcastWrapperCache

        original_disable = PodcastWrapperCache.disableCache
        PodcastWrapperCache.disableCache = True

        try:
            with patch.object(
                podcast_wrapper.service, "get_podcast_episodes", new_callable=AsyncMock
            ) as mock_method:
                mock_method.side_effect = Exception("Test error")

                result = await podcast_wrapper.get_podcast_episodes(feed_id=360084, max_results=10)

                assert isinstance(result, EpisodeListResponse)
                assert result.status_code == 500
                assert result.error == "Test error"
                assert result.results == []
                assert result.total_results == 0
        finally:
            PodcastWrapperCache.disableCache = original_disable


class TestGetPodcastWithLatestEpisode:
    """Tests for podcast_wrapper.get_podcast_with_latest_episode wrapper."""

    @pytest.mark.asyncio
    async def test_get_podcast_with_latest_episode_success(self, mock_auth):
        """Test successful podcast with latest episode wrapper."""
        mock_episode = MCEpisodeItem(id=1, title="Latest Episode", source_id="1")
        mock_podcast = PodcastWithLatestEpisode(
            id=360084,
            title="Test Podcast",
            url="https://example.com/feed.xml",
            latest_episode=mock_episode,
            source_id="360084",
        )

        # Ensure cache is disabled for this test
        from api.podcast.wrappers import PodcastWrapperCache

        original_disable = PodcastWrapperCache.disableCache
        PodcastWrapperCache.disableCache = True

        try:
            with patch.object(
                podcast_wrapper.service,
                "get_podcast_with_latest_episode",
                new_callable=AsyncMock,
            ) as mock_method:
                mock_method.return_value = mock_podcast

                result = await podcast_wrapper.get_podcast_with_latest_episode(feed_id=360084)

                assert isinstance(result, PodcastWithLatestEpisode)
                assert result.status_code == 200
                assert result.id == 360084
                assert result.latest_episode is not None
                assert result.latest_episode.id == 1
                assert result.error is None

                # Verify MCBaseItem fields are present
                assert result.mc_id is not None, "mc_id missing for podcast"
                assert result.mc_type == MCType.PODCAST, "mc_type incorrect for podcast"
                assert result.source is not None, "source missing for podcast"
                assert result.source == MCSources.PODCASTINDEX, "source incorrect for podcast"
                assert result.source_id is not None, "source_id missing for podcast"

                # Verify latest episode has required fields if present
                if result.latest_episode:
                    assert result.latest_episode.mc_id is not None, (
                        "mc_id missing for latest episode"
                    )
                    assert result.latest_episode.mc_type == MCType.PODCAST_EPISODE, (
                        "mc_type incorrect for latest episode"
                    )
                    assert result.latest_episode.source is not None, (
                        "source missing for latest episode"
                    )
                    assert result.latest_episode.source == MCSources.PODCASTINDEX, (
                        "source incorrect for latest episode"
                    )
                    assert result.latest_episode.source_id is not None, (
                        "source_id missing for latest episode"
                    )

                mock_method.assert_called_once_with(feed_id=360084)
                write_snapshot(
                    json.dumps(result.model_dump(), indent=4),
                    "get_podcast_with_latest_episode_success.json",
                )
        finally:
            PodcastWrapperCache.disableCache = original_disable

    @pytest.mark.asyncio
    async def test_get_podcast_with_latest_episode_not_found(self, mock_auth):
        """Test podcast with latest episode wrapper when not found."""
        with patch.object(
            podcast_wrapper.service,
            "get_podcast_with_latest_episode",
            new_callable=AsyncMock,
        ) as mock_method:
            mock_method.return_value = None

            result = await podcast_wrapper.get_podcast_with_latest_episode(feed_id=999999999)

            assert isinstance(result, PodcastWithLatestEpisode)
            assert result.status_code == 404
            assert result.error == "Podcast not found"

    @pytest.mark.asyncio
    async def test_get_podcast_with_latest_episode_handles_error(self, mock_auth):
        """Test error handling in podcast with latest episode wrapper."""
        # Ensure cache is disabled for this test
        from api.podcast.wrappers import PodcastWrapperCache

        original_disable = PodcastWrapperCache.disableCache
        PodcastWrapperCache.disableCache = True

        try:
            with patch.object(
                podcast_wrapper.service,
                "get_podcast_with_latest_episode",
                new_callable=AsyncMock,
            ) as mock_method:
                mock_method.side_effect = Exception("Test error")

                result = await podcast_wrapper.get_podcast_with_latest_episode(feed_id=360084)

                assert isinstance(result, PodcastWithLatestEpisode)
                assert result.status_code == 500
                assert result.error == "Test error"
        finally:
            PodcastWrapperCache.disableCache = original_disable


class TestSearchByPerson:
    """Tests for podcast_wrapper.search_by_person wrapper."""

    @pytest.mark.asyncio
    async def test_search_by_person_success(self, mock_auth):
        """Test successful person search wrapper."""
        mock_podcasts = [
            MCPodcastItem(
                id=1,
                title="Joe Rogan Experience",
                url="https://example.com/1",
                author="Joe Rogan",
                source_id="1",
            ),
        ]
        mock_episodes = [
            MCEpisodeItem(
                id=100,
                title="Episode with Guest",
                feed_id=2,
                feed_title="Other Podcast",
                source_id="100",
            ),
        ]
        mock_response = PersonSearchResponse(
            date=datetime.now(UTC).strftime("%Y-%m-%d"),
            podcasts=mock_podcasts,
            episodes=mock_episodes,
            total_podcasts=1,
            total_episodes=1,
            person_name="Joe Rogan",
        )

        # Ensure cache is disabled for this test
        from api.podcast.wrappers import PodcastWrapperCache

        original_disable = PodcastWrapperCache.disableCache
        PodcastWrapperCache.disableCache = True

        try:
            # Patch the service method
            with patch.object(
                podcast_wrapper.service, "search_by_person", new_callable=AsyncMock
            ) as mock_method:
                mock_method.return_value = mock_response

                result = await podcast_wrapper.search_by_person(
                    person_name="Joe Rogan", max_results=20
                )

                assert isinstance(result, PersonSearchResponse)
                assert result.status_code == 200
                assert result.total_podcasts == 1
                assert result.total_episodes == 1
                assert result.person_name == "Joe Rogan"
                assert len(result.podcasts) == 1
                assert len(result.episodes) == 1
                assert result.error is None

                # Verify MCSearchResponse fields
                assert result.data_source is not None
                assert result.data_type == MCType.PODCAST

                # Verify required fields on all podcasts
                for podcast in result.podcasts:
                    assert podcast.mc_id is not None, f"mc_id missing for podcast: {podcast.title}"
                    assert podcast.mc_type == MCType.PODCAST, (
                        f"mc_type incorrect for podcast: {podcast.title}"
                    )
                    assert podcast.source is not None, (
                        f"source missing for podcast: {podcast.title}"
                    )
                    assert podcast.source == MCSources.PODCASTINDEX, (
                        f"source incorrect for podcast: {podcast.title}"
                    )
                    assert podcast.source_id is not None, (
                        f"source_id missing for podcast: {podcast.title}"
                    )

                # Verify required fields on all episodes
                for episode in result.episodes:
                    assert episode.mc_id is not None, f"mc_id missing for episode: {episode.title}"
                    assert episode.mc_type == MCType.PODCAST_EPISODE, (
                        f"mc_type incorrect for episode: {episode.title}"
                    )
                    assert episode.source is not None, (
                        f"source missing for episode: {episode.title}"
                    )
                    assert episode.source == MCSources.PODCASTINDEX, (
                        f"source incorrect for episode: {episode.title}"
                    )
                    assert episode.source_id is not None, (
                        f"source_id missing for episode: {episode.title}"
                    )

                mock_method.assert_called_once_with(person_name="Joe Rogan", max_results=20)
                write_snapshot(
                    json.dumps(result.model_dump(), indent=4), "search_by_person_success.json"
                )
        finally:
            PodcastWrapperCache.disableCache = original_disable

    @pytest.mark.asyncio
    async def test_search_by_person_handles_error(self, mock_auth):
        """Test error handling in person search wrapper."""
        with patch.object(
            podcast_wrapper.service, "search_by_person", new_callable=AsyncMock
        ) as mock_method:
            mock_method.side_effect = Exception("Test error")

            result = await podcast_wrapper.search_by_person(
                person_name="Test Person", max_results=20
            )

            assert isinstance(result, PersonSearchResponse)
            assert result.status_code == 500
            assert result.error == "Test error"
            assert result.podcasts == []
            assert result.episodes == []
            assert result.total_podcasts == 0
            assert result.total_episodes == 0


class TestSearchPerson:
    """Tests for podcast_wrapper.search_person wrapper."""

    @pytest.mark.asyncio
    async def test_search_person_success(self, mock_auth):
        """Test successful podcaster search wrapper."""
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
                source_id="360084",
            ),
            MCPodcastItem(
                id=500000,
                title="JRE Clips",
                url="https://example.com/clips.xml",
                author="Joe Rogan",
                episode_count=500,
                source_id="500000",
            ),
        ]

        mock_response = PodcasterSearchResponse(
            date=datetime.now(UTC).strftime("%Y-%m-%d"),
            results=[
                MCPodcaster(
                    name="Joe Rogan",
                    id="joe_rogan",
                    podcasts=mock_podcasts,
                    total_episodes=2500,
                    podcast_count=2,
                    image="https://example.com/image.jpg",
                    bio="A podcast",
                    website="https://joerogan.com",
                    primary_podcast_title="The Joe Rogan Experience",
                    primary_podcast_id=360084,
                    source_id="joe_rogan",
                )
            ],
            total_results=1,
            query="Joe Rogan",
        )

        # Ensure cache is disabled for this test
        from api.podcast.wrappers import PodcastWrapperCache

        original_disable = PodcastWrapperCache.disableCache
        PodcastWrapperCache.disableCache = True

        try:
            # Patch the service method
            with patch.object(
                podcast_wrapper.service, "search_person", new_callable=AsyncMock
            ) as mock_method:
                mock_method.return_value = mock_response

                result = await podcast_wrapper.search_person(
                    person_name="Joe Rogan", max_results=20
                )

                assert isinstance(result, PodcasterSearchResponse)
                assert result.status_code == 200
                assert result.total_results == 1
                assert result.query == "Joe Rogan"
                assert len(result.results) == 1
                assert result.error is None

                # Verify MCSearchResponse fields
                assert result.data_source is not None
                assert result.data_type == MCType.PERSON

                podcaster = result.results[0]
                assert isinstance(podcaster, MCPodcaster)
                assert podcaster.name == "Joe Rogan"
                assert podcaster.podcast_count == 2
                assert podcaster.total_episodes == 2500
                assert podcaster.mc_type == MCType.PERSON
                assert podcaster.mc_subtype == MCSubType.PODCASTER

                # Verify required fields on podcaster
                assert podcaster.mc_id is not None, "mc_id missing for podcaster"
                assert podcaster.source is not None, "source missing for podcaster"
                assert podcaster.source == MCSources.PODCASTINDEX, "source incorrect for podcaster"
                assert podcaster.source_id is not None, "source_id missing for podcaster"

                # Verify required fields on all podcasts in podcaster
                for podcast in podcaster.podcasts:
                    assert podcast.mc_id is not None, f"mc_id missing for podcast: {podcast.title}"
                    assert podcast.mc_type == MCType.PODCAST, (
                        f"mc_type incorrect for podcast: {podcast.title}"
                    )
                    assert podcast.source is not None, (
                        f"source missing for podcast: {podcast.title}"
                    )
                    assert podcast.source == MCSources.PODCASTINDEX, (
                        f"source incorrect for podcast: {podcast.title}"
                    )
                    assert podcast.source_id is not None, (
                        f"source_id missing for podcast: {podcast.title}"
                    )

                mock_method.assert_called_once_with(person_name="Joe Rogan", max_results=20)
                write_snapshot(
                    json.dumps(result.model_dump(), indent=4), "search_person_success.json"
                )
        finally:
            PodcastWrapperCache.disableCache = original_disable

    @pytest.mark.asyncio
    async def test_search_person_handles_error(self, mock_auth):
        """Test error handling in podcaster search wrapper."""
        # Ensure cache is disabled for this test
        from api.podcast.wrappers import PodcastWrapperCache

        original_disable = PodcastWrapperCache.disableCache
        PodcastWrapperCache.disableCache = True

        try:
            with patch.object(
                podcast_wrapper.service, "search_person", new_callable=AsyncMock
            ) as mock_method:
                mock_method.side_effect = Exception("Test error")

                result = await podcast_wrapper.search_person(
                    person_name="Test Person", max_results=20
                )

                assert isinstance(result, PodcasterSearchResponse)
                assert result.status_code == 500
                assert result.error == "Test error"
                assert result.results == []
                assert result.total_results == 0
        finally:
            PodcastWrapperCache.disableCache = original_disable


class TestWrapperReturnFormat:
    """Tests for wrapper return format consistency."""

    @pytest.mark.asyncio
    async def test_all_wrappers_return_mcsearchresponse(self, mock_auth):
        """Test that search/list wrappers return MCSearchResponse format."""
        with (
            patch.object(
                podcast_wrapper.service, "get_trending_podcasts", new_callable=AsyncMock
            ) as mock_trending,
            patch.object(
                podcast_wrapper.service, "search_podcasts", new_callable=AsyncMock
            ) as mock_search,
            patch.object(
                podcast_wrapper.service, "get_podcast_by_id", new_callable=AsyncMock
            ) as mock_by_id,
            patch.object(
                podcast_wrapper.service, "get_podcast_episodes", new_callable=AsyncMock
            ) as mock_episodes,
            patch.object(
                podcast_wrapper.service,
                "get_podcast_with_latest_episode",
                new_callable=AsyncMock,
            ) as mock_latest,
        ):
            mock_trending.return_value = PodcastTrendingResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                results=[],
                total_results=0,
            )
            mock_search.return_value = PodcastSearchResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                results=[],
                total_results=0,
                query="test",
            )
            mock_by_id.return_value = None
            mock_episodes.return_value = EpisodeListResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                results=[],
                total_results=0,
                feed_id=1,
            )
            mock_latest.return_value = None

            # Test all wrappers
            result1 = await podcast_wrapper.get_trending_podcasts()
            result2 = await podcast_wrapper.search_podcasts(query="test")
            result3 = await podcast_wrapper.get_podcast_by_id(feed_id=1)
            result4 = await podcast_wrapper.get_podcast_episodes(feed_id=1)
            result5 = await podcast_wrapper.get_podcast_with_latest_episode(feed_id=1)

            # All should return MCSearchResponse derivatives (except get_podcast_by_id and get_podcast_with_latest_episode)
            assert isinstance(result1, PodcastTrendingResponse)
            assert isinstance(result2, PodcastSearchResponse)
            assert isinstance(
                result3, MCPodcastItem
            )  # This is still MCBaseItem (single item, not search)
            assert isinstance(result4, EpisodeListResponse)
            assert isinstance(
                result5, PodcastWithLatestEpisode
            )  # This is still MCBaseItem (single item, not search)

            # Search/list responses should have MCSearchResponse fields
            assert hasattr(result1, "status_code")
            assert hasattr(result1, "results")
            assert hasattr(result1, "total_results")
            assert hasattr(result1, "data_source")
            assert hasattr(result2, "status_code")
            assert hasattr(result2, "results")
            assert hasattr(result2, "total_results")
            assert hasattr(result2, "data_source")
            assert hasattr(result4, "status_code")
            assert hasattr(result4, "results")
            assert hasattr(result4, "total_results")
            assert hasattr(result4, "data_source")
            # Single item responses still have MCBaseItem fields
            assert hasattr(result3, "status_code")
            assert hasattr(result3, "mc_id")
            assert hasattr(result5, "status_code")
            assert hasattr(result5, "mc_id")
