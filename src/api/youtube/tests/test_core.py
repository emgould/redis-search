"""
Unit tests for YouTube Core Service.
Tests YouTubeService base class functionality.
"""

from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from api.youtube.core import YouTubeService
from api.youtube.tests.conftest import load_fixture
from contracts.models import MCType

FIXTURES_DIR = Path(__file__).parent / "fixtures"

pytestmark = pytest.mark.unit


class TestYouTubeService:
    """Tests for YouTubeService class."""

    @patch("api.youtube.core.YouTubeService.youtube_api_key", new_callable=PropertyMock)
    def test_service_initialization(self, mock_prop):
        """Test service initialization with API key."""
        mock_prop.return_value = "test_youtube_api_key_12345"
        service = YouTubeService()

        assert service.youtube_api_key == "test_youtube_api_key_12345"
        assert service.youtube is not None

    @patch("api.youtube.core.YouTubeService.youtube_api_key", new_callable=PropertyMock)
    @patch("api.youtube.auth.build")
    def test_service_initialization_without_api_key(self, mock_build, mock_prop):
        """Test that accessing youtube property fails without API key (lazy loading)."""
        mock_prop.return_value = None
        # Mock build to raise ValueError when developerKey is None
        mock_build.side_effect = (
            lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("YouTube API key is required"))
            if kwargs.get("developerKey") is None
            else MagicMock()
        )
        service = YouTubeService()

        # Service initialization succeeds (lazy loading)
        assert service is not None

        # But accessing youtube property should fail when API key is None
        with pytest.raises(ValueError, match="YouTube API key is required"):
            _ = service.youtube


class TestProcessVideoItem:
    """Tests for _process_video_item method."""

    @patch("api.youtube.core.YouTubeService.youtube_api_key", new_callable=PropertyMock)
    @patch("api.youtube.auth.build")
    def test_process_video_item_with_full_data(self, mock_build, mock_prop):
        """Test processing video item with complete data."""
        mock_prop.return_value = "test_youtube_api_key_12345"
        mock_build.return_value = MagicMock()
        service = YouTubeService()

        video_data = {
            "id": "test_video_id",
            "snippet": {
                "title": "Test Video",
                "description": "Test Description",
                "channelTitle": "Test Channel",
                "channelId": "test_channel_id",
                "publishedAt": "2024-01-01T00:00:00Z",
                "thumbnails": {
                    "high": {"url": "https://example.com/thumbnail.jpg"},
                },
                "tags": ["test", "video"],
                "categoryId": "28",
                "defaultLanguage": "en",
            },
            "statistics": {
                "viewCount": "1000",
                "likeCount": "100",
                "commentCount": "10",
            },
            "contentDetails": {
                "duration": "PT5M30S",
            },
        }

        result = service._process_video_item(video_data)

        assert result.id == "test_video_id"
        assert result.video_id == "test_video_id"
        assert result.title == "Test Video"
        assert result.description == "Test Description"
        assert result.channel_title == "Test Channel"
        assert result.channel_id == "test_channel_id"
        assert result.view_count == 1000
        assert result.like_count == 100
        assert result.comment_count == 10
        assert result.duration == "PT5M30S"
        assert result.mc_id is not None
        assert result.mc_type == "youtube_video"

    @patch("api.youtube.core.YouTubeService.youtube_api_key", new_callable=PropertyMock)
    @patch("api.youtube.auth.build")
    def test_process_video_item_selects_best_thumbnail(self, mock_build, mock_prop):
        """Test that maxres thumbnail is selected when available."""
        mock_prop.return_value = "test_youtube_api_key_12345"
        mock_build.return_value = MagicMock()
        service = YouTubeService()

        video_data = {
            "id": "test_video_id",
            "snippet": {
                "title": "Test Video",
                "thumbnails": {
                    "default": {"url": "https://example.com/default.jpg"},
                    "medium": {"url": "https://example.com/medium.jpg"},
                    "high": {"url": "https://example.com/high.jpg"},
                    "maxres": {"url": "https://example.com/maxres.jpg"},
                },
            },
            "statistics": {},
        }

        result = service._process_video_item(video_data)

        assert result.thumbnail_url == "https://example.com/maxres.jpg"

    @patch("api.youtube.core.YouTubeService.youtube_api_key", new_callable=PropertyMock)
    @patch("api.youtube.auth.build")
    def test_process_video_item_fallback_to_high_thumbnail(self, mock_build, mock_prop):
        """Test fallback to high quality thumbnail if maxres not available."""
        mock_prop.return_value = "test_youtube_api_key_12345"
        mock_build.return_value = MagicMock()
        service = YouTubeService()

        video_data = {
            "id": "test_video_id",
            "snippet": {
                "title": "Test Video",
                "thumbnails": {
                    "default": {"url": "https://example.com/default.jpg"},
                    "medium": {"url": "https://example.com/medium.jpg"},
                    "high": {"url": "https://example.com/high.jpg"},
                },
            },
            "statistics": {},
        }

        result = service._process_video_item(video_data)

        assert result.thumbnail_url == "https://example.com/high.jpg"

    @patch("api.youtube.core.YouTubeService.youtube_api_key", new_callable=PropertyMock)
    @patch("api.youtube.auth.build")
    def test_process_video_item_handles_missing_statistics(self, mock_build, mock_prop):
        """Test handling of missing statistics fields."""
        mock_prop.return_value = "test_youtube_api_key_12345"
        mock_build.return_value = MagicMock()
        service = YouTubeService()

        video_data = {
            "id": "test_video_id",
            "snippet": {
                "title": "Test Video",
            },
            "statistics": {},
        }

        result = service._process_video_item(video_data)

        assert result.view_count == 0
        assert result.like_count == 0
        assert result.comment_count == 0

    @patch("api.youtube.core.YouTubeService.youtube_api_key", new_callable=PropertyMock)
    @patch("api.youtube.auth.build")
    def test_process_video_item_handles_missing_tags(self, mock_build, mock_prop):
        """Test handling of missing tags field."""
        mock_prop.return_value = "test_youtube_api_key_12345"
        mock_build.return_value = MagicMock()
        service = YouTubeService()

        video_data = {
            "id": "test_video_id",
            "snippet": {
                "title": "Test Video",
            },
            "statistics": {},
        }

        result = service._process_video_item(video_data)

        assert result.tags == []

    @patch("api.youtube.core.YouTubeService.youtube_api_key", new_callable=PropertyMock)
    @patch("api.youtube.auth.build")
    def test_process_video_item_generates_url(self, mock_build, mock_prop):
        """Test that video URL is generated correctly."""
        mock_prop.return_value = "test_youtube_api_key_12345"
        mock_build.return_value = MagicMock()
        service = YouTubeService()

        video_data = {
            "id": "abc123xyz",
            "snippet": {
                "title": "Test Video",
            },
            "statistics": {},
        }

        result = service._process_video_item(video_data)

        assert result.url == "https://www.youtube.com/watch?v=abc123xyz"

    @patch("api.youtube.core.YouTubeService.youtube_api_key", new_callable=PropertyMock)
    @patch("api.youtube.auth.build")
    def test_process_video_item_handles_missing_snippet(self, mock_build, mock_prop):
        """Test handling of missing snippet in video data."""
        mock_prop.return_value = "test_youtube_api_key_12345"
        mock_build.return_value = MagicMock()
        service = YouTubeService()

        # Minimal data with missing snippet
        video_data = {"id": "test_id"}

        result = service._process_video_item(video_data)

        # Should still process without error, using defaults
        assert result.id == "test_id"
        assert result.video_id == "test_id"
        assert result.title == ""
        assert result.view_count == 0


class TestGetTrendingVideos:
    """Tests for get_trending_videos method."""

    @pytest.mark.asyncio
    @patch("api.youtube.core.YouTubeService.youtube_api_key", new_callable=PropertyMock)
    async def test_get_trending_videos_with_mock(self, mock_prop):
        """Test getting trending videos with mocked YouTube API."""
        mock_prop.return_value = "test_youtube_api_key_12345"
        # Mock search response (get_trending_videos now uses search_videos internally)
        mock_search_response = {
            "items": [
                {
                    "id": {"videoId": "video1"},
                }
            ],
        }

        # Mock videos response with detailed video information
        mock_videos_response = {
            "items": [
                {
                    "id": "video1",
                    "snippet": {
                        "title": "Trending Video 1",
                        "description": "Description 1",
                        "channelTitle": "Channel 1",
                        "channelId": "channel1",
                        "publishedAt": "2024-01-01T00:00:00Z",
                        "thumbnails": {"high": {"url": "https://example.com/thumb1.jpg"}},
                    },
                    "statistics": {
                        "viewCount": "1000000",  # Must be >= 20000 to pass filter
                        "likeCount": "50000",
                        "commentCount": "1000",
                    },
                    "contentDetails": {"duration": "PT10M30S"},
                }
            ],
        }

        with patch("api.youtube.auth.build") as mock_build:
            mock_youtube = MagicMock()
            mock_search = MagicMock()
            mock_videos = MagicMock()

            # Mock search API
            mock_search_request = MagicMock()
            mock_search_request.execute.return_value = mock_search_response
            mock_search.list.return_value = mock_search_request
            mock_youtube.search.return_value = mock_search

            # Mock videos API
            mock_videos_request = MagicMock()
            mock_videos_request.execute.return_value = mock_videos_response
            mock_videos.list.return_value = mock_videos_request
            mock_youtube.videos.return_value = mock_videos

            mock_build.return_value = mock_youtube

            service = YouTubeService()
            result = await service.get_trending_videos(region_code="US", language="en")

            assert len(result.videos) == 1
            assert result.videos[0].title == "Trending Video 1"
            assert result.region_code == "US"
            assert result.language == "en"
            assert result.error is None


class TestSearchVideos:
    """Tests for search_videos method."""

    @pytest.mark.asyncio
    @patch("api.youtube.core.YouTubeService.youtube_api_key", new_callable=PropertyMock)
    async def test_search_videos_with_mock(self, mock_prop):
        """Test searching videos with mocked YouTube API."""
        mock_prop.return_value = "test_youtube_api_key_12345"
        # Mock search response
        mock_search_response = {
            "items": [
                {
                    "id": {"videoId": "video1"},
                }
            ],
        }

        # Mock videos response
        mock_videos_response = {
            "items": [
                {
                    "id": "video1",
                    "snippet": {
                        "title": "Search Result 1",
                        "description": "Description 1",
                        "channelTitle": "Channel 1",
                        "channelId": "channel1",
                        "publishedAt": "2024-01-01T00:00:00Z",
                        "thumbnails": {"high": {"url": "https://example.com/thumb1.jpg"}},
                    },
                    "statistics": {
                        "viewCount": "1000",
                        "likeCount": "100",
                        "commentCount": "10",
                    },
                    "contentDetails": {"duration": "PT5M30S"},
                }
            ],
        }

        with patch("api.youtube.auth.build") as mock_build:
            mock_youtube = MagicMock()
            mock_search = MagicMock()
            mock_videos = MagicMock()

            mock_search_request = MagicMock()
            mock_search_request.execute.return_value = mock_search_response
            mock_search.list.return_value = mock_search_request

            mock_videos_request = MagicMock()
            mock_videos_request.execute.return_value = mock_videos_response
            mock_videos.list.return_value = mock_videos_request

            mock_youtube.search.return_value = mock_search
            mock_youtube.videos.return_value = mock_videos
            mock_build.return_value = mock_youtube

            service = YouTubeService()
            result = await service.search_videos(query="Python programming")

            assert len(result.results) == 1
            assert result.results[0].title == "Search Result 1"
            assert result.query == "Python programming"
            assert result.error is None

    @pytest.mark.asyncio
    @patch("api.youtube.core.YouTubeService.youtube_api_key", new_callable=PropertyMock)
    async def test_search_videos_handles_empty_results(self, mock_prop):
        """Test search with no results."""
        mock_prop.return_value = "test_youtube_api_key_12345"
        mock_search_response = {"items": []}

        with patch("api.youtube.auth.build") as mock_build:
            mock_youtube = MagicMock()
            mock_search = MagicMock()

            mock_search_request = MagicMock()
            mock_search_request.execute.return_value = mock_search_response
            mock_search.list.return_value = mock_search_request

            mock_youtube.search.return_value = mock_search
            mock_build.return_value = mock_youtube

            service = YouTubeService()
            result = await service.search_videos(query="nonexistent query")

            assert result.results == []
            assert result.total_results == 0
            assert result.query == "nonexistent query"
            assert result.error is None


class TestSearchVideosAsync:
    """Tests for search_videos_async method."""

    @pytest.mark.asyncio
    @patch("api.youtube.core.YouTubeService.youtube_api_key", new_callable=PropertyMock)
    @patch("api.youtube.core.search_videos_async")
    async def test_search_videos_async_with_mock(self, mock_search_async, mock_prop):
        """Test async video search with mocked dynamic search."""
        mock_prop.return_value = "test_youtube_api_key_12345"

        # Mock the dynamic search response
        from api.youtube.models import YouTubeVideo
        mock_videos = [
            YouTubeVideo(
                id="video1",
                video_id="video1",
                title="Async Search Result 1",
                url="https://www.youtube.com/watch?v=video1",
                description="Test description",
                view_count=1000,
            )
        ]
        mock_search_async.return_value = mock_videos

        with patch("api.youtube.auth.build") as mock_build:
            mock_build.return_value = MagicMock()
            service = YouTubeService()
            result = await service.search_videos_async(query="test query", max_results=10)

            assert len(result.results) == 1
            assert result.results[0].title == "Async Search Result 1"
            assert result.query == "test query"
            assert result.total_results == 1
            assert result.error is None

    @pytest.mark.asyncio
    @patch("api.youtube.core.YouTubeService.youtube_api_key", new_callable=PropertyMock)
    @patch("api.youtube.core.search_videos_async")
    async def test_search_videos_async_handles_empty_results(self, mock_search_async, mock_prop):
        """Test async search with no results."""
        mock_prop.return_value = "test_youtube_api_key_12345"
        mock_search_async.return_value = []

        with patch("api.youtube.auth.build") as mock_build:
            mock_build.return_value = MagicMock()
            service = YouTubeService()
            result = await service.search_videos_async(query="nonexistent", max_results=10)

            assert result.results == []
            assert result.total_results == 0
            assert result.query == "nonexistent"


class TestGetPersonDetails:
    """Tests for get_person_details method."""

    @pytest.mark.asyncio
    @patch("api.youtube.core.YouTubeService.youtube_api_key", new_callable=PropertyMock)
    @patch("api.youtube.core.get_person_details")
    async def test_get_person_details_success(self, mock_get_person, mock_prop):
        """Test successful person details fetch."""
        mock_prop.return_value = "test_youtube_api_key_12345"

        # Mock the dynamic get_person_details response
        from api.youtube.models import YouTubeCreator
        mock_creator = YouTubeCreator(
            id="UC_test_channel_id",
            title="Test Channel",
            url="https://www.youtube.com/channel/UC_test_channel_id",
            subscriber_count=1000000,
            video_count=500,
            description="Test channel description",
        )
        mock_get_person.return_value = mock_creator

        with patch("api.youtube.auth.build") as mock_build:
            mock_build.return_value = MagicMock()
            service = YouTubeService()
            result = await service.get_person_details(source_id="UC_test_channel_id", limit=10)

            assert result.total_results == 1
            assert len(result.results) == 1
            assert result.results[0].title == "Test Channel"
            assert result.results[0].id == "UC_test_channel_id"
            assert result.data_type == MCType.PERSON
            assert result.error is None

    @pytest.mark.asyncio
    @patch("api.youtube.core.YouTubeService.youtube_api_key", new_callable=PropertyMock)
    @patch("api.youtube.core.get_person_details")
    async def test_get_person_details_with_error(self, mock_get_person, mock_prop):
        """Test person details fetch with error from dynamic module."""
        mock_prop.return_value = "test_youtube_api_key_12345"

        # Mock a creator with error field set (as dynamic.py does)
        from api.youtube.models import YouTubeCreator
        mock_creator_with_error = YouTubeCreator(
            id="UC_invalid_id",
            title="UC_invalid_id",
            url="https://www.youtube.com/channel/UC_invalid_id",
            description="Error fetching details for UC_invalid_id",
            error="API error",
        )
        mock_get_person.return_value = mock_creator_with_error

        with patch("api.youtube.auth.build") as mock_build:
            mock_build.return_value = MagicMock()
            service = YouTubeService()
            result = await service.get_person_details(source_id="UC_invalid_id", limit=10)

            # Should return a response with the error creator
            assert result.total_results == 1
            assert len(result.results) == 1
            assert result.results[0].error == "API error"


class TestSearchPeople:
    """Tests for search_people method."""

    @pytest.mark.asyncio
    @patch("api.youtube.core.YouTubeService.youtube_api_key", new_callable=PropertyMock)
    @patch("api.youtube.core.get_person")
    async def test_search_people_success(self, mock_get_person, mock_prop):
        """Test successful people search."""
        mock_prop.return_value = "test_youtube_api_key_12345"

        # Mock the dynamic get_person response
        from api.youtube.models import YouTubeCreator
        mock_creators = [
            YouTubeCreator(
                id="UC_channel_1",
                title="Test Creator 1",
                url="https://www.youtube.com/channel/UC_channel_1",
                subscriber_count=500000,
                video_count=100,
            ),
            YouTubeCreator(
                id="UC_channel_2",
                title="Test Creator 2",
                url="https://www.youtube.com/channel/UC_channel_2",
                subscriber_count=250000,
                video_count=50,
            ),
        ]
        mock_get_person.return_value = mock_creators

        with patch("api.youtube.auth.build") as mock_build:
            mock_build.return_value = MagicMock()
            service = YouTubeService()
            result = await service.search_people(query="test creator", limit=10)

            assert result.total_results == 2
            assert len(result.results) == 2
            assert result.results[0].title == "Test Creator 1"
            assert result.results[1].title == "Test Creator 2"
            assert result.query == "test creator"
            assert result.error is None

    @pytest.mark.asyncio
    @patch("api.youtube.core.YouTubeService.youtube_api_key", new_callable=PropertyMock)
    @patch("api.youtube.core.get_person")
    async def test_search_people_empty_results(self, mock_get_person, mock_prop):
        """Test people search with no results."""
        mock_prop.return_value = "test_youtube_api_key_12345"
        mock_get_person.return_value = []

        with patch("api.youtube.auth.build") as mock_build:
            mock_build.return_value = MagicMock()
            service = YouTubeService()
            result = await service.search_people(query="nonexistent creator", limit=10)

            assert result.results == []
            assert result.total_results == 0
            assert result.query == "nonexistent creator"
            assert result.error is None

    @pytest.mark.asyncio
    @patch("api.youtube.core.YouTubeService.youtube_api_key", new_callable=PropertyMock)
    @patch("api.youtube.core.get_person")
    async def test_search_people_with_exception(self, mock_get_person, mock_prop):
        """Test people search with exception."""
        mock_prop.return_value = "test_youtube_api_key_12345"
        mock_get_person.side_effect = Exception("Search error")

        with patch("api.youtube.auth.build") as mock_build:
            mock_build.return_value = MagicMock()
            service = YouTubeService()
            result = await service.search_people(query="test", limit=10)

            assert result.results == []
            assert result.total_results == 0
            assert result.status_code == 500
            assert "Search error" in result.error


class TestGetVideoCategories:
    """Tests for get_video_categories method."""

    @pytest.mark.asyncio
    @patch("api.youtube.core.YouTubeService.youtube_api_key", new_callable=PropertyMock)
    async def test_get_video_categories_with_mock(self, mock_prop):
        """Test getting video categories with mocked YouTube API."""
        mock_prop.return_value = "test_youtube_api_key_12345"
        mock_response = {
            "items": [
                {
                    "id": "28",
                    "snippet": {
                        "title": "Science & Technology",
                        "assignable": True,
                    },
                },
                {
                    "id": "10",
                    "snippet": {
                        "title": "Music",
                        "assignable": True,
                    },
                },
            ],
        }

        with patch("api.youtube.auth.build") as mock_build:
            mock_youtube = MagicMock()
            mock_categories = MagicMock()

            mock_request = MagicMock()
            mock_request.execute.return_value = mock_response
            mock_categories.list.return_value = mock_request

            mock_youtube.videoCategories.return_value = mock_categories
            mock_build.return_value = mock_youtube

            service = YouTubeService()
            result = await service.get_video_categories(region_code="US")

            assert len(result.categories) == 2
            assert result.categories[0].title == "Science & Technology"
            assert result.categories[0].assignable is True
            assert result.region_code == "US"
            assert result.error is None
