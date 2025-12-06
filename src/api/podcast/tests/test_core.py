"""
Unit tests for Podcast Core Service.
Tests PodcastService base class functionality.
"""

from unittest.mock import PropertyMock, patch

import pytest

from api.podcast.core import PodcastService
from api.podcast.models import MCEpisodeItem, MCPodcastItem

pytestmark = pytest.mark.unit


@pytest.fixture
def mock_podcast_data():
    """Mock podcast data from PodcastIndex."""
    return {
        "id": 360084,
        "title": "The Joe Rogan Experience",
        "url": "https://example.com/feed.xml",
        "site": "https://example.com",
        "description": "Test podcast description",
        "author": "Joe Rogan",
        "owner_name": "Joe Rogan",
        "image": "https://example.com/image.jpg",
        "artwork": "https://example.com/artwork.jpg",
        "last_update_time": "2024-01-01T00:00:00",
        "trend_score": 85.5,
        "language": "en",
        "categories": {"1": "Comedy", "2": "Society & Culture"},
        "episode_count": 2000,
        "itunes_id": 123456,
        "podcast_guid": "test-guid-123",
    }


@pytest.fixture
def mock_episode_data():
    """Mock episode data from PodcastIndex."""
    return {
        "id": 12345,
        "title": "Test Episode",
        "description": "Test episode description",
        "link": "https://example.com/episode",
        "guid": "episode-guid-123",
        "date_published": "2024-01-01T00:00:00",
        "enclosure_url": "https://example.com/audio.mp3",
        "enclosure_type": "audio/mpeg",
        "enclosure_length": 50000000,
        "duration_seconds": 3600,
        "explicit": False,
        "episode_type": "full",
        "season": 1,
        "episode": 1,
        "feed_id": 360084,
        "feed_title": "The Joe Rogan Experience",
        "image": "https://example.com/episode-image.jpg",
    }


class TestPodcastService:
    """Tests for PodcastService class."""

    def test_service_initialization(self, mock_auth, mock_podcast_api_key, mock_podcast_api_secret):
        """Test service initialization with API credentials."""
        service = PodcastService()

        assert service.podcast_api_key == mock_podcast_api_key
        assert service.podcast_api_secret == mock_podcast_api_secret
        assert service.base_url == "https://api.podcastindex.org/api/1.0"


class TestSafeEpisodeCount:
    """Tests for _safe_episode_count method."""

    def test_safe_episode_count_with_valid_int(self, mock_auth):
        """Test safe episode count with valid integer."""
        service = PodcastService()
        result = service._safe_episode_count(100)

        assert result == 100

    def test_safe_episode_count_with_string(self, mock_auth):
        """Test safe episode count with string number."""
        service = PodcastService()
        result = service._safe_episode_count("150")

        assert result == 150

    def test_safe_episode_count_with_negative(self, mock_auth):
        """Test safe episode count with negative number returns 0."""
        service = PodcastService()
        result = service._safe_episode_count(-10)

        assert result == 0

    def test_safe_episode_count_with_none(self, mock_auth):
        """Test safe episode count with None."""
        service = PodcastService()
        result = service._safe_episode_count(None)

        assert result == 0

    def test_safe_episode_count_with_invalid_string(self, mock_auth):
        """Test safe episode count with invalid string."""
        service = PodcastService()
        result = service._safe_episode_count("not_a_number")

        assert result == 0


class TestHasValidImage:
    """Tests for _has_valid_image method."""

    def test_has_valid_image_with_image_url(self, mock_auth):
        """Test valid image detection with image field."""
        service = PodcastService()
        podcast_data = {
            "image": "https://example.com/image.jpg",
            "artwork": None,
        }

        assert service._has_valid_image(podcast_data) is True

    def test_has_valid_image_with_artwork_url(self, mock_auth):
        """Test valid image detection with artwork field."""
        service = PodcastService()
        podcast_data = {
            "image": None,
            "artwork": "https://example.com/artwork.jpg",
        }

        assert service._has_valid_image(podcast_data) is True

    def test_has_valid_image_with_both_urls(self, mock_auth):
        """Test valid image detection with both fields."""
        service = PodcastService()
        podcast_data = {
            "image": "https://example.com/image.jpg",
            "artwork": "https://example.com/artwork.jpg",
        }

        assert service._has_valid_image(podcast_data) is True

    def test_has_valid_image_with_no_urls(self, mock_auth):
        """Test invalid image detection with no URLs."""
        service = PodcastService()
        podcast_data = {
            "image": None,
            "artwork": None,
        }

        assert service._has_valid_image(podcast_data) is False

    def test_has_valid_image_with_empty_strings(self, mock_auth):
        """Test invalid image detection with empty strings."""
        service = PodcastService()
        podcast_data = {
            "image": "",
            "artwork": "",
        }

        assert service._has_valid_image(podcast_data) is False

    def test_has_valid_image_with_short_url(self, mock_auth):
        """Test invalid image detection with too short URL."""
        service = PodcastService()
        podcast_data = {
            "image": "http://a",
            "artwork": None,
        }

        assert service._has_valid_image(podcast_data) is False

    def test_has_valid_image_with_podcast_result(self, mock_auth):
        """Test valid image detection with MCPodcastItem object."""
        service = PodcastService()
        podcast = MCPodcastItem(
            id=123,
            title="Test",
            url="https://feed.com",
            image="https://example.com/image.jpg",
        )

        assert service._has_valid_image(podcast) is True


class TestProcessPodcastItem:
    """Tests for _process_podcast_item method."""

    def test_process_podcast_item_with_full_data(self, mock_auth, mock_podcast_data):
        """Test processing podcast item with complete data."""
        service = PodcastService()
        result = service._process_podcast_item(mock_podcast_data)

        assert isinstance(result, MCPodcastItem)
        assert result.id == mock_podcast_data["id"]
        assert result.title == mock_podcast_data["title"]
        assert result.url == mock_podcast_data["url"]
        assert result.author == mock_podcast_data["author"]
        assert result.image == mock_podcast_data["image"]
        assert result.mc_id is not None
        assert result.mc_type == "podcast"

    def test_process_podcast_item_generates_mc_id(self, mock_auth):
        """Test that mc_id is auto-generated."""
        service = PodcastService()
        podcast_data = {
            "id": 12345,
            "title": "Test Podcast",
            "url": "https://example.com/feed.xml",
        }

        result = service._process_podcast_item(podcast_data)

        assert result.mc_id is not None
        assert "podcast_" in result.mc_id
        assert result.mc_type == "podcast"

    def test_process_podcast_item_with_minimal_data(self, mock_auth):
        """Test processing podcast with minimal required fields."""
        service = PodcastService()
        podcast_data = {
            "id": 12345,
            "title": "Minimal Podcast",
            "url": "https://example.com/feed.xml",
        }

        result = service._process_podcast_item(podcast_data)

        assert result.id == 12345
        assert result.title == "Minimal Podcast"
        assert result.url == "https://example.com/feed.xml"
        assert result.episode_count == 0  # Default value


class TestProcessEpisodeItem:
    """Tests for _process_episode_item method."""

    def test_process_episode_item_with_full_data(self, mock_auth, mock_episode_data):
        """Test processing episode item with complete data."""
        service = PodcastService()
        result = service._process_episode_item(mock_episode_data)

        assert isinstance(result, MCEpisodeItem)
        assert result.id == mock_episode_data["id"]
        assert result.title == mock_episode_data["title"]
        assert result.description == mock_episode_data["description"]
        assert result.enclosure_url == mock_episode_data["enclosure_url"]
        assert result.duration_seconds == mock_episode_data["duration_seconds"]
        assert result.mc_id is not None
        assert result.mc_type == "podcast_episode"

    def test_process_episode_item_generates_mc_id(self, mock_auth):
        """Test that mc_id is auto-generated for episodes."""
        service = PodcastService()
        episode_data = {
            "id": 54321,
            "title": "Test Episode",
        }

        result = service._process_episode_item(episode_data)

        assert result.mc_id is not None
        assert result.mc_id == "episode_54321"  # Format is "episode_{id}"
        assert result.mc_type == "podcast_episode"

    def test_process_episode_item_with_minimal_data(self, mock_auth):
        """Test processing episode with minimal required fields."""
        service = PodcastService()
        episode_data = {
            "id": 54321,
            "title": "Minimal Episode",
        }

        result = service._process_episode_item(episode_data)

        assert result.id == 54321
        assert result.title == "Minimal Episode"
        assert result.description is None
        assert result.enclosure_url is None
