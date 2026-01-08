"""
Unit tests for YouTube Models.
Tests Pydantic model validation and serialization.
"""

import pytest
from pydantic import ValidationError

from api.youtube.models import (
    DynamicYouTubeVideo,
    VideoSearchResponse,
    YouTubeCategoriesResponse,
    YouTubeCategory,
    YouTubeCreator,
    YouTubePopularResponse,
    YouTubeSearchResponse,
    YouTubeTrendingResponse,
    YouTubeVideo,
)
from contracts.models import MCType

pytestmark = pytest.mark.unit


class TestDynamicYouTubeVideo:
    """Tests for DynamicYouTubeVideo model."""

    def test_dynamic_video_with_full_data(self):
        """Test dynamic video model with complete data."""
        video_data = {
            "video_id": "test_video_id",
            "title": "Test Video",
            "channel": "Test Channel",
            "published_time": "2 hours ago",
            "view_count": "1.5M views",
            "thumbnail_url": "https://example.com/thumbnail.jpg",
            "description": "Test Description",
            "duration_seconds": 330,
            "category": "Entertainment",
            "tags": ["test", "video"],
            "publish_date": "2024-01-01",
            "is_live": False,
            "url": "https://www.youtube.com/watch?v=test_video_id",
        }

        video = DynamicYouTubeVideo(**video_data)

        assert video.video_id == "test_video_id"
        assert video.title == "Test Video"
        assert video.channel == "Test Channel"
        assert video.view_count == "1.5M views"
        assert video.duration_seconds == 330
        assert video.is_live is False

    def test_dynamic_video_with_minimal_data(self):
        """Test dynamic video model with minimal required data."""
        video_data = {
            "video_id": "test_id",
            "title": "Test Video",
            "url": "https://www.youtube.com/watch?v=test_id",
        }

        video = DynamicYouTubeVideo(**video_data)

        assert video.video_id == "test_id"
        assert video.title == "Test Video"
        assert video.channel is None
        assert video.tags == []
        assert video.is_live is False


class TestYouTubeVideo:
    """Tests for YouTubeVideo model."""

    def test_video_with_full_data(self):
        """Test video model with complete data."""
        video_data = {
            "id": "test_video_id",
            "video_id": "test_video_id",
            "title": "Test Video",
            "description": "Test Description",
            "channel_title": "Test Channel",
            "channel_id": "test_channel_id",
            "published_at": "2024-01-01T00:00:00Z",
            "thumbnail_url": "https://example.com/thumbnail.jpg",
            "url": "https://www.youtube.com/watch?v=test_video_id",
            "view_count": 1000,
            "like_count": 100,
            "comment_count": 10,
            "duration": "PT5M30S",
            "tags": ["test", "video"],
            "category_id": "28",
            "default_language": "en",
        }

        video = YouTubeVideo(**video_data)

        assert video.id == "test_video_id"
        assert video.video_id == "test_video_id"
        assert video.title == "Test Video"
        assert video.description == "Test Description"
        assert video.channel_title == "Test Channel"
        assert video.view_count == 1000
        assert video.like_count == 100
        assert video.comment_count == 10
        assert video.duration == "PT5M30S"
        assert video.tags == ["test", "video"]
        assert video.mc_id is not None
        assert video.mc_type == "youtube_video"

    def test_video_with_minimal_data(self):
        """Test video model with minimal required data."""
        video_data = {
            "id": "test_id",
            "video_id": "test_id",
            "title": "Test Video",
            "url": "https://www.youtube.com/watch?v=test_id",
        }

        video = YouTubeVideo(**video_data)

        assert video.id == "test_id"
        assert video.title == "Test Video"
        assert video.description == ""
        assert video.view_count == 0
        assert video.like_count == 0
        assert video.comment_count == 0
        assert video.tags == []

    def test_video_missing_required_fields(self):
        """Test that video model requires essential fields."""
        with pytest.raises(ValidationError):
            YouTubeVideo(id="test_id")  # Missing title, video_id, url

    def test_video_auto_generates_mc_id(self):
        """Test that mc_id is auto-generated if not provided."""
        video_data = {
            "id": "test_id",
            "video_id": "test_id",
            "title": "Test Video",
            "url": "https://www.youtube.com/watch?v=test_id",
        }

        video = YouTubeVideo(**video_data)

        assert video.mc_id is not None
        assert video.mc_type == "youtube_video"

    def test_video_auto_sets_source_id(self):
        """Test that source_id is auto-set from video_id if not provided."""
        video_data = {
            "id": "test_id",
            "video_id": "test_id",
            "title": "Test Video",
            "url": "https://www.youtube.com/watch?v=test_id",
        }

        video = YouTubeVideo(**video_data)

        assert video.source_id == "test_id"
        assert video.source_id == video.video_id
        assert video.source.value == "youtube"

    def test_video_preserves_provided_source_id(self):
        """Test that provided source_id is preserved."""
        video_data = {
            "id": "test_id",
            "video_id": "test_id",
            "title": "Test Video",
            "url": "https://www.youtube.com/watch?v=test_id",
            "source_id": "custom_source_id",
        }

        video = YouTubeVideo(**video_data)

        assert video.source_id == "custom_source_id"

    def test_video_preserves_provided_mc_id(self):
        """Test that provided mc_id is preserved."""
        video_data = {
            "id": "test_id",
            "video_id": "test_id",
            "title": "Test Video",
            "url": "https://www.youtube.com/watch?v=test_id",
            "mc_id": "custom_mc_id",
        }

        video = YouTubeVideo(**video_data)

        assert video.mc_id == "custom_mc_id"

    def test_video_from_dynamic_conversion(self):
        """Test converting DynamicYouTubeVideo to YouTubeVideo."""
        dynamic_video = DynamicYouTubeVideo(
            video_id="test_id",
            title="Test Video",
            channel="Test Channel",
            view_count="2.5M views",
            duration_seconds=330,
            url="https://www.youtube.com/watch?v=test_id",
            is_live=False,
        )

        video = YouTubeVideo.from_dynamic(dynamic_video)

        assert video.id == "test_id"
        assert video.video_id == "test_id"
        assert video.title == "Test Video"
        assert video.channel_title == "Test Channel"
        assert video.view_count == 2_500_000  # Converted from "2.5M views"
        assert video.duration == "5:30"  # Converted from 330 seconds
        assert video.is_live is False
        assert video.mc_type == "youtube_video"
        assert video.source.value == "youtube"

    def test_video_from_dynamic_view_count_parsing(self):
        """Test view count parsing from different formats."""
        # Test millions
        dynamic_video = DynamicYouTubeVideo(
            video_id="test1",
            title="Test",
            url="https://www.youtube.com/watch?v=test1",
            view_count="1.5M views",
        )
        video = YouTubeVideo.from_dynamic(dynamic_video)
        assert video.view_count == 1_500_000

        # Test thousands
        dynamic_video = DynamicYouTubeVideo(
            video_id="test2",
            title="Test",
            url="https://www.youtube.com/watch?v=test2",
            view_count="500K views",
        )
        video = YouTubeVideo.from_dynamic(dynamic_video)
        assert video.view_count == 500_000

        # Test plain number
        dynamic_video = DynamicYouTubeVideo(
            video_id="test3",
            title="Test",
            url="https://www.youtube.com/watch?v=test3",
            view_count="1000 views",
        )
        video = YouTubeVideo.from_dynamic(dynamic_video)
        assert video.view_count == 1000


class TestYouTubeCategory:
    """Tests for YouTubeCategory model."""

    def test_category_with_full_data(self):
        """Test category model with complete data."""
        category_data = {
            "id": "28",
            "title": "Science & Technology",
            "assignable": True,
        }

        category = YouTubeCategory(**category_data)

        assert category.id == "28"
        assert category.title == "Science & Technology"
        assert category.assignable is True

    def test_category_with_minimal_data(self):
        """Test category model with minimal data."""
        category_data = {
            "id": "28",
            "title": "Science & Technology",
        }

        category = YouTubeCategory(**category_data)

        assert category.id == "28"
        assert category.title == "Science & Technology"
        assert category.assignable is False  # Default value

    def test_category_missing_required_fields(self):
        """Test that category model requires essential fields."""
        with pytest.raises(ValidationError):
            YouTubeCategory(id="28")  # Missing title


class TestYouTubeSearchResponse:
    """Tests for YouTubeSearchResponse model."""

    def test_search_response_with_full_data(self):
        """Test search response model with complete data."""
        response_data = {
            "date": "2024-01-01",
            "results": [
                {
                    "id": "video1",
                    "video_id": "video1",
                    "title": "Video 1",
                    "url": "https://www.youtube.com/watch?v=video1",
                }
            ],
            "total_results": 1,
            "query": "Python programming",
        }

        response = YouTubeSearchResponse(**response_data)

        assert len(response.results) == 1
        assert response.total_results == 1
        assert response.query == "Python programming"

    def test_search_response_with_minimal_data(self):
        """Test search response model with minimal data."""
        response_data = {
            "date": "2024-01-01",
            "results": [],
            "total_results": 0,
            "query": "test query",
        }

        response = YouTubeSearchResponse(**response_data)

        assert response.results == []
        assert response.total_results == 0
        assert response.query == "test query"


class TestYouTubeTrendingResponse:
    """Tests for YouTubeTrendingResponse model."""

    def test_trending_response_with_full_data(self):
        """Test trending response model with complete data."""
        response_data = {
            "date": "2024-01-01",
            "videos": [
                {
                    "id": "video1",
                    "video_id": "video1",
                    "title": "Trending Video 1",
                    "url": "https://www.youtube.com/watch?v=video1",
                }
            ],
            "total_results": 1,
            "region_code": "US",
            "language": "en",
            "category_id": "28",
            "query": "tech",
            "fetched_at": "etag123",
            "next_page_token": "next_token",
        }

        response = YouTubeTrendingResponse(**response_data)

        assert len(response.videos) == 1
        assert response.total_results == 1
        assert response.region_code == "US"
        assert response.language == "en"
        assert response.category_id == "28"
        assert response.query == "tech"


class TestYouTubeCategoriesResponse:
    """Tests for YouTubeCategoriesResponse model."""

    def test_categories_response_with_full_data(self):
        """Test categories response model with complete data."""
        response_data = {
            "date": "2024-01-01",
            "categories": [
                {"id": "28", "title": "Science & Technology", "assignable": True},
                {"id": "10", "title": "Music", "assignable": True},
            ],
            "region_code": "US",
            "language": "en",
        }

        response = YouTubeCategoriesResponse(**response_data)

        assert len(response.categories) == 2
        assert response.categories[0].title == "Science & Technology"
        assert response.region_code == "US"
        assert response.language == "en"


class TestYouTubePopularResponse:
    """Tests for YouTubePopularResponse model."""

    def test_popular_response_with_full_data(self):
        """Test popular response model with complete data."""
        response_data = {
            "date": "2024-01-01",
            "videos": [
                {
                    "id": "video1",
                    "video_id": "video1",
                    "title": "Popular Video 1",
                    "url": "https://www.youtube.com/watch?v=video1",
                }
            ],
            "total_results": 1,
            "query": "trending",
            "type": "popular_videos",
            "method": "search_with_viewcount_ordering",
            "note": "Popular videos fetched via search API",
            "region_code": "US",
            "language": "en",
        }

        response = YouTubePopularResponse(**response_data)

        assert len(response.videos) == 1
        assert response.total_results == 1
        assert response.query == "trending"
        assert response.type == "popular_videos"
        assert response.method == "search_with_viewcount_ordering"
        assert response.region_code == "US"


class TestYouTubeCreator:
    """Tests for YouTubeCreator model."""

    def test_creator_with_full_data(self):
        """Test creator model with complete data."""
        creator_data = {
            "id": "UC_test_channel_id",
            "title": "Test Channel",
            "url": "https://www.youtube.com/channel/UC_test_channel_id",
            "subscriber_count": 1000000,
            "video_count": 500,
            "description": "Test channel description",
            "avatar": "https://example.com/avatar.jpg",
            "banner": "https://example.com/banner.jpg",
            "country": "US",
            "joined_date": "Jan 1, 2020",
        }

        creator = YouTubeCreator(**creator_data)

        assert creator.id == "UC_test_channel_id"
        assert creator.title == "Test Channel"
        assert creator.subscriber_count == 1000000
        assert creator.video_count == 500
        assert creator.description == "Test channel description"
        assert creator.avatar == "https://example.com/avatar.jpg"
        assert creator.banner == "https://example.com/banner.jpg"
        assert creator.country == "US"
        assert creator.joined_date == "Jan 1, 2020"
        assert creator.mc_type == MCType.PERSON
        assert creator.source.value == "youtube"

    def test_creator_with_minimal_data(self):
        """Test creator model with minimal required data."""
        creator_data = {
            "id": "UC_test_id",
            "title": "Test Channel",
            "url": "https://www.youtube.com/channel/UC_test_id",
        }

        creator = YouTubeCreator(**creator_data)

        assert creator.id == "UC_test_id"
        assert creator.title == "Test Channel"
        assert creator.subscriber_count == 0
        assert creator.video_count == 0
        assert creator.description == ""
        assert creator.avatar is None
        assert creator.banner is None

    def test_creator_missing_required_fields(self):
        """Test that creator model requires essential fields."""
        with pytest.raises(ValidationError):
            YouTubeCreator(id="UC_test_id")  # Missing title, url

    def test_creator_auto_generates_mc_id(self):
        """Test that mc_id is auto-generated if not provided."""
        creator_data = {
            "id": "UC_test_id",
            "title": "Test Channel",
            "url": "https://www.youtube.com/channel/UC_test_id",
        }

        creator = YouTubeCreator(**creator_data)

        assert creator.mc_id is not None
        assert creator.mc_type == MCType.PERSON
        assert creator.source.value == "youtube"


class TestVideoSearchResponse:
    """Tests for VideoSearchResponse model."""

    def test_video_search_response_with_full_data(self):
        """Test video search response model with complete data."""
        response_data = {
            "date": "2024-01-01",
            "results": [
                {
                    "id": "video1",
                    "video_id": "video1",
                    "title": "Video 1",
                    "url": "https://www.youtube.com/watch?v=video1",
                }
            ],
            "total_results": 1,
            "query": "Python programming",
        }

        response = VideoSearchResponse(**response_data)

        assert len(response.results) == 1
        assert response.total_results == 1
        assert response.query == "Python programming"
        assert response.data_source == "Youtube Videos"
        assert response.data_type.value == "youtube_video"

    def test_video_search_response_with_minimal_data(self):
        """Test video search response model with minimal data."""
        response_data = {
            "date": "2024-01-01",
            "results": [],
            "total_results": 0,
            "query": "test query",
        }

        response = VideoSearchResponse(**response_data)

        assert response.results == []
        assert response.total_results == 0
        assert response.query == "test query"
        assert response.data_source == "Youtube Videos"
