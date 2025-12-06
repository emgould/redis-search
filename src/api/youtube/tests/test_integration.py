"""
Integration tests for YouTube Wrapper.
These tests hit the actual YouTube Data API and require a valid API key.
"""

import os
from unittest.mock import PropertyMock, patch

import pytest

from api.youtube.wrappers import YouTubeWrapper
from contracts.models import MCType

pytestmark = pytest.mark.integration


@pytest.fixture
def youtube_api_key():
    """Get YouTube API key from environment."""
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        pytest.skip("YOUTUBE_API_KEY environment variable not set")
    return api_key


@pytest.fixture
def youtube_wrapper(youtube_api_key):
    """Create YouTubeWrapper instance with real API key."""
    wrapper = YouTubeWrapper()
    # Patch the API key on the wrapper's service instance
    with patch.object(wrapper.service, "youtube_api_key", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = youtube_api_key
        yield wrapper


class TestGetTrendingVideosIntegration:
    """Integration tests for get_trending_videos method."""

    @pytest.mark.asyncio
    async def test_get_trending_videos_us(self, youtube_wrapper):
        """Test getting trending videos for US region."""
        result = await youtube_wrapper.get_trending_videos(
            region_code="US", language="en", max_results=50
        )

        # Should not have error
        assert result.error is None

        # Should have videos
        assert isinstance(result.videos, list)
        assert len(result.videos) > 0

        # Check video structure - verify ALL required fields
        video = result.videos[0]
        assert video.id is not None
        assert video.video_id is not None
        assert video.title is not None
        assert video.description is not None
        assert video.channel_title is not None
        assert video.view_count is not None
        assert video.url is not None

        # Required MediaCircle fields
        assert video.mc_id is not None, "mc_id is required"
        assert video.mc_type == "youtube_video", "mc_type must be 'youtube_video'"
        assert video.source is not None, "source is required"
        assert video.source.value == "youtube", "source must be 'youtube'"
        assert video.source_id is not None, "source_id is required"
        assert video.source_id == video.video_id, "source_id should match video_id"

        # Check response metadata
        assert result.region_code == "US"
        assert result.language == "en"
        assert result.total_results > 0

    @pytest.mark.asyncio
    async def test_get_trending_videos_with_category(self, youtube_wrapper):
        """Test getting trending videos filtered by category."""
        result = await youtube_wrapper.get_trending_videos(
            region_code="US",
            language="en",
            max_results=50,
            category_id="28",  # Science & Technology
        )

        assert result.error is None
        assert len(result.videos) > 0
        assert result.category_id == "28"

    @pytest.mark.asyncio
    async def test_get_trending_videos_different_region(self, youtube_wrapper):
        """Test getting trending videos for different region."""
        result = await youtube_wrapper.get_trending_videos(
            region_code="GB", language="en", max_results=50
        )

        assert result.error is None
        assert len(result.videos) > 0
        assert result.region_code == "GB"


class TestSearchVideosIntegration:
    """Integration tests for search_videos method."""

    @pytest.mark.asyncio
    async def test_search_videos_basic(self, youtube_wrapper):
        """Test basic video search."""
        result = await youtube_wrapper.search_videos(
            query="Python programming", max_results=5, region_code="US", language="en"
        )

        # Should not have error
        assert result.error is None

        # Should have videos
        assert isinstance(result.results, list)
        assert len(result.results) > 0

        # Check video structure - verify ALL required fields
        video = result.results[0]
        assert video.id is not None
        assert video.title is not None
        assert video.description is not None
        assert video.channel_title is not None
        assert video.view_count is not None

        # Required MediaCircle fields
        assert video.mc_id is not None, "mc_id is required"
        assert video.mc_type is not None, "mc_type is required"
        assert video.mc_type == "youtube_video", "mc_type must be 'youtube_video'"
        assert video.source is not None, "source is required"
        assert video.source.value == "youtube", "source must be 'youtube'"
        assert video.source_id is not None, "source_id is required"
        assert video.source_id == video.video_id, "source_id should match video_id"

        # Check response metadata
        assert result.query == "Python programming"
        assert result.total_results > 0

    @pytest.mark.asyncio
    async def test_search_videos_with_order(self, youtube_wrapper):
        """Test video search with different ordering."""
        result = await youtube_wrapper.search_videos(
            query="Python tutorial", max_results=5, order="viewCount"
        )

        assert result.error is None
        assert len(result.results) > 0

        # Videos should be ordered by view count (descending)
        if len(result.results) > 1:
            view_counts = [v.view_count for v in result.results]
            # Check that view counts are generally decreasing
            # (may not be strictly decreasing due to other factors)
            assert view_counts[0] >= view_counts[-1]

    @pytest.mark.asyncio
    async def test_search_videos_empty_query(self, youtube_wrapper):
        """Test search with empty query returns results."""
        result = await youtube_wrapper.search_videos(query="", max_results=5)

        # Empty query should still return results (YouTube's default behavior)
        assert result.results is not None


class TestGetVideoCategoriesIntegration:
    """Integration tests for get_video_categories method."""

    @pytest.mark.asyncio
    async def test_get_video_categories_us(self, youtube_wrapper):
        """Test getting video categories for US region."""
        result = await youtube_wrapper.get_video_categories(region_code="US", language="en")

        # Should not have error
        assert result.error is None

        # Should have categories
        assert isinstance(result.categories, list)
        assert len(result.categories) > 0

        # Check category structure
        category = result.categories[0]
        assert category.id is not None
        assert category.title is not None
        assert category.assignable is not None

        # Check response metadata
        assert result.region_code == "US"
        assert result.language == "en"

    @pytest.mark.asyncio
    async def test_get_video_categories_different_region(self, youtube_wrapper):
        """Test getting video categories for different region."""
        result = await youtube_wrapper.get_video_categories(region_code="GB", language="en")

        assert result.error is None
        assert len(result.categories) > 0
        assert result.region_code == "GB"


class TestGetPopularVideosIntegration:
    """Integration tests for get_popular_videos method."""

    @pytest.mark.asyncio
    async def test_get_popular_videos_default_query(self, youtube_wrapper):
        """Test getting popular videos with default query."""
        result = await youtube_wrapper.get_popular_videos(max_results=5)

        # Should not have error
        assert result.error is None

        # Should have videos
        assert isinstance(result.videos, list)
        assert len(result.videos) > 0

        # Check response metadata
        assert result.type == "popular_videos"
        assert result.method == "search_with_viewcount_ordering"
        assert result.note is not None

    @pytest.mark.asyncio
    async def test_get_popular_videos_custom_query(self, youtube_wrapper):
        """Test getting popular videos with custom query."""
        result = await youtube_wrapper.get_popular_videos(query="music", max_results=5)

        assert result.error is None
        assert len(result.videos) > 0
        assert result.query == "music"


class TestErrorHandling:
    """Integration tests for error handling."""

    @pytest.mark.asyncio
    async def test_invalid_region_code(self, youtube_wrapper):
        """Test handling of invalid region code."""
        result = await youtube_wrapper.get_trending_videos(region_code="INVALID", max_results=50)

        # Should return empty results (no error, just empty list)
        assert result.videos == []
        assert result.total_results == 0

    @pytest.mark.asyncio
    async def test_invalid_category_id(self, youtube_wrapper):
        """Test handling of invalid category ID."""
        result = await youtube_wrapper.get_trending_videos(
            region_code="US", category_id="999999", max_results=50
        )

        # May return error or empty results depending on API behavior
        # Just verify it doesn't crash
        assert result.videos is not None or result.error is not None


class TestSearchPeopleIntegration:
    """Integration tests for search_people method."""

    @pytest.mark.asyncio
    async def test_search_people_basic(self, youtube_wrapper):
        """Test basic people/creator search."""
        result = await youtube_wrapper.search_people_async(query="MrBeast", limit=5)

        # Should not have error
        assert result.error is None

        # Should have results
        assert isinstance(result.results, list)
        assert len(result.results) > 0

        # Check creator structure
        creator = result.results[0]
        assert creator.id is not None
        assert creator.title is not None
        assert creator.url is not None

        # Required MediaCircle fields
        assert creator.mc_id is not None, "mc_id is required"
        assert creator.mc_type is not None, "mc_type is required"
        assert creator.mc_type == MCType.PERSON, "mc_type must be 'person'"
        assert creator.source is not None, "source is required"
        assert creator.source.value == "youtube", "source must be 'youtube'"

        # Check response metadata
        assert result.query == "MrBeast"
        assert result.total_results > 0

    @pytest.mark.asyncio
    async def test_search_people_empty_results(self, youtube_wrapper):
        """Test people search with query that returns no results."""
        result = await youtube_wrapper.search_people_async(
            query="xyzabc123nonexistentcreator999", limit=5
        )

        # Should not have error (empty results is valid)
        assert result.error is None or result.total_results == 0


class TestGetPersonDetailsIntegration:
    """Integration tests for get_person_details method."""

    @pytest.mark.asyncio
    async def test_get_person_details_basic(self, youtube_wrapper):
        """Test getting creator details by channel ID."""
        # Use a well-known channel ID (YouTube's official channel)
        result = await youtube_wrapper.get_person_details(
            source_id="UC_x5XG1OV2P6uZZ5FSM9Ttw", limit=10
        )

        # Should not have error
        assert result.error is None

        # Should have exactly one result
        assert len(result.results) == 1
        assert result.total_results == 1

        # Check creator structure
        creator = result.results[0]
        assert creator.id == "UC_x5XG1OV2P6uZZ5FSM9Ttw"
        assert creator.title is not None
        assert creator.url is not None

        # Required MediaCircle fields
        assert creator.mc_id is not None, "mc_id is required"
        assert creator.mc_type == MCType.PERSON, "mc_type must be 'person'"
        assert creator.source.value == "youtube", "source must be 'youtube'"


class TestRequiredFieldsIntegration:
    """Integration tests to verify all videos have required fields."""

    @pytest.mark.asyncio
    async def test_all_trending_videos_have_required_fields(self, youtube_wrapper):
        """Test that ALL trending videos have mc_id, mc_type, source, and source_id."""
        result = await youtube_wrapper.get_trending_videos(region_code="US", max_results=10)

        # Skip test if quota exceeded
        if result.error and "quotaExceeded" in result.error:
            pytest.skip("YouTube API quota exceeded")

        assert result.error is None
        assert len(result.videos) > 0

        for video in result.videos:
            # Verify required fields are present and not None/empty
            assert video.mc_id, f"mc_id is missing or empty for video: {video.title}"
            assert video.mc_type, f"mc_type is missing or empty for video: {video.title}"
            assert video.source, f"source is missing or empty for video: {video.title}"
            assert video.source_id, f"source_id is missing or empty for video: {video.title}"

            # Verify correct values
            assert video.mc_type == "youtube_video"
            assert video.source.value == "youtube"
            assert video.source_id == video.video_id

    @pytest.mark.asyncio
    async def test_all_search_videos_have_required_fields(self, youtube_wrapper):
        """Test that ALL search videos have mc_id, mc_type, source, and source_id."""
        result = await youtube_wrapper.search_videos(query="Python", max_results=10)

        # Skip test if quota exceeded
        if result.error and "quotaExceeded" in result.error:
            pytest.skip("YouTube API quota exceeded")

        assert result.error is None
        assert len(result.results) > 0

        for video in result.results:
            # Verify required fields are present and not None/empty
            assert video.mc_id, f"mc_id is missing or empty for video: {video.title}"
            assert video.mc_type, f"mc_type is missing or empty for video: {video.title}"
            assert video.source, f"source is missing or empty for video: {video.title}"
            assert video.source_id, f"source_id is missing or empty for video: {video.title}"

            # Verify correct values
            assert video.mc_type == "youtube_video"
            assert video.source.value == "youtube"
            assert video.source_id == video.video_id

    @pytest.mark.asyncio
    async def test_all_popular_videos_have_required_fields(self, youtube_wrapper):
        """Test that ALL popular videos have mc_id, mc_type, source, and source_id."""
        result = await youtube_wrapper.get_popular_videos(max_results=10)

        # Skip test if quota exceeded
        if result.error and "quotaExceeded" in result.error:
            pytest.skip("YouTube API quota exceeded")

        assert result.error is None
        assert len(result.videos) > 0

        for video in result.videos:
            # Verify required fields are present and not None/empty
            assert video.mc_id, f"mc_id is missing or empty for video: {video.title}"
            assert video.mc_type, f"mc_type is missing or empty for video: {video.title}"
            assert video.source, f"source is missing or empty for video: {video.title}"
            assert video.source_id, f"source_id is missing or empty for video: {video.title}"

            # Verify correct values
            assert video.mc_type == "youtube_video"
            assert video.source.value == "youtube"
            assert video.source_id == video.video_id
