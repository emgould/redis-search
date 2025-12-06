"""
Integration tests for TVDB service.

These tests hit the actual TVDB API and require:
- TVDB_API_KEY environment variable
- Internet connection
- Valid API credentials

Run with: pytest services/tvdb/tests/test_integration.py -v -m integration
"""

import os

import pytest

from api.subapi._tvdb.core import TVDBService

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture
def tvdb_api_key():
    """Get TVDB API key from environment."""
    api_key = os.getenv("TVDB_API_KEY")
    if not api_key:
        pytest.skip("TVDB_API_KEY environment variable not set")
    return api_key


@pytest.fixture
def tvdb_service(tvdb_api_key):
    """Create TVDBService instance."""
    return TVDBService(tvdb_api_key)


class TestTVDBIntegrationSearch:
    """Integration tests for TVDB search."""

    def test_search_popular_show(self, tvdb_service):
        """Test searching for a popular show."""
        results = tvdb_service.search("The Office", limit=5)

        assert len(results) > 0
        assert any("office" in show["name"].lower() for show in results)

        # Check first result has expected fields
        first_result = results[0]
        assert "id" in first_result
        assert "name" in first_result
        assert isinstance(first_result["id"], int)

    def test_search_with_limit(self, tvdb_service):
        """Test search respects limit parameter."""
        results = tvdb_service.search("Breaking Bad", limit=3)

        assert len(results) <= 3

    def test_search_no_results(self, tvdb_service):
        """Test search with query that returns no results."""
        results = tvdb_service.search("xyzabc123nonexistentshow999", limit=5)

        assert len(results) == 0


class TestTVDBIntegrationShowDetails:
    """Integration tests for TVDB show details."""

    def test_get_show_details_basic(self, tvdb_service):
        """Test getting basic show details."""
        # The Office (US) - TVDB ID: 73244
        result = tvdb_service.get_show_details(73244, extended=False)

        assert result is not None
        assert result["id"] == 73244
        assert "office" in result["name"].lower()
        assert "overview" in result
        assert "status" in result

    def test_get_show_details_extended(self, tvdb_service):
        """Test getting extended show details."""
        # The Office (US) - TVDB ID: 73244
        result = tvdb_service.get_show_details(73244, extended=True)

        assert result is not None
        assert result["id"] == 73244

        # Extended fields
        assert "genres" in result
        assert isinstance(result["genres"], list)
        assert "external_ids" in result
        assert isinstance(result["external_ids"], dict)
        assert "seasons_count" in result
        assert result["seasons_count"] > 0

    def test_get_show_details_invalid_id(self, tvdb_service):
        """Test getting details for invalid show ID."""
        with pytest.raises((Exception, ValueError)):
            tvdb_service.get_show_details(999999999, extended=False)


class TestTVDBIntegrationImages:
    """Integration tests for TVDB images."""

    def test_get_show_images_with_id(self, tvdb_service):
        """Test getting show images with TVDB ID."""
        # The Office (US) - TVDB ID: 73244
        result = tvdb_service.get_show_images(
            query="", tvdb_id=73244, lang="eng", image_types=["poster", "logo"]
        )

        assert result["tvdbid"] == 73244
        assert result["show_name"] is not None
        assert "office" in result["show_name"].lower()

        # Should have at least one image type
        assert result.get("poster") is not None or result.get("logo") is not None

    def test_get_show_images_with_query(self, tvdb_service):
        """Test getting show images with search query."""
        result = tvdb_service.get_show_images(
            query="The Office", tvdb_id=None, lang="eng", image_types=["poster"]
        )

        assert result["tvdbid"] is not None
        assert result["show_name"] is not None

    def test_get_all_images(self, tvdb_service):
        """Test getting all images for a show."""
        # The Office (US) - TVDB ID: 73244
        result = tvdb_service.get_all_images(73244, lang="eng")

        assert isinstance(result, dict)
        assert len(result) > 0

        # Check that images are organized by type
        for _image_type, images in result.items():
            assert isinstance(images, list)
            if images:
                # Check first image has expected fields
                assert "url" in images[0]
                assert "score" in images[0]


class TestTVDBIntegrationComplete:
    """Integration tests for complete show data."""

    def test_get_show_complete(self, tvdb_service):
        """Test getting complete show data."""
        # The Office (US) - TVDB ID: 73244
        result = tvdb_service.get_show_complete(73244, lang="eng")

        assert result is not None
        assert result["id"] == 73244

        # Should have extended details
        assert "genres" in result
        assert "external_ids" in result

        # Should have images
        assert "images" in result
        assert "all" in result["images"]
        assert "best" in result["images"]


class TestTVDBIntegrationExternalId:
    """Integration tests for external ID search."""

    @pytest.mark.skip(
        reason="TVDB search_by_remote_id API appears to be unreliable or changed format"
    )
    def test_search_by_imdb_id(self, tvdb_service: TVDBService):
        """Test searching by IMDB ID."""
        # The Office (US) - IMDB ID: tt0386676
        # NOTE: This test is currently skipped because the TVDB SDK's search_by_remote_id
        # method is not returning expected results. The API may have changed or the SDK
        # needs updating. Regular search works fine.
        result = tvdb_service.search_by_external_id("tt0386676", source="imdb")

        assert result is not None
        assert result["id"] is not None
        assert "office" in result["name"].lower()
        assert result["external_id"] == "tt0386676"
        assert result["external_source"] == "imdb"

    def test_search_by_invalid_external_id(self, tvdb_service):
        """Test searching by invalid external ID."""
        result = tvdb_service.search_by_external_id("tt9999999999", source="imdb")

        # Should return None or empty result
        assert result is None or result == {}


class TestTVDBIntegrationTMDB:
    """Integration tests for TMDB integration."""

    def test_search_tmdb_multi(self, tvdb_service):
        """Test TMDB multi search."""
        tmdb_token = os.getenv("TMDB_API_TOKEN")
        if not tmdb_token:
            pytest.skip("TMDB_API_TOKEN environment variable not set")

        result = tvdb_service.search_tmdb_multi(
            query="Breaking Bad", tmdb_token=tmdb_token, page=1, limit=5
        )

        assert "results" in result
        assert len(result["results"]) > 0
        assert result["query"] == "Breaking Bad"
        assert result["data_source"] == "TMDB Multi Search"

        # Check first result
        first_result = result["results"][0]
        assert "id" in first_result
        assert "name" in first_result
        assert "media_type" in first_result


# Comprehensive workflow test
@pytest.mark.slow
class TestTVDBIntegrationWorkflow:
    """Test complete TVDB workflow."""

    def test_complete_workflow(self, tvdb_service):
        """Test a complete workflow: search -> details -> images."""
        # Step 1: Search for a show
        search_results = tvdb_service.search("The Office", limit=5)
        assert len(search_results) > 0

        # Step 2: Get details for first result
        tvdb_id = search_results[0]["id"]
        details = tvdb_service.get_show_details(tvdb_id, extended=True)
        assert details is not None
        assert details["id"] == tvdb_id

        # Step 3: Get images for the show
        images = tvdb_service.get_show_images(
            query="", tvdb_id=tvdb_id, lang="eng", image_types=["poster", "logo"]
        )
        assert images["tvdbid"] == tvdb_id

        # Step 4: Get complete data
        complete = tvdb_service.get_show_complete(tvdb_id, lang="eng")
        assert complete is not None
        assert "images" in complete
