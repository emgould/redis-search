"""
Integration tests for NewsAI service.
These tests make real API calls to Event Registry and require a valid API key.

Run with: ./bin/run_tests.sh --integration
"""

import json
import os
from unittest.mock import MagicMock

import pytest

from api.newsai.handlers import newsai_handler
from api.newsai.wrappers import newsai_wrapper
from utils.pytest_utils import write_snapshot

pytestmark = pytest.mark.integration

# Skip integration tests if API key is not available or is a test key
# Note: We allow ENVIRONMENT=test for integration tests (set by conftest.py)
# but skip if the API key itself is a test key
SKIP_INTEGRATION = not os.getenv("NEWSAI_API_KEY") or os.getenv("NEWSAI_API_KEY", "").startswith(
    "test_"
)


def create_mock_request(args: dict) -> MagicMock:
    """Create a mock Firebase Functions Request object."""
    mock_req = MagicMock()
    mock_req.args = args
    mock_req.method = "GET"
    return mock_req


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests require valid NEWSAI_API_KEY")
@pytest.mark.asyncio
async def test_get_trending_events_handler_default_query():
    """Integration test for get_trending_events handler with NO query parameter."""
    # Create mock request with no query parameter
    mock_req = create_mock_request({"page_size": "5"})

    # Call handler (makes real API call)
    response = await newsai_handler.get_trending_events(mock_req)

    # Verify response - status is a string like "200 OK"
    assert "200" in str(response.status)
    assert response.headers["Content-Type"] == "application/json"

    # Parse response data
    result = json.loads(
        response.data.decode("utf-8") if isinstance(response.data, bytes) else response.data
    )

    # Verify structure
    assert result["status_code"] == 200
    assert result["error"] is None
    assert isinstance(result["results"], list)
    assert result["data_source"] == "NewsAI Trending Events"

    # If results returned, verify event structure
    if len(result["results"]) > 0:
        event = result["results"][0]
        assert event["mc_id"] is not None
        assert event["uri"] is not None
        assert event["title"] is not None
        assert event["event_date"] is not None
        assert event["total_article_count"] > 0

        print(f"\n  First event: {event['title'][:80]}")
        print(f"  Event Date: {event['event_date']}")
        print(f"  Total Articles: {event['total_article_count']}")

    # Write snapshot
    result_copy = result.copy()
    result_copy.pop("fetched_at", None)
    # Remove dynamic fields from results
    if "results" in result_copy:
        for event in result_copy["results"]:
            event.pop("mc_id", None)
            event.pop("fetched_at", None)

    write_snapshot(
        result_copy,
        "get_trending_events_handler_default_query_result.json",
    )

    print(
        f"\n✓ get_trending_events handler (default query) passed: {len(result['results'])} events"
    )


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests require valid NEWSAI_API_KEY")
@pytest.mark.asyncio
async def test_get_trending_news_handler_basic():
    """Integration test for get_trending_news handler with basic parameters."""
    # Create mock request
    mock_req = create_mock_request({"country": "us", "page_size": "5"})

    # Call handler (makes real API call)
    response = await newsai_handler.get_trending_news(mock_req)

    # Verify response - status is a string like "200 OK"
    assert "200" in str(response.status)
    assert response.headers["Content-Type"] == "application/json"

    # Parse response data
    result = json.loads(
        response.data.decode("utf-8") if isinstance(response.data, bytes) else response.data
    )

    # Verify structure
    assert result["status_code"] == 200
    assert result["error"] is None
    assert isinstance(result["results"], list)
    assert result["data_source"] == "NewsAI Trending"

    # Validate article quality - check for error indicators
    error_articles = []
    for i, article in enumerate(result["results"]):
        if article.get("description") == "Error processing article data":
            error_articles.append(
                {
                    "index": i,
                    "title": article.get("title"),
                    "url": article.get("url"),
                    "source": article.get("news_source", {}).get("name"),
                }
            )

    # Fail test if any articles have processing errors
    if error_articles:
        error_msg = f"Found {len(error_articles)} articles with processing errors:\n"
        for err in error_articles:
            error_msg += f"  [{err['index']}] {err['title']}\n"
            error_msg += f"      URL: {err['url']}\n"
            error_msg += f"      Source: {err['source']}\n"
        pytest.fail(error_msg)

    # Write snapshot
    result_copy = result.copy()
    result_copy.pop("fetched_at", None)
    if "results" in result_copy:
        for article in result_copy["results"]:
            article.pop("mc_id", None)

    write_snapshot(
        result_copy,
        "get_trending_news_handler_basic_result.json",
    )

    print(f"\n✓ get_trending_news handler (basic) passed: {len(result['results'])} articles")


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests require valid NEWSAI_API_KEY")
@pytest.mark.asyncio
async def test_search_news_handler_basic():
    """Integration test for search_news handler with basic query."""
    # Create mock request
    mock_req = create_mock_request({"query": "technology", "language": "en", "page_size": "5"})

    # Call handler (makes real API call)
    response = await newsai_handler.search_news(mock_req)

    # Verify response - status is a string like "200 OK"
    assert "200" in str(response.status)
    assert response.headers["Content-Type"] == "application/json"

    # Parse response data
    result = json.loads(
        response.data.decode("utf-8") if isinstance(response.data, bytes) else response.data
    )

    # Verify structure
    assert result["status_code"] == 200
    assert result["error"] is None
    assert isinstance(result["results"], list)
    assert result["query"] == "technology"

    # Write snapshot
    result_copy = result.copy()
    result_copy.pop("fetched_at", None)
    if "results" in result_copy:
        for article in result_copy["results"]:
            article.pop("mc_id", None)

    write_snapshot(
        result_copy,
        "search_news_handler_basic_result.json",
    )

    print(f"\n✓ search_news handler (basic) passed: {len(result['results'])} articles")


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests require valid NEWSAI_API_KEY")
@pytest.mark.asyncio
async def test_get_news_sources_handler_basic():
    """Integration test for get_news_sources handler."""
    # Create mock request
    mock_req = create_mock_request({})

    # Call handler (makes real API call)
    response = await newsai_handler.get_news_sources(mock_req)

    # Verify response - status is a string like "200 OK"
    assert "200" in str(response.status)
    assert response.headers["Content-Type"] == "application/json"

    # Parse response data
    result = json.loads(
        response.data.decode("utf-8") if isinstance(response.data, bytes) else response.data
    )

    # Verify structure
    assert result["status_code"] == 200
    assert result["error"] is None
    assert isinstance(result["results"], list)

    # Write snapshot
    result_copy = result.copy()
    if "results" in result_copy:
        for source in result_copy["results"]:
            source.pop("mc_id", None)

    write_snapshot(
        result_copy,
        "get_news_sources_handler_basic_result.json",
    )

    print(f"\n✓ get_news_sources handler passed: {len(result['results'])} sources")


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests require valid NEWSAI_API_KEY")
@pytest.mark.asyncio
async def test_get_trending_events_wrapper_with_complex_query():
    """Integration test for get_trending_events wrapper with custom complex query."""
    # Custom complex query
    complex_query = {
        "$query": {
            "$and": [
                {"$or": [{"categoryUri": "dmoz/Computers"}]},
                {"dateStart": "2025-11-01", "dateEnd": "2025-11-11"},
            ]
        }
    }

    # Call wrapper directly (no handler for this yet)
    result = await newsai_wrapper.get_trending_events(
        complex_query=complex_query,
        page_size=5,
    )

    # Verify response
    assert result.status_code == 200
    assert result.error is None
    assert isinstance(result.results, list)

    # Write snapshot
    result_dict = result.model_dump()
    result_copy = result_dict.copy()
    result_copy.pop("fetched_at", None)
    if "results" in result_copy:
        for event in result_copy["results"]:
            event.pop("mc_id", None)

    write_snapshot(
        result_copy,
        "get_trending_events_wrapper_complex_query_result.json",
    )

    print(f"\n✓ get_trending_events wrapper (complex query) passed: {len(result.results)} events")


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests require valid NEWSAI_API_KEY")
@pytest.mark.asyncio
async def test_search_news_wrapper_with_date_range():
    """Integration test for search_news wrapper with date range."""
    # Call wrapper with date range
    result = await newsai_wrapper.search_news(
        query="artificial intelligence",
        from_date="2025-11-01",
        to_date="2025-11-11",
        language="en",
        page_size=5,
    )

    # Verify response
    assert result.status_code == 200
    assert result.error is None
    assert isinstance(result.results, list)
    assert result.query == "artificial intelligence"

    # Write snapshot
    result_dict = result.model_dump()
    result_copy = result_dict.copy()
    result_copy.pop("fetched_at", None)
    if "results" in result_copy:
        for article in result_copy["results"]:
            article.pop("mc_id", None)

    write_snapshot(
        result_copy,
        "search_news_wrapper_date_range_result.json",
    )

    print(f"\n✓ search_news wrapper (date range) passed: {len(result.results)} articles")


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests require valid NEWSAI_API_KEY")
@pytest.mark.asyncio
async def test_get_media_reviews_handler_movie():
    """Integration test for get_media_reviews handler with movie."""
    # Create mock request for a popular movie
    CURRENT_MOVIE = "The Lost Bus"
    mock_req = create_mock_request(
        {"title": CURRENT_MOVIE, "media_type": "movie", "page_size": "5"}
    )

    # Call handler (makes real API call)
    response = await newsai_handler.get_media_reviews(mock_req)

    # Verify response - status is a string like "200 OK"
    assert "200" in str(response.status)
    assert response.headers["Content-Type"] == "application/json"

    # Parse response data
    result = json.loads(
        response.data.decode("utf-8") if isinstance(response.data, bytes) else response.data
    )

    # Verify structure
    assert result["status_code"] == 200
    assert result["error"] is None
    assert isinstance(result["results"], list)
    assert result["query"] == CURRENT_MOVIE
    assert result["data_source"] == "NewsAI Search"

    # If results returned, verify article structure
    if len(result["results"]) > 0:
        article = result["results"][0]
        assert article["mc_id"] is not None
        assert article["title"] is not None
        assert article["url"] is not None
        assert article["mc_type"] == "news_article"

        print(f"\n  First review: {article['title'][:80]}")
        print(f"  Source: {article.get('news_source', {}).get('name', 'Unknown')}")

    # Write snapshot
    result_copy = result.copy()
    result_copy.pop("fetched_at", None)
    if "results" in result_copy:
        for article in result_copy["results"]:
            article.pop("mc_id", None)

    write_snapshot(
        result_copy,
        "get_media_reviews_handler_movie_result.json",
    )

    print(f"\n✓ get_media_reviews handler (movie) passed: {len(result['results'])} reviews")


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests require valid NEWSAI_API_KEY")
@pytest.mark.asyncio
async def test_get_media_reviews_handler_tv():
    """Integration test for get_media_reviews handler with TV show."""
    # Create mock request for a popular TV show
    CURRENT_TV_SHOW = "The Diplomat"
    mock_req = create_mock_request({"title": CURRENT_TV_SHOW, "media_type": "tv", "page_size": "5"})

    # Call handler (makes real API call)
    response = await newsai_handler.get_media_reviews(mock_req)

    # Verify response - status is a string like "200 OK"
    assert "200" in str(response.status)
    assert response.headers["Content-Type"] == "application/json"

    # Parse response data
    result = json.loads(
        response.data.decode("utf-8") if isinstance(response.data, bytes) else response.data
    )

    # Verify structure
    assert result["status_code"] == 200
    assert result["error"] is None
    assert isinstance(result["results"], list)
    assert result["query"] == CURRENT_TV_SHOW

    # Write snapshot
    result_copy = result.copy()
    result_copy.pop("fetched_at", None)
    if "results" in result_copy:
        for article in result_copy["results"]:
            article.pop("mc_id", None)

    write_snapshot(
        result_copy,
        "get_media_reviews_handler_tv_result.json",
    )

    print(f"\n✓ get_media_reviews handler (tv) passed: {len(result['results'])} reviews")


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests require valid NEWSAI_API_KEY")
@pytest.mark.asyncio
async def test_get_media_reviews_wrapper_movie():
    """Integration test for get_media_reviews wrapper with movie."""
    # Call wrapper directly
    CURRENT_MOVIE = "The Lost Bus"
    result = await newsai_wrapper.get_media_reviews(
        title=CURRENT_MOVIE,
        media_type="movie",
        page_size=5,
    )

    # Verify response
    assert result.status_code == 200
    assert result.error is None
    assert isinstance(result.results, list)
    assert result.query == CURRENT_MOVIE

    # Write snapshot
    result_dict = result.model_dump()
    result_copy = result_dict.copy()
    result_copy.pop("fetched_at", None)
    if "results" in result_copy:
        for article in result_copy["results"]:
            article.pop("mc_id", None)

    write_snapshot(
        result_copy,
        "get_media_reviews_wrapper_movie_result.json",
    )

    print(f"\n✓ get_media_reviews wrapper (movie) passed: {len(result.results)} reviews")


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests require valid NEWSAI_API_KEY")
@pytest.mark.asyncio
async def test_get_media_reviews_handler_validation():
    """Integration test for get_media_reviews handler parameter validation."""
    # Test missing title parameter
    mock_req = create_mock_request({"media_type": "movie"})
    response = await newsai_handler.get_media_reviews(mock_req)
    assert "400" in str(response.status)
    result = json.loads(
        response.data.decode("utf-8") if isinstance(response.data, bytes) else response.data
    )
    assert "title parameter is required" in result["error"]

    # Test missing media_type parameter
    mock_req = create_mock_request({"title": "Dune"})
    response = await newsai_handler.get_media_reviews(mock_req)
    assert "400" in str(response.status)
    result = json.loads(
        response.data.decode("utf-8") if isinstance(response.data, bytes) else response.data
    )
    assert "media_type parameter is required" in result["error"]

    # Test invalid media_type
    mock_req = create_mock_request({"title": "Dune", "media_type": "podcast"})
    response = await newsai_handler.get_media_reviews(mock_req)
    assert "400" in str(response.status)
    result = json.loads(
        response.data.decode("utf-8") if isinstance(response.data, bytes) else response.data
    )
    assert "media_type must be 'movie' or 'tv'" in result["error"]

    print("\n✓ get_media_reviews handler validation tests passed")
