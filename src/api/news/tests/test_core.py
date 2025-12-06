"""
Unit tests for News core service (core.py only).
Tests core functionality with mocked NewsAPI responses.
"""

from unittest.mock import PropertyMock, patch

import pytest

from api.news.core import NewsService
from api.news.models import MCNewsItem

pytestmark = pytest.mark.unit


@pytest.fixture
def news_service(mock_newsapi_key):
    """Create NewsService instance with mock API key."""
    # Patch the Auth class property before creating service
    with patch("api.news.auth.Auth.news_api_key", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_newsapi_key
        service = NewsService()
        # Trigger property access to initialize newsapi
        _ = service.newsapi
        # Keep the patch active for the duration of the test
        yield service


@pytest.fixture
def mock_article_data():
    """Mock article data for testing."""
    return {
        "source": {"id": "bbc-news", "name": "BBC News"},
        "author": "Test Author",
        "title": "Test Article Title",
        "description": "Test article description",
        "url": "https://example.com/article",
        "urlToImage": "https://example.com/image.jpg",
        "publishedAt": "2024-01-15T10:30:00Z",
        "content": "Test article content...",
    }


def test_news_service_init(mock_newsapi_key):
    """Test NewsService initialization."""
    # Patch the Auth class property before creating service
    with patch("api.news.auth.Auth.news_api_key", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_newsapi_key
        service = NewsService()
        assert service.news_api_key == mock_newsapi_key
        assert service.newsapi is not None


def test_news_service_init_no_key():
    """Test NewsService initialization without API key."""
    # Patch the Auth class property to return None
    with patch("api.news.auth.Auth.news_api_key", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = None
        service = NewsService()
        with pytest.raises(ValueError, match="NewsAPI key is required"):
            _ = service.newsapi


def test_process_article_item(news_service, mock_article_data):
    """Test article processing."""
    article = news_service._process_article_item(mock_article_data)

    assert isinstance(article, MCNewsItem)
    assert article.title == "Test Article Title"
    assert article.author == "Test Author"
    assert article.url == "https://example.com/article"
    assert article.news_source.name == "BBC News"  # news_source is the NewsSource object
    assert article.news_source.id == "bbc-news"
    assert article.source.value == "newsapi"  # source is the MCSources enum
    assert article.mc_id is not None
    assert article.mc_type == "news_article"


def test_process_article_item_with_error(news_service):
    """Test article processing with invalid data."""
    invalid_data = {"title": "Test", "url": ""}
    article = news_service._process_article_item(invalid_data)

    assert isinstance(article, MCNewsItem)
    assert article.title == "Test"
    assert article.news_source.name == "Unknown Source"  # news_source is the NewsSource object
    assert article.source.value == "newsapi"  # source is the MCSources enum
