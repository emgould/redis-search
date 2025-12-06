"""
Unit tests for NewsAI core service.
Tests article and event processing logic.
"""

import pytest

from api.newsai.core import NewsAIService
from api.newsai.event_models import MCEventItem
from api.newsai.models import MCNewsItem

pytestmark = pytest.mark.unit


def test_process_article_item_basic():
    """Test processing a basic article item from Event Registry."""
    service = NewsAIService()

    article_data = {
        "uri": "123456",
        "title": "Test Article",
        "body": "This is a test article body",
        "url": "https://example.com/article",
        "image": "https://example.com/image.jpg",
        "dateTime": "2024-01-15T10:30:00Z",
        "lang": "eng",
        "source": {
            "uri": "test-source",
            "title": "Test Source",
        },
    }

    article = service._process_article_item(article_data)

    assert isinstance(article, MCNewsItem)
    assert article.uri == "123456"
    assert article.title == "Test Article"
    assert article.url == "https://example.com/article"
    assert article.url_to_image == "https://example.com/image.jpg"
    assert article.published_at == "2024-01-15T10:30:00+00:00"  # ISO format with timezone
    assert article.lang == "eng"
    assert article.news_source.name == "Test Source"
    assert article.mc_id is not None
    assert article.source_id == "123456"


def test_process_article_item_with_date_time_fields():
    """Test processing article with separate date and time fields."""
    service = NewsAIService()

    article_data = {
        "uri": "789",
        "title": "Test Article",
        "url": "https://example.com/article",
        "date": "2024-01-15",
        "time": "10:30:00",
        "source": {"title": "Test Source"},
    }

    article = service._process_article_item(article_data)

    assert article.date == "2024-01-15"
    assert article.time == "10:30:00"
    # published_at is set from date field (not combined with time in this case)
    assert article.published_at == "2024-01-15"


def test_process_article_item_error_handling():
    """Test error handling when processing malformed article data."""
    service = NewsAIService()

    # Minimal/malformed data
    article_data = {
        "title": "Error Test",
    }

    article = service._process_article_item(article_data)

    # Should return a minimal valid article, not raise exception
    assert isinstance(article, MCNewsItem)
    assert article.title == "Error Test"
    assert article.mc_id is not None


def test_process_event_item_basic():
    """Test processing a basic event item from Event Registry."""
    service = NewsAIService()

    event_data = {
        "uri": "eng-11134778",
        "title": "Test Event Title",
        "summary": "Test event summary",
        "eventDate": "2025-11-11",
        "totalArticleCount": "14",
        "concepts": [
            {
                "uri": "http://en.wikipedia.org/wiki/Test",
                "type": "wiki",
                "score": 100,
                "title": "Test Concept",
            }
        ],
        "categories": [
            {
                "uri": "dmoz/Arts/Television/Programs",
                "label": "dmoz/Arts/Television/Programs",
                "wgt": 36,
            }
        ],
        "images": ["https://example.com/image.jpg"],
        "socialScore": "0",
        "sentiment": 0.5,
        "wgt": 500515200,
        "relevance": 8,
    }

    event = service._process_event_item(event_data)

    assert isinstance(event, MCEventItem)
    assert event.uri == "eng-11134778"
    assert event.title == "Test Event Title"
    assert event.summary == "Test event summary"
    assert event.event_date == "2025-11-11"
    assert event.total_article_count == 14  # Should be converted to int
    assert len(event.concepts) == 1
    assert len(event.categories) == 1
    assert len(event.images) == 1
    assert event.mc_id is not None
    assert event.source_id == "eng-11134778"


def test_process_event_item_with_multilingual_title():
    """Test processing event with multilingual title."""
    service = NewsAIService()

    event_data = {
        "uri": "test-123",
        "title": {"eng": "English Title", "spa": "Título en Español"},
        "eventDate": "2025-11-11",
        "totalArticleCount": 5,
    }

    event = service._process_event_item(event_data)

    # Title should be normalized to English string
    assert isinstance(event.title, str)
    assert event.title == "English Title"


def test_process_event_item_error_handling():
    """Test error handling when processing malformed event data."""
    service = NewsAIService()

    # Minimal data
    event_data = {
        "uri": "error-test",
    }

    event = service._process_event_item(event_data)

    # Should return a minimal valid event, not raise exception
    assert isinstance(event, MCEventItem)
    assert event.uri == "error-test"
    assert event.mc_id is not None
