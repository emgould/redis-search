"""
Unit tests for NYTimes Models.
"""

import pytest

from api.nytimes.models import (
    NYTimesBestsellerList,
    NYTimesBestsellerListResponse,
    NYTimesBestsellerListResults,
    NYTimesBook,
)

pytestmark = pytest.mark.unit


def test_nytimes_book_mc_fields_generation():
    """NYTimesBook should generate mc_id and mc_type automatically."""
    book = NYTimesBook(
        title="Test Book",
        author="Test Author",
        primary_isbn13="9781234567890",
        description="desc",
    )

    assert book.mc_type == "book"
    assert book.mc_id is not None
    assert book.mc_id.startswith("book_")


def test_nytimes_bestseller_list_mc_fields_generation():
    """NYTimesBestsellerList should generate mc_id and custom mc_type 'book_list'."""
    bl = NYTimesBestsellerList(
        list_id=704,
        list_name="Combined Print and E-Book Fiction",
        list_name_encoded="combined-print-and-e-book-fiction",
        display_name="Combined Print & E-Book Fiction",
        updated="WEEKLY",
        books=[],
    )

    assert bl.mc_type == "book_list"
    assert bl.mc_id == "nyt_list_704"


def test_bestseller_response_parsing():
    """Validate bestseller list response parsing into typed models."""
    payload = {
        "status": "OK",
        "num_results": 1,
        "results": {
            "bestsellers_date": "2024-01-13",
            "published_date": "2024-01-28",
            "published_date_description": "latest",
            "previous_published_date": None,
            "next_published_date": None,
            "list_name": "Combined Print and E-Book Fiction",
            "list_name_encoded": "combined-print-and-e-book-fiction",
            "display_name": "Combined Print & E-Book Fiction",
            "normal_list_ends_at": 0,
            "updated": "WEEKLY",
            "books": [
                {
                    "rank": 1,
                    "rank_last_week": 0,
                    "weeks_on_list": 1,
                    "asterisk": 0,
                    "dagger": 0,
                    "primary_isbn10": "1234567890",
                    "primary_isbn13": "9781234567890",
                    "publisher": "Test Publisher",
                    "description": "Test",
                    "price": "10.00",
                    "title": "TEST BOOK",
                    "author": "Test Author",
                    "contributor": "by Test Author",
                    "contributor_note": "",
                    "book_image": None,
                    "book_image_width": None,
                    "book_image_height": None,
                    "amazon_product_url": None,
                    "age_group": "",
                    "book_review_link": None,
                    "first_chapter_link": None,
                    "sunday_review_link": None,
                    "article_chapter_link": None,
                    "isbns": [{"isbn10": "1234567890", "isbn13": "9781234567890"}],
                    "buy_links": [],
                    "book_uri": None,
                }
            ],
            "corrections": [],
        },
    }

    parsed = NYTimesBestsellerListResponse.model_validate(payload)
    assert parsed.status == "OK"
    # Results is now a list of NYTimesBook (MCSearchResponse pattern)
    assert isinstance(parsed.results, list)
    assert len(parsed.results) == 1
    first = parsed.results[0]
    assert isinstance(first, NYTimesBook)
    assert first.mc_id is not None
    # Verify list_results is preserved for backward compatibility
    assert parsed.list_results is not None
    assert isinstance(parsed.list_results, NYTimesBestsellerListResults)
    assert parsed.total_results == 1
    assert parsed.data_type == "book"
    assert parsed.data_source == "NYTimes Bestseller List"
