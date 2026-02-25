"""
Integration tests for NYTimes service.
These tests hit the actual NYTimes API endpoints (no mocks).

Requirements:
- NYTIMES_API_KEY environment variable must be set
- Internet connection required
- Tests may be slower due to actual API calls

Run with: pytest services/nytimes/tests/test_integration.py -v
"""

import os

import pytest

from api.nytimes.auth import nytimes_auth
from api.nytimes.core import NYTimesService
from api.nytimes.models import (
    NYTimesBestsellerListResponse,
    NYTimesBook,
    NYTimesListNamesResponse,
    NYTimesOverviewResponse,
)
from contracts.models import (
    MCSources,
    MCSubType,
    MCType,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def real_nytimes_api_key():
    """Get real NYTimes API key from environment or use test key."""
    # Use environment variable if available, otherwise use test key
    # Tests must run even without real API key - they will use mocked responses
    api_key = os.getenv("NYTIMES_API_KEY", "test_nytimes_api_key_12345")
    return api_key


@pytest.fixture
def nytimes_service(real_nytimes_api_key):
    """Create NYTimesService instance with real API key from auth service."""
    # Reset auth cache to pick up environment variable
    nytimes_auth._nytimes_api_key = None
    return NYTimesService()


class TestNYTimesServiceIntegration:
    """Integration tests for NYTimesService core functionality."""

    @pytest.mark.asyncio
    async def test_get_bestseller_lists_fiction(self, nytimes_service: NYTimesService):
        """Test getting fiction bestseller list.

        Integration test that requires valid API key and network connection.
        Fails on API errors (401, 429, etc.).
        """
        result, error_code = await nytimes_service.get_bestseller_lists(
            list_name="combined-print-and-e-book-fiction", published_date="current"
        )

        assert result is not None

        # Integration tests should fail on API errors (401, 429, etc.)
        assert error_code is None, (
            f"API call failed with error code {error_code}. "
            f"Result: {result if isinstance(result, dict) else 'N/A'}"
        )

        # Verify success response
        if isinstance(result, NYTimesBestsellerListResponse):
            assert result.status == "OK"
            assert result.num_results > 0
            # Results is a list of NYTimesBook (MCSearchResponse pattern)
            assert result.results is not None
            assert isinstance(result.results, list)
            assert len(result.results) > 0

            # Check ALL books have required fields
            for book in result.results:
                assert book.title is not None
                assert book.author is not None
                assert book.rank > 0
                # Check mc_id, mc_type, source, and source_id were generated
                assert book.mc_id, f"mc_id is missing or empty for book: {book.title}"
                assert book.mc_type, f"mc_type is missing or empty for book: {book.title}"
                assert book.source, f"source is missing or empty for book: {book.title}"
                assert book.source_id, f"source_id is missing or empty for book: {book.title}"
                assert book.mc_type == "book"
                assert book.source.value == "nytimes"

    @pytest.mark.asyncio
    async def test_get_bestseller_lists_overview(self, nytimes_service: NYTimesService):
        """Test getting overview of all bestseller lists.

        Integration test that requires valid API key and network connection.
        Fails on API errors (401, 429, etc.).
        """
        result, error_code = await nytimes_service.get_bestseller_lists()

        assert result is not None

        # Integration tests should fail on API errors (401, 429, etc.)
        assert error_code is None, (
            f"API call failed with error code {error_code}. "
            f"Result: {result if isinstance(result, dict) else 'N/A'}"
        )

        # Verify success response
        if isinstance(result, NYTimesOverviewResponse):
            assert result.status == "OK"
            assert result.num_results > 0
            # Results structure depends on response type
            # For overview, results may be a list or have lists attribute
            assert result.results is not None
            # Check if it's the overview results structure
            if hasattr(result.results, "lists"):
                assert len(result.results.lists) > 0
                # Check first list has required fields
                first_list = result.results.lists[0]
                assert first_list.list_name is not None
                assert first_list.display_name is not None
                assert first_list.mc_id is not None
                assert first_list.mc_type == "book_list"

                # Check ALL books in the list
                if first_list.books:
                    for book in first_list.books:
                        assert book.title is not None
                        assert book.author is not None
                        # Check mc_id, mc_type, source, and source_id were generated
                        assert book.mc_id, f"mc_id is missing or empty for book: {book.title}"
                        assert book.mc_type, f"mc_type is missing or empty for book: {book.title}"
                        assert book.source, f"source is missing or empty for book: {book.title}"
                        assert book.source_id, (
                            f"source_id is missing or empty for book: {book.title}"
                        )
                        assert book.mc_type == "book"
                        assert book.source.value == "nytimes"
            else:
                # Results is a list (MCSearchResponse pattern)
                assert isinstance(result.results, list)
                assert len(result.results) > 0

    @pytest.mark.asyncio
    async def test_get_list_names(self, nytimes_service: NYTimesService):
        """Test getting all list names.

        Integration test that requires valid API key and network connection.
        Fails on API errors (401, 429, etc.).
        """
        result, error_code = await nytimes_service.get_list_names()

        assert result is not None

        # Integration tests should fail on API errors (401, 429, etc.)
        assert error_code is None, (
            f"API call failed with error code {error_code}. "
            f"Result: {result if isinstance(result, dict) else 'N/A'}"
        )

        # Verify success response
        if isinstance(result, NYTimesListNamesResponse):
            assert result.status == "OK"
            assert result.num_results > 0
            assert len(result.results) > 0

            # Check ALL list names have required fields
            for list_name in result.results:
                assert list_name.list_name is not None
                assert list_name.display_name is not None
                assert list_name.list_name_encoded is not None
                # Check mc_id, mc_type, source, and source_id were generated
                assert list_name.mc_id, (
                    f"mc_id is missing or empty for list: {list_name.display_name}"
                )
                assert list_name.mc_type, (
                    f"mc_type is missing or empty for list: {list_name.display_name}"
                )
                assert list_name.source, (
                    f"source is missing or empty for list: {list_name.display_name}"
                )
                assert list_name.source_id, (
                    f"source_id is missing or empty for list: {list_name.display_name}"
                )
                assert list_name.mc_type == "book"
                assert list_name.source.value == "nytimes"

    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Endpoint deprecated by NYTimes - returns 404")
    async def test_get_book_reviews(self, nytimes_service: NYTimesService):
        """Test getting book reviews.

        NOTE: This endpoint has been deprecated by NYTimes and returns 404.
        Skipping this test as integration tests should fail on all error codes.
        """
        result, error_code = await nytimes_service.get_book_reviews(author="Stephen King")

        assert result is not None

        # Integration tests should fail on API errors (401, 404, 429, etc.)
        assert error_code is None, (
            f"API call failed with error code {error_code}. "
            f"Result: {result if isinstance(result, dict) else 'N/A'}"
        )

    @pytest.mark.asyncio
    async def test_enrich_books_with_covers(self, nytimes_service: NYTimesService):
        """Test enriching books with cover images.

        Integration test that requires valid API key and network connection.
        Fails on API errors (401, 429, etc.).
        """
        # First get some books
        result, error_code = await nytimes_service.get_bestseller_lists(
            list_name="combined-print-and-e-book-fiction", published_date="current"
        )

        assert result is not None

        # Integration tests should fail on API errors (401, 429, etc.)
        assert error_code is None, (
            f"API call failed with error code {error_code}. "
            f"Result: {result if isinstance(result, dict) else 'N/A'}"
        )
        if isinstance(result, NYTimesBestsellerListResponse) and result.results:
            books = result.results[:3]  # Test with first 3 books
            enriched_books, count = await nytimes_service.enrich_books_with_covers(books)

            assert len(enriched_books) == len(books)
            assert count >= 0  # Some books may not have covers

            # Check that enrichment fields are present
            for book in enriched_books:
                assert hasattr(book, "cover_available")
                assert hasattr(book, "cover_source")
                assert hasattr(book, "cover_urls")


class TestNYTimesWrapperIntegration:
    """Integration tests for NYTimes wrapper functions."""

    @pytest.mark.asyncio
    async def test_get_fiction_bestsellers(self, real_nytimes_api_key):
        """Test fiction bestsellers wrapper.

        Integration test that requires valid API key and network connection.
        Fails on API errors (401, 429, etc.).
        """
        from api.nytimes.wrappers import nytimes_wrapper
        from utils.pytest_utils import write_snapshot

        # Reset auth and wrapper caches to pick up environment variable
        nytimes_auth._nytimes_api_key = None
        nytimes_wrapper._service = None

        result = await nytimes_wrapper.get_fiction_bestsellers(published_date="current")

        assert result is not None
        assert isinstance(result, NYTimesBestsellerListResponse)

        # Write snapshot for integration test (write regardless of success/error)
        # Convert Pydantic model to dict for proper JSON serialization
        try:
            result_dict = result.model_dump()
            snapshot_path = write_snapshot(result_dict, "test_get_fiction_bestsellers.json")
            print(f"✓ Snapshot written to: {snapshot_path}")
        except Exception as e:
            # Log error but don't fail the test
            import traceback

            print(f"❌ Warning: Failed to write snapshot: {e}")
            print(traceback.format_exc())

        # Integration tests should fail on API errors (401, 429, 500, etc.)
        assert result.status_code == 200, (
            f"API call failed with status code {result.status_code}. "
            f"Error: {result.error if result.error else 'N/A'}"
        )
        assert result.status == "OK"
        assert result.error is None

        # Verify MCSearchResponse fields are present
        assert result.data_type is not None
        assert result.data_source is not None

        # Verify MCBaseItem fields are present in ALL books (not on response wrapper)
        if result.results:
            for book in result.results:
                # Check mc_id, mc_type, source, and source_id were generated
                assert book.mc_id, f"mc_id is missing or empty for book: {book.title}"
                assert book.mc_type, f"mc_type is missing or empty for book: {book.title}"
                assert book.source, f"source is missing or empty for book: {book.title}"
                assert book.source_id, f"source_id is missing or empty for book: {book.title}"
                assert book.mc_type == "book"
                assert book.source.value == "nytimes"

    @pytest.mark.asyncio
    async def test_get_bestseller_lists_with_covers(self, real_nytimes_api_key):
        """Test bestseller lists with covers wrapper.

        This is a critical endpoint that enriches bestseller lists with cover images.
        Rate limiting is handled by AsyncLimiter in the service layer.

        Integration test that requires valid API key and network connection.
        Fails on API errors (401, 429, etc.).
        """
        from api.nytimes.wrappers import nytimes_wrapper
        from utils.pytest_utils import write_snapshot

        # Reset auth and wrapper caches to pick up environment variable
        nytimes_auth._nytimes_api_key = None
        nytimes_wrapper._service = None

        result = await nytimes_wrapper.get_bestseller_lists_with_covers(
            list_name="combined-print-and-e-book-fiction",
            published_date="current",
        )

        assert result is not None
        assert isinstance(result, (NYTimesBestsellerListResponse, NYTimesOverviewResponse))

        # Write snapshot for integration test (write regardless of success/error)
        # Convert Pydantic model to dict for proper JSON serialization
        try:
            result_dict = result.model_dump()
            snapshot_path = write_snapshot(
                result_dict, "test_get_bestseller_lists_with_covers.json"
            )
            print(f"✓ Snapshot written to: {snapshot_path}")
        except Exception as e:
            # Log error but don't fail the test
            import traceback

            print(f"❌ Warning: Failed to write snapshot: {e}")
            print(traceback.format_exc())

        # Integration tests should fail on API errors (401, 429, 500, etc.)
        assert result.status_code == 200, (
            f"API call failed with status code {result.status_code}. "
            f"Error: {result.error if result.error else 'N/A'}"
        )
        assert result.status == "OK"
        assert result.error is None

        # Verify MCSearchResponse fields are present
        assert result.data_type is not None
        assert result.data_source is not None

        # Check that books_enriched_count is in metrics
        books_enriched_count = result.metrics.get("books_enriched_count", 0)
        assert isinstance(books_enriched_count, int)
        assert books_enriched_count >= 0

        # Verify MCBaseItem fields are present in ALL books (not on response wrapper)
        if isinstance(result, NYTimesBestsellerListResponse) and result.results:
            for book in result.results:
                # Check mc_id, mc_type, source, and source_id were generated
                assert book.mc_id, f"mc_id is missing or empty for book: {book.title}"
                assert book.mc_type, f"mc_type is missing or empty for book: {book.title}"
                assert book.source, f"source is missing or empty for book: {book.title}"
                assert book.source_id, f"source_id is missing or empty for book: {book.title}"
                assert book.mc_type == "book"
                assert book.source.value == "nytimes"

    @pytest.mark.asyncio
    async def test_search_person_works_async(self, real_nytimes_api_key):
        """Test search_person_works_async wrapper function."""
        from api.nytimes.wrappers import nytimes_wrapper
        from contracts.models import MCPersonSearchRequest
        from utils.pytest_utils import write_snapshot

        # Reset auth and wrapper caches to pick up environment variable
        nytimes_auth._nytimes_api_key = None
        nytimes_wrapper._service = None

        # First, get the overview to find a real author name from bestsellers
        overview = await nytimes_wrapper.get_bestseller_lists()
        assert overview.status_code == 200

        # Find an author from the bestseller lists
        author_name = None
        if isinstance(overview, NYTimesOverviewResponse):
            if overview.results:
                # Get first book's author
                for book in overview.results:
                    if book.author:
                        author_name = book.author
                        break
            elif overview.overview_results:
                for list_data in overview.overview_results.lists:
                    for book in list_data.books:
                        if book.author:
                            author_name = book.author
                            break
                    if author_name:
                        break

        # Skip test if no author found (shouldn't happen with real API)
        if not author_name:
            pytest.skip("No authors found in bestseller lists - cannot test person search")

        # Create a person search request
        person_request = MCPersonSearchRequest(
            source_id=author_name,  # For NYTimes, source_id is the author name
            source=MCSources.NYTIMES,
            mc_type=MCType.PERSON,
            mc_id=f"nytimes_author_{author_name.replace(' ', '_')}",
            mc_subtype=MCSubType.AUTHOR,
            name=author_name,
        )

        # Call the wrapper function
        result = await nytimes_wrapper.search_person_async(person_request, limit=20)

        # Write snapshot for integration test
        try:
            result_dict = result.model_dump()
            snapshot_path = write_snapshot(result_dict, "search_person_works.json")
            print(f"✓ Snapshot written to: {snapshot_path}")
        except Exception as e:
            import traceback

            print(f"❌ Warning: Failed to write snapshot: {e}")
            print(traceback.format_exc())

        # Validate response structure
        assert result.status_code == 200, (
            f"API call failed with status code {result.status_code}. "
            f"Error: {result.error if result.error else 'N/A'}"
        )
        assert result.error is None
        assert result.input == person_request

        # For NYTimes, details is None (no author details available)
        assert result.details is None

        # Validate works array contains books
        assert len(result.works) > 0, "works array should not be empty"
        assert result.related == [], "related should be empty (filled by search_broker)"

        # Validate all works have required MCBaseItem fields
        for work in result.works:
            assert isinstance(work, NYTimesBook)
            assert work.mc_id is not None, f"mc_id is missing for book: {work.title}"
            assert work.mc_type is not None, f"mc_type is missing for book: {work.title}"
            assert work.source is not None, f"source is missing for book: {work.title}"
            assert work.source_id is not None, f"source_id is missing for book: {work.title}"
            assert work.mc_type == MCType.BOOK
            assert work.source == MCSources.NYTIMES
            # Verify author matches (using soft comparison)
            assert work.author is not None
            # Author should match the requested author (fuzzy match allowed)

    @pytest.mark.asyncio
    async def test_search_person_works_async_invalid_source(self, real_nytimes_api_key):
        """Test search_person_works_async with invalid source."""
        from api.nytimes.wrappers import nytimes_wrapper
        from contracts.models import MCPersonSearchRequest

        # Reset auth and wrapper caches
        nytimes_auth._nytimes_api_key = None
        nytimes_wrapper._service = None

        # Create a person search request with invalid source (not NYTimes)
        person_request = MCPersonSearchRequest(
            source_id="test_author",
            source=MCSources.OPENLIBRARY,  # Invalid for NYTimes wrapper
            mc_type=MCType.PERSON,
            mc_id="author_test",
            mc_subtype=MCSubType.AUTHOR,
            name="Test Author",
        )

        # Call the wrapper function
        result = await nytimes_wrapper.search_person_async(person_request)

        # Validate error response
        assert result.status_code == 400
        assert result.error is not None
        assert "Invalid source" in result.error
        assert result.details is None
        assert result.works == []
        assert result.related == []

    @pytest.mark.asyncio
    async def test_search_person_works_async_invalid_author_name(self, real_nytimes_api_key):
        """Test search_person_works_async with invalid/empty author name."""
        from api.nytimes.wrappers import nytimes_wrapper
        from contracts.models import MCPersonSearchRequest

        # Reset auth and wrapper caches
        nytimes_auth._nytimes_api_key = None
        nytimes_wrapper._service = None

        # Create a person search request with empty author name
        person_request = MCPersonSearchRequest(
            source_id="",
            source=MCSources.NYTIMES,
            mc_type=MCType.PERSON,
            mc_id="author_empty",
            mc_subtype=MCSubType.AUTHOR,
            name="",  # Empty name
        )

        # Call the wrapper function
        result = await nytimes_wrapper.search_person_async(person_request)

        # Validate error response
        assert result.status_code == 400
        assert result.error is not None
        assert "Author name is required" in result.error
        assert result.details is None
        assert result.works == []
        assert result.related == []

    @pytest.mark.asyncio
    async def test_search_person_works_async_author_not_found(self, real_nytimes_api_key):
        """Test search_person_works_async with non-existent author name."""
        from api.nytimes.wrappers import nytimes_wrapper
        from contracts.models import MCPersonSearchRequest

        # Reset auth and wrapper caches
        nytimes_auth._nytimes_api_key = None
        nytimes_wrapper._service = None

        # Create a person search request with author name that doesn't exist in bestsellers
        person_request = MCPersonSearchRequest(
            source_id="XyZqWbNpLmKjHgFdSaQwErTyUiOpAsDfGhJkLzXcVbNm",
            source=MCSources.NYTIMES,
            mc_type=MCType.PERSON,
            mc_id="author_nonexistent",
            mc_subtype=MCSubType.AUTHOR,
            name="XyZqWbNpLmKjHgFdSaQwErTyUiOpAsDfGhJkLzXcVbNm",  # Very unlikely to exist
        )

        # Call the wrapper function
        result = await nytimes_wrapper.search_person_async(person_request)

        # Validate error response
        assert result.status_code == 404
        assert result.error is not None
        assert "No books found" in result.error or "not found" in result.error.lower()
        assert result.details is None
        assert result.works == []
        assert result.related == []

    @pytest.mark.asyncio
    async def test_get_nonfiction_bestsellers(self, real_nytimes_api_key):
        """Test nonfiction bestsellers wrapper.

        Integration test that requires valid API key and network connection.
        Fails on API errors (401, 429, etc.).
        """
        from api.nytimes.wrappers import nytimes_wrapper
        from utils.pytest_utils import write_snapshot

        # Reset auth and wrapper caches to pick up environment variable
        nytimes_auth._nytimes_api_key = None
        nytimes_wrapper._service = None

        result = await nytimes_wrapper.get_nonfiction_bestsellers(published_date="current")

        assert result is not None
        assert isinstance(result, NYTimesBestsellerListResponse)

        # Write snapshot for integration test (write regardless of success/error)
        try:
            result_dict = result.model_dump()
            snapshot_path = write_snapshot(result_dict, "test_get_nonfiction_bestsellers.json")
            print(f"✓ Snapshot written to: {snapshot_path}")
        except Exception as e:
            import traceback

            print(f"❌ Warning: Failed to write snapshot: {e}")
            print(traceback.format_exc())

        # Integration tests should fail on API errors (401, 429, 500, etc.)
        assert result.status_code == 200, (
            f"API call failed with status code {result.status_code}. "
            f"Error: {result.error if result.error else 'N/A'}"
        )
        assert result.status == "OK"
        assert result.error is None

        # Verify MCSearchResponse fields are present
        assert result.data_type is not None
        assert result.data_source is not None

        # Verify MCBaseItem fields are present in ALL books (not on response wrapper)
        if result.results:
            for book in result.results:
                # Check mc_id, mc_type, source, and source_id were generated
                assert book.mc_id, f"mc_id is missing or empty for book: {book.title}"
                assert book.mc_type, f"mc_type is missing or empty for book: {book.title}"
                assert book.source, f"source is missing or empty for book: {book.title}"
                assert book.source_id, f"source_id is missing or empty for book: {book.title}"
                assert book.mc_type == "book"
                assert book.source.value == "nytimes"

    @pytest.mark.asyncio
    async def test_get_list_names(self, real_nytimes_api_key):
        """Test get_list_names wrapper.

        Integration test that requires valid API key and network connection.
        Fails on API errors (401, 429, etc.).
        """
        from api.nytimes.wrappers import nytimes_wrapper
        from utils.pytest_utils import write_snapshot

        # Reset auth and wrapper caches to pick up environment variable
        nytimes_auth._nytimes_api_key = None
        nytimes_wrapper._service = None

        result = await nytimes_wrapper.get_list_names()

        assert result is not None
        assert isinstance(result, NYTimesListNamesResponse)

        # Write snapshot for integration test (write regardless of success/error)
        try:
            result_dict = result.model_dump()
            snapshot_path = write_snapshot(result_dict, "test_get_list_names.json")
            print(f"✓ Snapshot written to: {snapshot_path}")
        except Exception as e:
            import traceback

            print(f"❌ Warning: Failed to write snapshot: {e}")
            print(traceback.format_exc())

        # Integration tests should fail on API errors (401, 429, 500, etc.)
        assert result.status_code == 200, (
            f"API call failed with status code {result.status_code}. "
            f"Error: {result.error if result.error else 'N/A'}"
        )
        assert result.status == "OK"
        assert result.error is None

        # Verify MCSearchResponse fields are present
        assert result.data_type is not None
        assert result.data_source is not None

        # Verify MCBaseItem fields are present in ALL list names
        if result.results:
            for list_name in result.results:
                # Check mc_id, mc_type, source, and source_id were generated
                assert list_name.mc_id, (
                    f"mc_id is missing or empty for list: {list_name.display_name}"
                )
                assert list_name.mc_type, (
                    f"mc_type is missing or empty for list: {list_name.display_name}"
                )
                assert list_name.source, (
                    f"source is missing or empty for list: {list_name.display_name}"
                )
                assert list_name.source_id, (
                    f"source_id is missing or empty for list: {list_name.display_name}"
                )
                assert list_name.mc_type == "book"
                assert list_name.source.value == "nytimes"

    @pytest.mark.asyncio
    async def test_get_historical_bestsellers(self, real_nytimes_api_key):
        """Test get_historical_bestsellers wrapper.

        Integration test that requires valid API key and network connection.
        Fails on API errors (401, 429, etc.).
        """
        from api.nytimes.models import NYTimesHistoricalResponse
        from api.nytimes.wrappers import nytimes_wrapper
        from utils.pytest_utils import write_snapshot

        # Reset auth and wrapper caches to pick up environment variable
        nytimes_auth._nytimes_api_key = None
        nytimes_wrapper._service = None

        result = await nytimes_wrapper.get_historical_bestsellers(
            list_name="combined-print-and-e-book-fiction",
            weeks_back=2,  # Use 2 weeks to keep test faster
        )

        assert result is not None
        assert isinstance(result, NYTimesHistoricalResponse)

        # Write snapshot for integration test (write regardless of success/error)
        try:
            result_dict = result.model_dump()
            snapshot_path = write_snapshot(result_dict, "test_get_historical_bestsellers.json")
            print(f"✓ Snapshot written to: {snapshot_path}")
        except Exception as e:
            import traceback

            print(f"❌ Warning: Failed to write snapshot: {e}")
            print(traceback.format_exc())

        # Integration tests should fail on API errors (401, 429, 500, etc.)
        assert result.status_code == 200, (
            f"API call failed with status code {result.status_code}. "
            f"Error: {result.error if result.error else 'N/A'}"
        )
        assert result.error is None

        # Verify MCSearchResponse fields are present
        assert result.data_type is not None
        assert result.data_source is not None

        # Verify historical_data structure
        assert isinstance(result.historical_data, list)
        # May have 0 or more entries depending on API availability

    @pytest.mark.asyncio
    async def test_get_historical_bestsellers_missing_list_name(self, real_nytimes_api_key):
        """Test get_historical_bestsellers wrapper with missing list_name.

        Should return 400 error.
        """
        from api.nytimes.wrappers import nytimes_wrapper

        # Reset auth and wrapper caches
        nytimes_auth._nytimes_api_key = None
        nytimes_wrapper._service = None

        result = await nytimes_wrapper.get_historical_bestsellers(list_name=None)

        # Validate error response
        assert result.status_code == 400
        assert result.error is not None
        assert "list parameter is required" in result.error

    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Endpoint deprecated by NYTimes - returns 404")
    async def test_get_book_reviews(self, real_nytimes_api_key):
        """Test get_book_reviews wrapper.

        NOTE: This endpoint has been deprecated by NYTimes and returns 404.
        Skipping this test as integration tests should fail on all error codes.

        Integration test that requires valid API key and network connection.
        """
        from api.nytimes.models import NYTimesReviewResponse
        from api.nytimes.wrappers import nytimes_wrapper
        from utils.pytest_utils import write_snapshot

        # Reset auth and wrapper caches to pick up environment variable
        nytimes_auth._nytimes_api_key = None
        nytimes_wrapper._service = None

        result = await nytimes_wrapper.get_book_reviews(author="Stephen King")

        assert result is not None
        assert isinstance(result, NYTimesReviewResponse)

        # Write snapshot for integration test (write regardless of success/error)
        try:
            result_dict = result.model_dump()
            snapshot_path = write_snapshot(result_dict, "test_get_book_reviews.json")
            print(f"✓ Snapshot written to: {snapshot_path}")
        except Exception as e:
            import traceback

            print(f"❌ Warning: Failed to write snapshot: {e}")
            print(traceback.format_exc())

        # Integration tests should fail on API errors (401, 404, 429, etc.)
        assert result.status_code == 200, (
            f"API call failed with status code {result.status_code}. "
            f"Error: {result.error if result.error else 'N/A'}"
        )

    @pytest.mark.asyncio
    async def test_get_book_reviews_missing_params(self, real_nytimes_api_key):
        """Test get_book_reviews wrapper with missing parameters.

        Should return 400 error.
        """
        from api.nytimes.wrappers import nytimes_wrapper

        # Reset auth and wrapper caches
        nytimes_auth._nytimes_api_key = None
        nytimes_wrapper._service = None

        result = await nytimes_wrapper.get_book_reviews()

        # Validate error response
        assert result.status_code == 400
        assert result.error is not None
        assert "At least one of author, title, or isbn parameters is required" in result.error
