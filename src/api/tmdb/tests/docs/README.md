# TMDB Services Test Suite

Comprehensive unit tests for the modular TMDB service architecture.

## Test Structure

```
tests/
├── __init__.py           # Test package marker
├── conftest.py           # Shared fixtures and test utilities
├── test_models.py        # Pydantic model tests
├── test_core.py          # TMDBService base class tests
├── test_search.py        # TMDBSearchService tests
├── test_person.py        # TMDBPersonService tests
└── test_wrappers.py      # Async wrapper function tests
```

## Running Tests

### Run all tests

```bash
cd firebase/python_functions
pytest services/tmdb/tests/ -v -n auto
```

### Run specific test file

```bash
pytest services/tmdb/tests/test_models.py -v 
pytest services/tmdb/tests/test_core.py -v
pytest services/tmdb/tests/test_search.py -v
pytest services/tmdb/tests/test_person.py -v
pytest services/tmdb/tests/test_wrappers.py -v
```

### Run specific test class

```bash
pytest services/tmdb/tests/test_models.py::TestTMDBMediaItem -v
```

### Run specific test method

```bash
pytest services/tmdb/tests/test_models.py::TestTMDBMediaItem::test_create_movie_item -v
```

### Run with coverage

```bash
pytest services/tmdb/tests/ --cov=api.tmdb --cov-report=html
```

## Test Coverage

### test_models.py (250+ lines)

Tests all Pydantic 2.0 models:

- ✅ TMDBMediaItem - Movie and TV show data models
- ✅ TMDBPersonResult - Person/actor data models
- ✅ TMDBCastMember - Cast member data
- ✅ TMDBVideo - Video/trailer data
- ✅ TMDBWatchProvider - Streaming provider data
- ✅ TMDBKeyword - Keyword data
- ✅ Response Models - All response types
- ✅ Field validation and auto-generation
- ✅ mc_id and mc_type generation
- ✅ Model serialization (model_dump)

### test_core.py (350+ lines)

Tests TMDBService base class:

- ✅ Service initialization
- ✅ HTTP request handling (_make_request)
- ✅ Error handling (404, exceptions)
- ✅ Date sorting utilities (_get_sort_date)
- ✅ Media details fetching (get_media_details)
- ✅ Media enhancement (enhance_media_item)
- ✅ Cast and crew data (_get_cast_and_crew)
- ✅ Videos/trailers (_get_videos)
- ✅ Watch providers (_get_watch_providers)
- ✅ Keywords (_get_keywords)
- ✅ process_media_item function

### test_search.py (400+ lines)

Tests TMDBSearchService:

- ✅ Trending content (movies, TV, multiple pages)
- ✅ Now playing movies (with/without details)
- ✅ Popular TV shows (filtering by date)
- ✅ Multi-search (with person filtering)
- ✅ TV show search (weighted sorting)
- ✅ TV show search (image filtering)
- ✅ TV show search (cache bypass)
- ✅ Keyword search
- ✅ Discover by keywords
- ✅ Keyword syntax support (keyword: "name")
- ✅ Pagination handling

### test_person.py (400+ lines)

Tests TMDBPersonService:

- ✅ Person details (with profile images)
- ✅ Movie credits (with filtering, sorting, limits)
- ✅ TV credits (with filtering, sorting, limits)
- ✅ Talk show filtering
- ✅ Low popularity filtering
- ✅ Complete cast details
- ✅ Person search (with sorting, limits)
- ✅ Invalid person ID handling
- ✅ Recency-based sorting

### test_wrappers.py (500+ lines)

Tests async wrapper functions:

- ✅ get_trending_async
- ✅ get_now_playing_async (with box office sorting)
- ✅ get_popular_tv_async
- ✅ search_multi_async (with Pydantic model conversion)
- ✅ search_tv_shows_async (with cache bypass)
- ✅ discover_by_keywords_async
- ✅ get_media_details_async
- ✅ get_cast_details_async
- ✅ search_people_async
- ✅ movie_credit_search_async
- ✅ tv_credit_search_async
- ✅ Error handling (no token, exceptions)
- ✅ Return type validation (tuple[dict, int | None])

## Fixtures (conftest.py)

### Mock Data Fixtures

- `mock_tmdb_token` - Mock API token
- `mock_movie_data` - Complete movie data
- `mock_tv_data` - Complete TV show data
- `mock_person_data` - Complete person data
- `mock_cast_data` - Cast and crew data
- `mock_videos_data` - Video/trailer data
- `mock_watch_providers_data` - Streaming provider data
- `mock_keywords_data` - Keywords data
- `mock_search_results` - Search results

### Mock HTTP Fixtures

- `mock_aiohttp_response` - Mock aiohttp response
- `mock_aiohttp_session` - Mock aiohttp session
- `mock_tmdb_service_request` - Mock TMDBService._make_request

### Utility Fixtures

- `mock_process_media_item` - Mock process_media_item function

## Test Patterns

### Async Test Pattern

```python
@pytest.mark.asyncio
async def test_async_function(mock_tmdb_token):
    service = TMDBSearchService(mock_tmdb_token)
  
    with patch.object(service, '_make_request') as mock_request:
        mock_request.return_value = {"id": 123}
      
        result = await service.some_method()
      
        assert result is not None
```

### Pydantic Model Test Pattern

```python
def test_model_validation(mock_data):
    model = TMDBMediaItem.model_validate(mock_data)
  
    assert model.id == mock_data["id"]
    assert model.mc_id is not None
    assert model.mc_type == "movie"
```

### Wrapper Function Test Pattern

```python
@pytest.mark.asyncio
async def test_wrapper_function(mock_tmdb_token):
    with patch("api.tmdb.wrappers.TMDBSearchService") as mock_service_class:
        mock_service = AsyncMock()
        mock_service.method.return_value = {"data": "test"}
        mock_service_class.return_value = mock_service
      
        result, error = await wrapper_function(tmdb_token=mock_tmdb_token)
      
        assert error is None
        assert "data" in result
```

## Test Statistics

- **Total Test Files**: 6
- **Total Test Classes**: 30+
- **Total Test Methods**: 150+
- **Lines of Test Code**: ~2000
- **Code Coverage Target**: >90%

## Key Testing Features

### 1. Comprehensive Coverage

- All public methods tested
- All error paths tested
- All edge cases covered
- All Pydantic models validated

### 2. Isolation

- Each test is independent
- Mocked external dependencies
- No actual API calls
- Fast execution

### 3. Realistic Data

- Real TMDB API response structures
- Complete data fixtures
- Edge case data (missing fields, invalid data)

### 4. Error Testing

- Missing tokens
- Invalid IDs
- Network errors
- API errors (404, 500)
- Invalid data

### 5. Type Safety

- Pydantic model validation
- Return type checking
- Type hint verification

## Common Test Commands

```bash
# Run all tests with verbose output
pytest services/tmdb/tests/ -v

# Run with coverage report
pytest services/tmdb/tests/ --cov=api.tmdb --cov-report=term-missing

# Run only failed tests
pytest services/tmdb/tests/ --lf

# Run tests matching pattern
pytest services/tmdb/tests/ -k "search"

# Run with print statements visible
pytest services/tmdb/tests/ -s

# Run in parallel (requires pytest-xdist)
pytest services/tmdb/tests/ -n auto

# Generate HTML coverage report
pytest services/tmdb/tests/ --cov=api.tmdb --cov-report=html
open htmlcov/index.html
```

## Continuous Integration

These tests are designed to run in CI/CD pipelines:

```yaml
# Example GitHub Actions
- name: Run TMDB Tests
  run: |
    cd firebase/python_functions
    pytest services/tmdb/tests/ -v --cov=api.tmdb
```

## Contributing

When adding new functionality:

1. Write tests first (TDD)
2. Ensure >90% coverage
3. Follow existing test patterns
4. Add fixtures to conftest.py
5. Update this README

## Notes

- All tests use mocking - no actual API calls
- Tests are fast (<1 second per test)
- Tests are deterministic and repeatable
- Tests follow pytest best practices
- Tests use async/await properly
