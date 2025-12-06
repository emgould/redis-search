# SchedulesDirect API Tests

This directory contains integration tests for the SchedulesDirect API that hit the real APIs.

## Test Structure

- **`test_integration.py`**: Integration tests that hit the real SchedulesDirect and TMDB APIs
- **`test_core.py`**: Unit tests for `SchedulesDirectService` core functionality (mocked)

## Running Tests

### Integration Tests (Requires Credentials)

Integration tests require SchedulesDirect credentials to be set in the environment:

```bash
export SCHEDULES_DIRECT_USERNAME="your_username"
export SCHEDULES_DIRECT_PASSWORD="your_password"

cd firebase/python_functions
source venv/bin/activate
pytest api/schedulesdirect/tests/test_integration.py -v -m integration
```

### All Tests

```bash
cd firebase/python_functions
source venv/bin/activate
pytest api/schedulesdirect/tests/ -v
```

### Using the Test Runner

```bash
cd firebase/python_functions/api/schedulesdirect/tests
./bin/run_tests.sh
```

## Test Coverage

The integration test suite covers:

- ✅ Token authentication against real SchedulesDirect API
- ✅ Schedule retrieval from real SchedulesDirect API
- ✅ Program metadata retrieval from real SchedulesDirect API
- ✅ Full primetime schedule flow with real APIs (SchedulesDirect + TMDB)
- ✅ Timezone conversion with real data
- ✅ Token caching behavior
- ✅ Error handling with invalid credentials
- ✅ Custom network filtering
- ✅ End-to-end validation of response structure

## Integration Test Requirements

Integration tests are marked with `@pytest.mark.integration` and will be skipped if credentials are not available. These tests:

- Make real API calls to SchedulesDirect
- Make real API calls to TMDB
- Verify end-to-end functionality
- Test with actual data structures
- Validate the complete request/response flow

**Note**: Integration tests may take longer to run and consume API quota. Use them for validation before deployment.

## Why Integration Tests Only?

The focus is on integration tests with live API calls because:
- They validate the actual API integration works correctly
- They catch real-world issues that mocks might miss
- They verify data transformations with real responses
- They ensure the end-to-end flow works as expected
