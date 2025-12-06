#!/bin/bash
# Run integration tests for SchedulesDirect API - these hit the real APIs

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../../../.." && pwd)"
PYTHON_FUNCTIONS_DIR="$PROJECT_ROOT/firebase/python_functions"

cd "$PYTHON_FUNCTIONS_DIR"

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
elif [ -d ".venv" ]; then
    source .venv/bin/activate
fi

if [ -z "$SCHEDULES_DIRECT_USERNAME" ]; then
    echo "SCHEDULES_DIRECT_USERNAME not set, attempting to load from .env file..."
    
    # Look for .env in python_functions directory
    if [ -f "$PYTHON_FUNCTIONS_DIR/.env" ]; then
        echo -e "${GREEN}✓${NC} Found .env at: $PYTHON_FUNCTIONS_DIR/.env"
        # Use set -a to automatically export all variables when sourcing
        set -a
        source "$PYTHON_FUNCTIONS_DIR/.env"
        set +a
    else
        echo -e "${YELLOW}⚠${NC}  No .env file found at: $PYTHON_FUNCTIONS_DIR/.env"
    fi
fi

# Run integration tests if credentials are available
if [ -n "$SCHEDULES_DIRECT_USERNAME" ] && [ -n "$SCHEDULES_DIRECT_PASSWORD" ]; then
    echo "Running integration tests (hitting real APIs)..."
    # Enable cache for integration tests so token caching works
    export ENABLE_CACHE_FOR_TESTS=1
    pytest api/schedulesdirect/tests/test_integration.py \
        -v \
        -m integration
    echo ""
    echo "Integration tests completed!"
else
    echo "ERROR: SCHEDULES_DIRECT_USERNAME and SCHEDULES_DIRECT_PASSWORD must be set"
    echo ""
    echo "To run integration tests:"
    echo "  export SCHEDULES_DIRECT_USERNAME='your_username'"
    echo "  export SCHEDULES_DIRECT_PASSWORD='your_password'"
    echo "  ./bin/run_tests.sh"
    exit 1
fi
