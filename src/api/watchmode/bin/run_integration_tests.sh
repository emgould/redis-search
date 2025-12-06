#!/bin/bash

# Integration Test Runner for Watchmode Service
# Runs tests against actual Watchmode API endpoints
# 
# Usage: Run from the watchmode directory:
#   cd firebase/python_functions/services/watchmode
#   ./bin/run_integration_tests.sh -v
#   ./bin/run_integration_tests.sh test_get_title_details_movie
#   ./bin/run_integration_tests.sh test_get_title_details_movie -v

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Watchmode Integration Test Runner${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Get the directory where this script is located (bin/)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Watchmode directory is parent of bin/
WATCHMODE_DIR="$( cd "$SCRIPT_DIR/.." && pwd )"
# python_functions is 2 levels up from watchmode
PYTHON_FUNCTIONS_DIR="$( cd "$WATCHMODE_DIR/../.." && pwd )"

echo "Watchmode directory: $WATCHMODE_DIR"
echo "Python functions directory: $PYTHON_FUNCTIONS_DIR"
echo ""

# Disable cloud storage for local integration tests
export FIRESTORE_EMULATOR_HOST="localhost:8080"  # Trick cache into thinking we're in emulator mode
echo -e "${GREEN}✓${NC} Cloud storage disabled for local testing"
echo ""

# Try to load WATCHMODE_API_KEY from .env files if not already set
if [ -z "$WATCHMODE_API_KEY" ]; then
    echo "WATCHMODE_API_KEY not set, attempting to load from .env file..."
    
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

# Explicitly export WATCHMODE_API_KEY to ensure it's available to pytest subprocess
export WATCHMODE_API_KEY
export TMDB_READ_TOKEN

# Check if WATCHMODE_API_KEY is now set
if [ -z "$WATCHMODE_API_KEY" ]; then
    echo -e "${RED}ERROR: WATCHMODE_API_KEY environment variable is not set${NC}"
    echo ""
    echo "Please either:"
    echo "  1. Create $PYTHON_FUNCTIONS_DIR/.env with WATCHMODE_API_KEY=your_key"
    echo "  2. Export it manually: export WATCHMODE_API_KEY='your_key_here'"
    exit 1
fi

echo -e "${GREEN}✓${NC} WATCHMODE_API_KEY is set"
echo ""

# Check if venv exists in python_functions directory
if [ -d "$PYTHON_FUNCTIONS_DIR/venv" ]; then
    echo -e "${GREEN}✓${NC} Activating virtual environment..."
    source "$PYTHON_FUNCTIONS_DIR/venv/bin/activate"
elif [ -d "$PYTHON_FUNCTIONS_DIR/.venv" ]; then
    echo -e "${GREEN}✓${NC} Activating virtual environment..."
    source "$PYTHON_FUNCTIONS_DIR/.venv/bin/activate"
else
    echo -e "${YELLOW}⚠${NC}  No virtual environment found (venv or .venv)"
    echo "Continuing without venv activation..."
fi

echo ""

# Parse command line arguments
VERBOSE=""
SPECIFIC_TEST=""
COVERAGE=""
MARKERS=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE="-v"
            shift
            ;;
        -vv)
            VERBOSE="-vv"
            shift
            ;;
        -s|--show-output)
            VERBOSE="-s"
            shift
            ;;
        -c|--coverage)
            COVERAGE="--cov=services/watchmode --cov-report=html --cov-report=term"
            shift
            ;;
        -k)
            SPECIFIC_TEST="-k $2"
            shift 2
            ;;
        -m|--marker)
            MARKERS="-m $2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: ./bin/run_integration_tests.sh [TEST_NAME] [OPTIONS]"
            echo ""
            echo "Arguments:"
            echo "  TEST_NAME           Optional test name to run (e.g., test_get_title_details_movie)"
            echo ""
            echo "Options:"
            echo "  -v, --verbose       Verbose output"
            echo "  -vv                 Very verbose output"
            echo "  -s, --show-output   Show print statements"
            echo "  -c, --coverage      Run with coverage report"
            echo "  -k PATTERN          Run tests matching pattern"
            echo "  -m MARKER           Run tests with specific marker"
            echo "  -h, --help          Show this help message"
            echo ""
            echo "Examples:"
            echo "  ./bin/run_integration_tests.sh"
            echo "  ./bin/run_integration_tests.sh -v"
            echo "  ./bin/run_integration_tests.sh test_get_title_details_movie"
            echo "  ./bin/run_integration_tests.sh test_get_title_details_movie -v"
            echo "  ./bin/run_integration_tests.sh -k movie -vv"
            exit 0
            ;;
        -*)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
        *)
            # If it doesn't start with -, treat it as a test name
            if [ -z "$SPECIFIC_TEST" ]; then
                SPECIFIC_TEST="-k $1"
                echo -e "${GREEN}Running specific test: $1${NC}"
                echo ""
            else
                echo -e "${RED}Error: Multiple test names specified${NC}"
                exit 1
            fi
            shift
            ;;
    esac
done

# Change to Watchmode directory to run tests
cd "$WATCHMODE_DIR"

# Build pytest command (relative to watchmode directory)
PYTEST_CMD="pytest tests/test_integration.py -x"

if [ -n "$VERBOSE" ]; then
    PYTEST_CMD="$PYTEST_CMD $VERBOSE"
fi

if [ -n "$SPECIFIC_TEST" ]; then
    PYTEST_CMD="$PYTEST_CMD $SPECIFIC_TEST"
fi

if [ -n "$MARKERS" ]; then
    PYTEST_CMD="$PYTEST_CMD $MARKERS"
fi

if [ -n "$COVERAGE" ]; then
    PYTEST_CMD="$PYTEST_CMD $COVERAGE"
fi

# Add color output
PYTEST_CMD="$PYTEST_CMD --color=yes"

echo -e "${GREEN}Running integration tests...${NC}"
echo "Working directory: $(pwd)"
echo "Command: $PYTEST_CMD"
echo ""
echo -e "${YELLOW}Note: These tests hit actual Watchmode API endpoints${NC}"
echo -e "${YELLOW}      Tests may take longer than unit tests${NC}"
echo ""

# Run the tests
if eval $PYTEST_CMD; then
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}✓ All integration tests passed!${NC}"
    echo -e "${GREEN}========================================${NC}"
    exit 0
else
    echo ""
    echo -e "${RED}========================================${NC}"
    echo -e "${RED}✗ Some integration tests failed${NC}"
    echo -e "${RED}========================================${NC}"
    exit 1
fi
