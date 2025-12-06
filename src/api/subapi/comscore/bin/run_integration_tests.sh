#!/bin/bash

# Integration Test Runner for Comscore Service
# Runs tests against actual Comscore API endpoints
# 
# Usage: Run from the comscore directory:
#   cd firebase/python_functions/services/comscore
#   ./bin/run_integration_tests.sh -v
#   ./bin/run_integration_tests.sh test_get_domestic_rankings
#   ./bin/run_integration_tests.sh test_get_domestic_rankings -v

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Comscore Integration Test Runner${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Get the directory where this script is located (bin/)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Comscore directory is parent of bin/
COMSCORE_DIR="$( cd "$SCRIPT_DIR/.." && pwd )"
# python_functions is 2 levels up from comscore
PYTHON_FUNCTIONS_DIR="$( cd "$COMSCORE_DIR/../.." && pwd )"

echo "Comscore directory: $COMSCORE_DIR"
echo "Python functions directory: $PYTHON_FUNCTIONS_DIR"
echo ""

# Disable cloud storage for local integration tests
export FIRESTORE_EMULATOR_HOST="localhost:8080"  # Trick cache into thinking we're in emulator mode
echo -e "${GREEN}✓${NC} Cloud storage disabled for local testing"
echo ""

# Note: Comscore API requires no authentication
echo -e "${GREEN}✓${NC} Comscore API requires no authentication"
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
            COVERAGE="--cov=services/comscore --cov-report=html --cov-report=term"
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
            echo "  TEST_NAME           Optional test name to run (e.g., test_get_domestic_rankings)"
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
            echo "  ./bin/run_integration_tests.sh test_get_domestic_rankings"
            echo "  ./bin/run_integration_tests.sh test_get_domestic_rankings -v"
            echo "  ./bin/run_integration_tests.sh -k rankings -vv"
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

# Change to Comscore directory to run tests
cd "$COMSCORE_DIR"

# Build pytest command (relative to comscore directory)
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
echo -e "${YELLOW}Note: These tests hit actual Comscore API endpoints${NC}"
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

