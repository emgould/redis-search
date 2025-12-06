#!/bin/bash

# Run FlixPatrol tests - both unit and integration tests
# 
# This script runs:
#   1. Unit tests (fast, no network required)
#   2. Integration tests (slower, requires internet connection)

set -e  # Exit on first error

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get the directory where this script is located (bin/)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# FlixPatrol directory is parent of bin/
FLIXPATROL_DIR="$( cd "$SCRIPT_DIR/.." && pwd )"
# python_functions is 3 levels up from flixpatrol (flixpatrol -> subapi -> api -> python_functions)
PYTHON_FUNCTIONS_DIR="$( cd "$FLIXPATROL_DIR/../../.." && pwd )"

# Change to flixpatrol directory to run tests
cd "$FLIXPATROL_DIR"

# Check if venv exists in python_functions directory
if [ -d "$PYTHON_FUNCTIONS_DIR/venv" ]; then
    echo "Activating virtual environment..."
    source "$PYTHON_FUNCTIONS_DIR/venv/bin/activate"
elif [ -d "$PYTHON_FUNCTIONS_DIR/.venv" ]; then
    echo "Activating virtual environment..."
    source "$PYTHON_FUNCTIONS_DIR/.venv/bin/activate"
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}🎬 FlixPatrol Test Suite${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# ============================================================================
# STEP 1: Run Unit Tests
# ============================================================================
echo -e "${GREEN}📦 Running Unit Tests...${NC}"
echo -e "${YELLOW}(Fast tests, no network required)${NC}"
echo ""
# Check if the __snapshots__ directory exists, else force snapshot update
if [ ! -d "tests/snapshots" ]; then
    SNAPSHOT_FLAG="--snapshot-update"
    echo -e "${YELLOW}snapshots directory not found, will run pytest with --snapshot-update to create snapshots${NC}"
else
    SNAPSHOT_FLAG=""
fi

pytest tests/ -n auto --dist=loadscope -v -x -m "not integration" $SNAPSHOT_FLAG
unit_exit_code=$?

if [ $unit_exit_code -ne 0 ]; then
    echo ""
    echo -e "${RED}❌ Unit tests failed. Stopping here.${NC}"
    exit $unit_exit_code
fi

echo ""
echo -e "${GREEN}✅ All unit tests passed!${NC}"
echo ""

# ============================================================================
# STEP 2: Run Integration Tests
# ============================================================================
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}🌐 Running Integration Tests...${NC}"
echo -e "${YELLOW}(Slower tests, requires internet connection)${NC}"
echo ""

# Change to python_functions directory for integration tests
cd "$PYTHON_FUNCTIONS_DIR"

# Load .env file if it exists
if [ -f ".env" ]; then
    echo -e "${YELLOW}Loading environment from .env file...${NC}"
    set -a
    source .env
    set +a
    echo -e "${GREEN}✅ Environment loaded from .env${NC}"
    echo ""
fi

# Run integration tests
if [ ! -d "api/subapi/flixpatrol/tests/snapshots" ]; then
    SNAPSHOT_FLAG="--snapshot-update"
    echo -e "${YELLOW}snapshots directory not found, will run pytest with --snapshot-update to create snapshots${NC}"
else
    SNAPSHOT_FLAG=""
fi
pytest api/subapi/flixpatrol/tests/test_integration.py -v -m integration $SNAPSHOT_FLAG "$@"
integration_exit_code=$?

echo ""
if [ $integration_exit_code -eq 0 ]; then
    echo -e "${GREEN}✅ All integration tests passed!${NC}"
else
    echo -e "${RED}❌ Some integration tests failed${NC}"
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Test Summary${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "Unit Tests: ${GREEN}✅ PASSED${NC}"
if [ $integration_exit_code -eq 0 ]; then
    echo -e "Integration Tests: ${GREEN}✅ PASSED${NC}"
else
    echo -e "Integration Tests: ${RED}❌ FAILED${NC}"
fi
echo ""

exit $integration_exit_code

