#!/bin/bash

# Run Comscore tests - both unit and integration tests
# 
# This script runs:
#   1. Unit tests (fast, no network required)
#   2. Integration tests (slower, requires API keys and network)

set -e  # Exit on first error

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get the directory where this script is located (bin/)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Comscore directory is parent of bin/
COMSCORE_DIR="$( cd "$SCRIPT_DIR/.." && pwd )"
# python_functions is 2 levels up from comscore
PYTHON_FUNCTIONS_DIR="$( cd "$COMSCORE_DIR/../.." && pwd )"

# Change to comscore directory to run tests
cd "$COMSCORE_DIR"

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
echo -e "${BLUE}📊 Comscore Test Suite${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# ============================================================================
# STEP 1: Run Unit Tests
# ============================================================================
echo -e "${GREEN}📦 Running Unit Tests...${NC}"
echo -e "${YELLOW}(Fast tests, no network required)${NC}"
echo ""

pytest tests/ -n auto --dist=loadscope -v -x -m "not integration" 
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
echo -e "${YELLOW}(Slower tests, requires API keys and network)${NC}"
echo ""

"$SCRIPT_DIR/run_integration_tests.sh"
integration_exit_code=$?

echo ""
if [ $integration_exit_code -eq 0 ]; then
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}✅ All tests passed!${NC}"
    echo -e "${GREEN}   - Unit tests: ✅${NC}"
    echo -e "${GREEN}   - Integration tests: ✅${NC}"
    echo -e "${GREEN}========================================${NC}"
else
    echo -e "${RED}========================================${NC}"
    echo -e "${RED}❌ Integration tests failed${NC}"
    echo -e "${GREEN}   - Unit tests: ✅${NC}"
    echo -e "${RED}   - Integration tests: ❌${NC}"
    echo -e "${RED}========================================${NC}"
fi

exit $integration_exit_code

