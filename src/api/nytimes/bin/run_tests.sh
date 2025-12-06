#!/bin/bash

# Run NYTimes tests - both unit and integration tests
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

DIR=$(dirname $0)
cd $(dirname $0)

# Activate virtual environment if it exists
if [ -d "../../../venv" ]; then
    echo "Activating virtual environment..."
    source ../../../venv/bin/activate
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}📚 NYTimes Test Suite${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# ============================================================================
# STEP 1: Run Unit Tests
# ============================================================================
echo -e "${GREEN}📦 Running Unit Tests...${NC}"
echo -e "${YELLOW}(Fast tests, no network required)${NC}"
echo ""

pytest ../tests/ -n auto --dist=loadscope -v -x -m "not integration"
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

./run_integration_tests.sh
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
