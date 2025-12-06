#!/bin/bash

# Run YouTube tests - both unit and integration tests
# 
# This script runs:
#   1. Unit tests (fast, no network required)
#   2. Integration tests (slower, requires API key and network)

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
elif [ -d "../../../.venv" ]; then
    echo "Activating virtual environment..."
    source ../../../.venv/bin/activate
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}📺 YouTube Test Suite${NC}"
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
echo -e "${YELLOW}(Slower tests, requires API key and network)${NC}"
echo ""

# Change to python_functions directory for integration tests
cd ../../..

# Load .env file if it exists
if [ -f ".env" ]; then
    echo -e "${YELLOW}Loading environment from .env file...${NC}"
    set -a
    source .env
    set +a
    echo -e "${GREEN}✅ Environment loaded${NC}"
    echo ""
fi

# Check for required environment variables
if [ -z "$YOUTUBE_API_KEY" ]; then
    echo -e "${YELLOW}⚠️  Warning: YOUTUBE_API_KEY not set${NC}"
    echo -e "${YELLOW}Skipping integration tests${NC}"
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}✅ Unit tests passed (integration tests skipped)${NC}"
    echo -e "${GREEN}========================================${NC}"
    exit 0
fi

# Run integration tests
pytest api/youtube/tests/test_integration.py -v -m integration
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



