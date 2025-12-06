#!/bin/bash
# Run FlixPatrol integration tests
# These tests hit actual FlixPatrol website and require internet connection

set -e

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🎬 FlixPatrol Integration Tests${NC}"
echo "================================"
echo ""

# Change to python_functions directory first
cd "$(dirname "$0")/../../../.."

# Load .env file if it exists
if [ -f ".env" ]; then
    echo -e "${YELLOW}Loading environment from .env file...${NC}"
    set -a
    source .env
    set +a
    echo -e "${GREEN}✅ Environment loaded from .env${NC}"
    echo ""
elif [ -f "../../.env" ]; then
    echo -e "${YELLOW}Loading environment from ../../.env file...${NC}"
    set -a
    source ../../.env
    set +a
    echo -e "${GREEN}✅ Environment loaded from .env${NC}"
    echo ""
fi

# No API keys required for FlixPatrol scraping
echo -e "${GREEN}✅ No API keys required (FlixPatrol scraper)${NC}"
echo ""

# Activate venv if it exists
if [ -d "venv" ]; then
    echo -e "${YELLOW}Activating virtual environment...${NC}"
    source venv/bin/activate
elif [ -d ".venv" ]; then
    echo -e "${YELLOW}Activating virtual environment...${NC}"
    source .venv/bin/activate
fi

echo -e "${YELLOW}Running integration tests...${NC}"
echo ""

# Run integration tests with verbose output
pytest api/subapi/flixpatrol/tests/test_integration.py -v -m integration --snapshot-update"$@"

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✅ All integration tests passed!${NC}"
else
    echo ""
    echo -e "${RED}❌ Some integration tests failed${NC}"
fi

exit $exit_code

