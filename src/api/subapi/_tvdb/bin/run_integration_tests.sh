#!/bin/bash
# Run TVDB integration tests
# These tests hit actual APIs and require environment variables

set -e

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}📺 TVDB Integration Tests${NC}"
echo "================================"
echo ""

# Change to python_functions directory first
cd "$(dirname "$0")/../../.."

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

# Check for required environment variables
if [ -z "$TVDB_API_KEY" ]; then
    echo -e "${RED}❌ Error: TVDB_API_KEY environment variable not set${NC}"
    echo "Please set it in your environment or create a .env file"
    echo ""
    echo "Expected location: firebase/python_functions/.env"
    echo ""
    echo "Example .env file:"
    echo "TVDB_API_KEY=your_tvdb_api_key"
    exit 1
fi

echo -e "${GREEN}✅ Environment variables configured${NC}"
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
pytest api/tvdb/tests/test_integration.py -v -m integration "$@"

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✅ All integration tests passed!${NC}"
else
    echo ""
    echo -e "${RED}❌ Some integration tests failed${NC}"
fi

exit $exit_code

