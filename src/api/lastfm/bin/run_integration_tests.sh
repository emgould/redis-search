#!/bin/bash
# Run LastFM integration tests
# These tests hit actual APIs and require environment variables

set -e

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🎵 LastFM Integration Tests${NC}"
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
if [ -z "$LASTFM_API_KEY" ]; then
    echo -e "${RED}❌ Error: LASTFM_API_KEY environment variable not set${NC}"
    echo "Please set it in your environment or create a .env file"
    echo ""
    echo "Expected location: firebase/python_functions/.env"
    echo ""
    echo "Example .env file:"
    echo "LASTFM_API_KEY=your_lastfm_api_key"
    echo "SPOTIFY_CLIENT_ID=your_spotify_client_id"
    echo "SPOTIFY_CLIENT_SECRET=your_spotify_client_secret"
    exit 1
fi

if [ -z "$SPOTIFY_CLIENT_ID" ] || [ -z "$SPOTIFY_CLIENT_SECRET" ]; then
    echo -e "${RED}❌ Error: SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_SECRET not set${NC}"
    echo "Please set them in your environment or create a .env file"
    echo ""
    echo "Expected location: firebase/python_functions/.env"
    echo ""
    echo "Example .env file:"
    echo "LASTFM_API_KEY=your_lastfm_api_key"
    echo "SPOTIFY_CLIENT_ID=your_spotify_client_id"
    echo "SPOTIFY_CLIENT_SECRET=your_spotify_client_secret"
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
pytest api/lastfm/tests/test_integration.py -v -m integration --snapshot-update "$@"

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✅ All integration tests passed!${NC}"
else
    echo ""
    echo -e "${RED}❌ Some integration tests failed${NC}"
fi

exit $exit_code
