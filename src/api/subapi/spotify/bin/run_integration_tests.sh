#!/bin/bash

# Run Spotify integration tests only
# 
# This script runs integration tests (requires internet connection and API credentials)

set -e  # Exit on first error

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get the directory where this script is located (bin/)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Spotify directory is parent of bin/
SPOTIFY_DIR="$( cd "$SCRIPT_DIR/.." && pwd )"
# python_functions is 2 levels up from spotify
PYTHON_FUNCTIONS_DIR="$( cd "$SPOTIFY_DIR/../.." && pwd )"

# Check if venv exists in python_functions directory
if [ -d "$PYTHON_FUNCTIONS_DIR/venv" ]; then
    echo "Activating virtual environment..."
    source "$PYTHON_FUNCTIONS_DIR/venv/bin/activate"
elif [ -d "$PYTHON_FUNCTIONS_DIR/.venv" ]; then
    echo "Activating virtual environment..."
    source "$PYTHON_FUNCTIONS_DIR/.venv/bin/activate"
fi

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

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}🌐 Spotify Integration Tests${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Run integration tests
pytest api/spotify/tests/test_integration.py -v -m integration "$@"
integration_exit_code=$?

echo ""
if [ $integration_exit_code -eq 0 ]; then
    echo -e "${GREEN}✅ All integration tests passed!${NC}"
else
    echo -e "${RED}❌ Some integration tests failed${NC}"
fi
echo ""

exit $integration_exit_code

