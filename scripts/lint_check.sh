#!/bin/bash
# Lint and type check Python functions

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Running Python linting and type checking...${NC}\n"

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
cd ../

# Check if virtual environment exists and activate it
if [ -d "venv" ]; then
    echo -e "${YELLOW}Activating virtual environment...${NC}"
    source venv/bin/activate
else
    echo -e "${RED}Virtual environment not found. Please run setup.sh first.${NC}"
    exit 1
fi

# Check if required packages are installed
if ! python -c "import ruff" 2>/dev/null; then
    echo -e "${YELLOW}Installing development dependencies...${NC}"
    pip install -q -r requirements-dev.txt
fi

# Counter for errors
ERRORS=0

# Run Ruff linter
echo -e "\n${YELLOW}=== Running Ruff Linter ===${NC}"
if ruff check . --exclude venv --exclude __pycache__ --exclude tests --exclude .pytest_cache --fix; then
    echo -e "${GREEN}✓ Ruff linting passed${NC}"
else
    echo -e "${RED}✗ Ruff linting failed${NC}"
    ERRORS=$((ERRORS + 1))
fi

# Run mypy type checker
echo -e "\n${YELLOW}=== Running mypy Type Checker ===${NC}"
if mypy . --exclude venv --exclude __pycache__ --exclude tests --exclude .pytest_cache; then
    echo -e "${GREEN}✓ Type checking passed${NC}"
else
    echo -e "${RED}✗ Type checking failed${NC}"
    ERRORS=$((ERRORS + 1))
fi

# Summary
echo -e "\n${YELLOW}=== Summary ===${NC}"
if [ $ERRORS -eq 0 ]; then
    echo -e "${GREEN}All checks passed!${NC}"
    exit 0
else
    echo -e "${RED}$ERRORS check(s) failed${NC}"
    exit 1
fi

