#!/bin/bash

# Run NewsAI tests - both unit and integration tests
#
# This script runs:
#   1. Unit tests (fast, no network required)
#   2. Integration tests (slower, requires API keys and network)

# Don't use set -e here because we need to handle pytest exit codes manually
# set -e  # Exit on first error

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

cd "$PROJECT_ROOT"

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}📰 NewsAI Service Test Suite${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# ============================================================================
# STEP 1: Run Unit Tests
# ============================================================================
echo -e "${GREEN}📦 Running Unit Tests...${NC}"
echo -e "${YELLOW}(Fast tests, no network required)${NC}"
echo ""

# Check if snapshots directory exists, else force snapshot update
if [ ! -d "api/newsai/tests/snapshots" ]; then
    SNAPSHOT_FLAG="--snapshot-update"
    echo -e "${YELLOW}snapshots directory not found, will run pytest with --snapshot-update to create snapshots${NC}"
else
    SNAPSHOT_FLAG=""
fi

pytest api/newsai/tests/ \
    -v \
    -n auto --dist=loadscope \
    -x \
    -m "not integration" \
    --tb=short \
    --color=yes \
    -W ignore::DeprecationWarning \
    $SNAPSHOT_FLAG \
    $PYTEST_VERBOSE \
    $PYTEST_PATTERN \
    "$@"

unit_exit_code=$?

# pytest-snapshots can exit with code 1 when snapshots are modified/created
# If snapshots were created successfully, treat exit code 1 as success
if [ $unit_exit_code -eq 1 ] && [ -n "$SNAPSHOT_FLAG" ]; then
    echo ""
    echo -e "${YELLOW}⚠️  Snapshots were updated (expected with --snapshot-update)${NC}"
    unit_exit_code=0
fi

# If using a pattern filter and no tests ran, don't fail - the pattern might match integration tests
if [ $unit_exit_code -eq 5 ] && [ -n "$PYTEST_PATTERN" ]; then
    echo ""
    echo -e "${YELLOW}⚠️  No unit tests matched pattern - will check integration tests${NC}"
    unit_exit_code=0
fi

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

# Only pass $1 if it's not empty
if [ -n "$1" ]; then
    "$SCRIPT_DIR/run_integration_tests.sh" "$1"
else
    "$SCRIPT_DIR/run_integration_tests.sh"
fi
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
