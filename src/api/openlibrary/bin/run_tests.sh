#!/bin/bash
# Run OpenLibrary service tests
# Usage: ./run_tests.sh [pytest args]

set -e

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SERVICE_DIR="$(dirname "$SCRIPT_DIR")"
PYTHON_FUNCTIONS_DIR="$(dirname "$(dirname "$SERVICE_DIR")")"

echo "🧪 Running OpenLibrary Service Tests"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Service: $SERVICE_DIR"
echo ""

# Change to python_functions directory
cd "$PYTHON_FUNCTIONS_DIR"

# Run pytest excluding integration tests (they run separately)
pytest api/openlibrary/tests/ \
    -v \
    --tb=short \
    --color=yes \
    -m "not integration" \
    "$@"

echo ""
echo "✅ Tests completed!"

# ============================================================================
# STEP 2: Run Integration Tests
# ============================================================================
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}🌐 Running Integration Tests...${NC}"
echo -e "${YELLOW}(Slower tests, requires API keys and network)${NC}"
echo ""

"$SCRIPT_DIR/run_integration_tests.sh" "$@"
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
