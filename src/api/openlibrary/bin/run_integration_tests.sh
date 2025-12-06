#!/bin/bash
# Run OpenLibrary integration tests
# Usage: ./run_integration_tests.sh

set -e

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SERVICE_DIR="$(dirname "$SCRIPT_DIR")"
PYTHON_FUNCTIONS_DIR="$(dirname "$(dirname "$SERVICE_DIR")")"

echo "🌐 Running OpenLibrary Integration Tests"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Service: $SERVICE_DIR"
echo ""
echo "⚠️  Note: Integration tests make real API calls"
echo ""

# Change to python_functions directory
cd "$PYTHON_FUNCTIONS_DIR"

# Run only integration tests
pytest api/openlibrary/tests/test_integration.py \
    -v \
    --tb=short \
    --color=yes \
    -m "not slow" \
    "$@"

echo ""
echo "✅ Integration tests completed!"
