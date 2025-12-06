#!/bin/bash
# Run RottenTomatoes API tests

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
API_DIR="$(dirname "$SCRIPT_DIR")"
PYTHON_FUNCTIONS_DIR="$(dirname "$(dirname "$API_DIR")")"

# Change to python_functions directory
cd "$PYTHON_FUNCTIONS_DIR"

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
elif [ -d ".venv" ]; then
    source .venv/bin/activate
fi

# Run interactive test
echo "Starting RottenTomatoes Interactive Test..."
echo "============================================"
python -m api.rottentomatoes.bin.interactive_test "$@"

