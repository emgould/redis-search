#!/bin/bash
set -e

# Check for Python 3.11
if ! command -v python3.11 &> /dev/null; then
    echo "❌ Python 3.11 not found. Please install Python 3.11:"
    echo "   pyenv install 3.11"
    echo "   pyenv local 3.11  # or: pyenv global 3.11"
    exit 1
fi

echo "✅ Python 3.11 found: $(python3.11 --version)"

# Cleaning up old venv
if [ -d "venv" ]; then
    echo "Removing old venv..."
    rm -rf venv
fi

# Create venv and install dependencies
python3.11 -m venv venv
. venv/bin/activate
pip install -r requirements.txt

echo "✅ Setup complete. Activate with: source venv/bin/activate"

