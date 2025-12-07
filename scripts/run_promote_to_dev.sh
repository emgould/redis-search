#!/bin/bash
# scripts/run_promote_to_dev.sh
# Promote local Redis documents to the public/dev Redis instance
#
# Prerequisites:
#   - Local Redis running (docker)
#   - IAP tunnel to public Redis running (`make tunnel`)
#
# Usage:
#   ./scripts/run_promote_to_dev.sh                  # Copy all docs to dev
#   ./scripts/run_promote_to_dev.sh --dry-run        # Show what would be copied
#   ./scripts/run_promote_to_dev.sh --create-index   # Create index on target
#   ./scripts/run_promote_to_dev.sh --clear-target   # Clear target before copy

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "============================================================"
echo "Promote to Dev - Redis Sync Utility"
echo "============================================================"
echo "Arguments: $@"
echo "============================================================"
echo ""

# Change to project root
cd "$PROJECT_ROOT"

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Load secrets from local.env
echo "🔐 Loading environment from config/local.env..."
if [ -f "config/local.env" ]; then
    set -a
    source config/local.env
    set +a
    echo "✅ Environment loaded"
else
    echo "❌ Error: config/local.env not found"
    exit 1
fi

echo ""
echo "🚀 Running promote..."
echo ""

# Run the promote script with all passed arguments
python scripts/promote_to_dev.py "$@"

echo ""
echo "✅ Done!"

