#!/bin/bash
# scripts/run_redis_etl.sh
# Load TMDB JSON files into Redis Search index
#
# Usage:
#   ./scripts/run_redis_etl.sh --list                           # List available files
#   ./scripts/run_redis_etl.sh --list --type movie              # List movie files only
#   ./scripts/run_redis_etl.sh --files tmdb_tv_2025_11.json     # Load specific file
#   ./scripts/run_redis_etl.sh --type tv --year 2025            # Load TV shows from 2025
#   ./scripts/run_redis_etl.sh --type tv --year-lte 2020        # Load TV shows 2020 and earlier
#   ./scripts/run_redis_etl.sh --type movie --all               # Load all movies
#   ./scripts/run_redis_etl.sh --type tv --year 2025 --no-gcs   # Skip GCS upload

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "============================================================"
echo "Redis Search ETL Loader"
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
echo "🚀 Running ETL..."
echo ""

# Run the ETL with all passed arguments
python -m src.etl.bulk_loader "$@"

echo ""
echo "✅ Done!"

