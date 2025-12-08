#!/bin/bash
# scripts/run_tmdb_movie_etl.sh
# Run the TMDB Movie ETL to extract and enrich movies by monthly release date
#
# Usage:
#   ./scripts/run_tmdb_movie_etl.sh 2025-10 1      # October 2025, 1 month
#   ./scripts/run_tmdb_movie_etl.sh 2025-11 12     # November 2025, 12 months back
#   ./scripts/run_tmdb_movie_etl.sh                # Defaults: current month, 1 month

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default values
DEFAULT_MONTHS_BACK=1
CURRENT_YEAR=$(date +%Y)
CURRENT_MONTH=$(date +%m)
DEFAULT_START_DATE="${CURRENT_YEAR}-${CURRENT_MONTH}"

# Parse arguments
START_DATE="${1:-$DEFAULT_START_DATE}"
MONTHS_BACK="${2:-$DEFAULT_MONTHS_BACK}"
OUTPUT_DIR="${3:-data/us/movie}"

echo "============================================================"
echo "TMDB Movie ETL"
echo "============================================================"
echo "Start Date:   ${START_DATE}"
echo "Months Back:  ${MONTHS_BACK}"
echo "Output Dir:   ${OUTPUT_DIR}"
echo "============================================================"
echo ""

# Change to project root
cd "$PROJECT_ROOT"

# Activate virtual environment (skip in Docker where Python is global)
if [ -d "venv" ]; then
echo "🔧 Activating virtual environment..."
source venv/bin/activate
else
    echo "🔧 Using system Python (Docker environment)"
fi

# Load secrets from local.env (preserve Docker env vars if set)
echo "🔐 Loading secrets from config/local.env..."
SAVE_REDIS_HOST="${REDIS_HOST:-}"
SAVE_REDIS_PORT="${REDIS_PORT:-}"
if [ -f "config/local.env" ]; then
    set -a
    source config/local.env
    set +a
    echo "✅ Secrets loaded"
else
    echo "❌ Error: config/local.env not found"
    exit 1
fi
# Restore Docker's Redis settings if they were set
if [ -n "$SAVE_REDIS_HOST" ]; then
    export REDIS_HOST="$SAVE_REDIS_HOST"
    export REDIS_PORT="$SAVE_REDIS_PORT"
    echo "🐳 Using Docker Redis: $REDIS_HOST:$REDIS_PORT"
fi

# Verify TMDB token is set
if [ -z "$TMDB_READ_TOKEN" ]; then
    echo "❌ Error: TMDB_READ_TOKEN is not set"
    exit 1
fi

echo ""
echo "🚀 Starting Movie ETL..."
echo ""

# Run the ETL
python scripts/tmdb_movie_etl.py \
    --start-date "$START_DATE" \
    --months-back "$MONTHS_BACK" \
    --output-dir "$OUTPUT_DIR"

echo ""
echo "✅ Movie ETL complete!"
echo ""

# Show output files (sorted by date, newest first)
echo "📁 Output files:"
ls -lht "$OUTPUT_DIR"/*.json 2>/dev/null || echo "   No JSON files found in $OUTPUT_DIR"


