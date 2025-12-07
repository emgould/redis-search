#!/bin/bash
# scripts/run_download_person_ids.sh
# Download TMDB person ID exports from the daily file exports
#
# Usage:
#   ./scripts/run_download_person_ids.sh                    # Yesterday's date (default)
#   ./scripts/run_download_person_ids.sh 2025-12-07         # Specific date
#   ./scripts/run_download_person_ids.sh 2025-12-07 data/custom/  # Custom output dir

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default values - yesterday since today's may not be ready
YESTERDAY=$(date -v-1d +%Y-%m-%d 2>/dev/null || date -d "yesterday" +%Y-%m-%d)
DATE="${1:-$YESTERDAY}"
OUTPUT_DIR="${2:-data/person}"

echo "============================================================"
echo "TMDB Person IDs Download"
echo "============================================================"
echo "Date:         ${DATE}"
echo "Output Dir:   ${OUTPUT_DIR}"
echo "============================================================"
echo ""

# Change to project root
cd "$PROJECT_ROOT"

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

echo ""
echo "🚀 Starting download..."
echo ""

# Run the download script
python scripts/download_tmdb_person_ids.py \
    --date "$DATE" \
    --output-dir "$OUTPUT_DIR"

echo ""
echo "📁 Output files:"
ls -lh "$OUTPUT_DIR"/*.json 2>/dev/null || echo "   No JSON files found in $OUTPUT_DIR"


