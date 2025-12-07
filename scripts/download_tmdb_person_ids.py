#!/usr/bin/env python3
"""
TMDB Person IDs Download Script - Download daily person ID exports from TMDB

This script downloads the daily person ID exports from TMDB's file exports,
uncompresses the gzip file, and saves it to the data/person directory.

The files are available at: https://files.tmdb.org/p/exports/person_ids_MM_DD_YYYY.json.gz

Note: Each line in the output file is a valid JSON object (not a full JSON array).
Example line: {"adult":false,"id":12345,"name":"John Doe","popularity":5.123}

Usage:
    python scripts/download_tmdb_person_ids.py --date 2025-12-07
    python scripts/download_tmdb_person_ids.py  # Uses yesterday's date (today's may not be ready)
"""

import argparse
import gzip
import json
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from utils.get_logger import get_logger

logger = get_logger(__name__)

TMDB_EXPORTS_BASE_URL = "https://files.tmdb.org/p/exports"

# Pattern for names typeable on standard Western keyboards
# Allows: Latin letters (including accented), spaces, hyphens, apostrophes, periods, commas
# This covers: English, Spanish, French, German, Italian, Portuguese, etc.
LATIN_NAME_PATTERN = re.compile(
    r"^[A-Za-z\u00C0-\u00FF\u0100-\u017F\s\-\'\.\,]+$"
)


def is_latin_name(name: str) -> bool:
    """
    Check if a name uses only Latin script characters that can be typed
    on a standard Western keyboard (possibly with accent support).

    This filters out:
    - Chinese characters
    - Japanese (Hiragana, Katakana, Kanji)
    - Korean (Hangul)
    - Cyrillic (Russian, etc.)
    - Arabic
    - Hebrew
    - Thai
    - And other non-Latin scripts

    Args:
        name: The name to check

    Returns:
        True if name uses only Latin characters, False otherwise
    """
    if not name or not name.strip():
        return False
    return bool(LATIN_NAME_PATTERN.match(name))


def get_person_ids_url(date: datetime) -> str:
    """
    Build the URL for the person IDs export file for a given date.

    Args:
        date: The date for the export file

    Returns:
        Full URL to the gzipped person IDs file
    """
    # TMDB uses MM_DD_YYYY format
    date_str = date.strftime("%m_%d_%Y")
    return f"{TMDB_EXPORTS_BASE_URL}/person_ids_{date_str}.json.gz"


def download_and_extract(url: str, output_path: Path) -> int:
    """
    Download a gzipped file, sort by popularity descending, and save to output path.

    Args:
        url: URL to download from
        output_path: Path to save the uncompressed file

    Returns:
        Number of lines (records) in the file

    Raises:
        HTTPError: If the download fails
        URLError: If there's a network error
    """
    logger.info(f"Downloading from: {url}")

    # Download the gzipped file
    with urlopen(url, timeout=60) as response:
        compressed_data = response.read()
        logger.info(f"Downloaded {len(compressed_data):,} bytes (compressed)")

    # Decompress the data
    logger.info("Decompressing...")
    decompressed_data = gzip.decompress(compressed_data)
    logger.info(f"Decompressed to {len(decompressed_data):,} bytes")

    # Parse each line as JSON, filter to Latin names, and sort by popularity descending
    logger.info("Parsing, filtering to Latin names, and sorting by popularity (descending)...")
    lines = decompressed_data.decode("utf-8").strip().split("\n")
    records = []
    filtered_count = 0
    for line in lines:
        if line.strip():
            record = json.loads(line)
            name = record.get("name", "")
            if is_latin_name(name):
                records.append(record)
            else:
                filtered_count += 1

    logger.info(f"Filtered out {filtered_count:,} non-Latin names")

    # Sort by popularity descending (highest first)
    records.sort(key=lambda x: x.get("popularity", 0), reverse=True)
    logger.info(f"Sorted {len(records):,} records by popularity")

    # Write sorted records to output file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    logger.info(f"Saved {len(records):,} records to {output_path}")

    return len(records)


def parse_date(date_str: str) -> datetime:
    """
    Parse a date string in YYYY-MM-DD format.

    Args:
        date_str: Date string in YYYY-MM-DD format

    Returns:
        datetime object

    Raises:
        ValueError: If date string is invalid
    """
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format: {date_str}. Expected YYYY-MM-DD") from e


def main() -> None:
    """Main entry point for the TMDB person IDs download script."""
    parser = argparse.ArgumentParser(
        description="Download TMDB person ID exports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Download person IDs for a specific date
    python scripts/download_tmdb_person_ids.py --date 2025-12-07

    # Download using yesterday's date (default - today's may not be ready until ~8AM UTC)
    python scripts/download_tmdb_person_ids.py

    # Custom output directory
    python scripts/download_tmdb_person_ids.py --date 2025-12-07 --output-dir data/custom/

Note: TMDB exports are generated daily around 7-8 AM UTC. Files are kept for 3 months.
Each line in the output file is a valid JSON object like:
    {"adult":false,"id":12345,"name":"John Doe","popularity":5.123}
        """,
    )

    # Default to yesterday since today's file may not be ready yet
    yesterday = datetime.now() - timedelta(days=1)
    default_date = yesterday.strftime("%Y-%m-%d")

    parser.add_argument(
        "--date",
        type=str,
        default=default_date,
        help=f"Date in YYYY-MM-DD format (default: {default_date} - yesterday)",
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/person",
        help="Output directory for the JSON file (default: data/person/)",
    )

    args = parser.parse_args()

    # Parse and validate date
    try:
        target_date = parse_date(args.date)
    except ValueError as e:
        parser.error(str(e))

    # Build paths
    output_dir = Path(args.output_dir)
    output_filename = f"person_ids_{target_date.strftime('%Y_%m_%d')}.json"
    output_path = output_dir / output_filename

    # Check if file already exists
    if output_path.exists():
        logger.warning(f"Output file already exists: {output_path}")
        logger.warning("Delete it first if you want to re-download")
        return

    # Build URL and download
    url = get_person_ids_url(target_date)

    logger.info("=" * 60)
    logger.info("TMDB Person IDs Download")
    logger.info("=" * 60)
    logger.info(f"Date:        {target_date.strftime('%Y-%m-%d')}")
    logger.info(f"Output:      {output_path}")
    logger.info("=" * 60)

    try:
        line_count = download_and_extract(url, output_path)
        logger.info("")
        logger.info("=" * 60)
        logger.info(f"✅ Success! Downloaded {line_count:,} person records")
        logger.info(f"   File saved to: {output_path}")
        logger.info("=" * 60)
    except HTTPError as e:
        if e.code == 404:
            logger.error(f"❌ File not found (404): {url}")
            logger.error("   The file for this date may not exist yet or may have been deleted.")
            logger.error("   TMDB exports are available ~8AM UTC and kept for 3 months.")
        else:
            logger.error(f"❌ HTTP error {e.code}: {e.reason}")
        sys.exit(1)
    except URLError as e:
        logger.error(f"❌ Network error: {e.reason}")
        sys.exit(1)


if __name__ == "__main__":
    main()

