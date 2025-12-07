"""
Entry point for running the TMDB ETL process.

Usage:
    # List available files
    python -m src.etl.run_etl --list
    python -m src.etl.run_etl --list --type movie

    # Load specific files
    python -m src.etl.run_etl --files tmdb_movie_2025_10.json tmdb_movie_2025_11.json
    python -m src.etl.run_etl --files tmdb_tv_2020_10.json --no-gcs

    # Load by exact date
    python -m src.etl.run_etl --type movie --year 2025 --month 10
    python -m src.etl.run_etl --type tv --year 2025

    # Load by date range
    python -m src.etl.run_etl --type tv --year-lte 2020      # 2020 and earlier
    python -m src.etl.run_etl --type tv --year-gte 2023      # 2023 and later
    python -m src.etl.run_etl --type movie --year-gte 2020 --year-lte 2022  # 2020-2022

    # Load all files of a type (use with caution)
    python -m src.etl.run_etl --type movie --all
"""

import os
import sys

# Add src to path for imports (handles 'from utils.' style imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import argparse
import asyncio

from src.services.etl_service import ETLConfig, TMDBETLService


def main():
    parser = argparse.ArgumentParser(
        description="Load TMDB data from local JSON files into Redis Search index"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available files without loading",
    )
    parser.add_argument(
        "--type",
        choices=["movie", "tv"],
        help="Type of media (required for --list, --year, --month, --all)",
    )
    parser.add_argument(
        "--files",
        nargs="+",
        help="Specific file names to load (e.g., tmdb_movie_2025_10.json)",
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Filter files by exact year (e.g., 2025)",
    )
    parser.add_argument(
        "--month",
        type=int,
        help="Filter files by month (e.g., 10 for October)",
    )
    parser.add_argument(
        "--year-lte",
        type=int,
        dest="year_lte",
        help="Filter files by year <= value (e.g., --year-lte 2020 loads 2020 and earlier)",
    )
    parser.add_argument(
        "--year-gte",
        type=int,
        dest="year_gte",
        help="Filter files by year >= value (e.g., --year-gte 2023 loads 2023 and later)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        dest="load_all",
        help="Load ALL files of the specified type (use with caution)",
    )
    parser.add_argument(
        "--no-gcs",
        action="store_true",
        help="Skip uploading to GCS after loading",
    )

    args = parser.parse_args()

    # Validation
    if args.list:
        asyncio.run(list_files(args.type))
        return

    has_date_filter = args.year or args.month or args.year_lte or args.year_gte

    if not args.files and not args.load_all and not has_date_filter:
        parser.error(
            "You must specify one of:\n"
            "  --files <file1> <file2> ...   (specific files)\n"
            "  --year <YYYY> [--month <MM>]  (filter by exact date)\n"
            "  --year-lte <YYYY>             (year and earlier)\n"
            "  --year-gte <YYYY>             (year and later)\n"
            "  --all --type <movie|tv>       (all files of type)\n"
            "  --list                        (show available files)"
        )

    if args.load_all and not args.type:
        parser.error("--all requires --type to be specified")

    if has_date_filter and not args.type:
        parser.error("Date filters require --type to be specified")

    # Don't allow --year with --year-lte or --year-gte
    if args.year and (args.year_lte or args.year_gte):
        parser.error("Cannot use --year with --year-lte or --year-gte. Use --year for exact year, or --year-lte/--year-gte for ranges.")

    upload_to_gcs = not args.no_gcs

    asyncio.run(
        run_etl_with_selection(
            media_type=args.type,
            files=args.files,
            year=args.year,
            month=args.month,
            year_lte=args.year_lte,
            year_gte=args.year_gte,
            load_all=args.load_all,
            upload_to_gcs=upload_to_gcs,
        )
    )


async def list_files(media_type: str | None):
    """List available files without loading."""
    config = ETLConfig.from_env()
    service = TMDBETLService(config)

    print("=" * 60)
    print("📁 Available JSON Files")
    print("=" * 60)
    print(f"   Data directory: {config.data_dir}")
    print()

    types_to_list = [media_type] if media_type else ["movie", "tv"]

    for mtype in types_to_list:
        files = service.discover_json_files(mtype)
        print(f"📽️  {mtype.upper()} ({len(files)} files):")
        for f in files:
            print(f"   - {f.name}")
        print()


async def run_etl_with_selection(
    media_type: str | None,
    files: list[str] | None,
    year: int | None,
    month: int | None,
    year_lte: int | None,
    year_gte: int | None,
    load_all: bool,
    upload_to_gcs: bool,
):
    """Run ETL with file selection."""
    config = ETLConfig.from_env()
    service = TMDBETLService(config)

    # Determine which files to load
    if files:
        # Specific files provided
        selected_files = service.resolve_file_names(files, media_type)
    elif load_all:
        # All files of the specified type
        selected_files = service.discover_json_files(media_type)
    else:
        # Filter by date criteria
        selected_files = service.discover_json_files(media_type)
        selected_files = service.filter_files_by_date(
            selected_files,
            year=year,
            month=month,
            year_lte=year_lte,
            year_gte=year_gte,
        )

    if not selected_files:
        print("❌ No files match the specified criteria")
        return

    # Confirm before loading
    print("=" * 60)
    print("📋 Files to load:")
    print("=" * 60)
    for f in selected_files:
        print(f"   - {f.name}")
    print()
    print(f"   Total: {len(selected_files)} files")
    print(f"   GCS upload: {'Yes' if upload_to_gcs else 'No'}")
    print()

    # Run the ETL
    await service.run_etl_for_files(selected_files, upload_to_gcs=upload_to_gcs)


if __name__ == "__main__":
    main()
