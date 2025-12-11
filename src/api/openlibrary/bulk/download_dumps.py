"""
Download OpenLibrary dump files on demand.

Downloads:
- ol_dump_authors_latest.txt.gz
- ol_dump_wikidata_latest.txt.gz

Extracts to data/openlibrary/
"""

import asyncio
import gzip
import shutil
import sys
import time
from pathlib import Path

import aiohttp

from utils.get_logger import get_logger

logger = get_logger(__name__)

# OpenLibrary dump URLs
DUMPS = {
    "authors": {
        "url": "https://openlibrary.org/data/ol_dump_authors_latest.txt.gz",
        "output": "ol_dump_authors_latest.txt",
    },
    "wikidata": {
        "url": "https://openlibrary.org/data/ol_dump_wikidata_latest.txt.gz",
        "output": "ol_dump_wikidata_latest.txt",
    },
}


async def download_file(
    session: aiohttp.ClientSession,
    url: str,
    output_path: Path,
    chunk_size: int = 1024 * 1024,  # 1MB chunks
) -> bool:
    """
    Download a file with progress tracking.

    Args:
        session: aiohttp session
        url: URL to download
        output_path: Where to save the file
        chunk_size: Download chunk size

    Returns:
        True if successful
    """
    logger.info(f"Downloading: {url}")
    logger.info(f"Output: {output_path}")

    try:
        async with session.get(url) as response:
            if response.status != 200:
                logger.error(f"Download failed: HTTP {response.status}")
                return False

            total_size = int(response.headers.get("content-length", 0))
            downloaded = 0
            start_time = time.time()
            last_update = start_time

            with open(output_path, "wb") as f:
                async for chunk in response.content.iter_chunked(chunk_size):
                    f.write(chunk)
                    downloaded += len(chunk)

                    # Progress update every 0.5s
                    current_time = time.time()
                    if current_time - last_update >= 0.5:
                        elapsed = current_time - start_time
                        rate = downloaded / elapsed / 1024 / 1024  # MB/s
                        if total_size:
                            pct = downloaded * 100 / total_size
                            progress = (
                                f"\r  {downloaded / 1024 / 1024:.1f} / "
                                f"{total_size / 1024 / 1024:.1f} MB "
                                f"({pct:.1f}%) - {rate:.1f} MB/s"
                            )
                        else:
                            progress = f"\r  {downloaded / 1024 / 1024:.1f} MB - {rate:.1f} MB/s"
                        sys.stdout.write(progress)
                        sys.stdout.flush()
                        last_update = current_time

            sys.stdout.write("\n")
            logger.info(f"Download complete: {downloaded / 1024 / 1024:.1f} MB")
            return True

    except Exception as e:
        logger.error(f"Download error: {e}")
        return False


def extract_gzip(gz_path: Path, output_path: Path) -> bool:
    """
    Extract a gzip file with progress.

    Args:
        gz_path: Path to .gz file
        output_path: Where to extract

    Returns:
        True if successful
    """
    logger.info(f"Extracting: {gz_path}")
    logger.info(f"Output: {output_path}")

    try:
        start_time = time.time()
        with gzip.open(gz_path, "rb") as f_in, open(output_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

        elapsed = time.time() - start_time
        size = output_path.stat().st_size / 1024 / 1024 / 1024  # GB
        logger.info(f"Extracted: {size:.2f} GB in {elapsed:.1f}s")
        return True

    except Exception as e:
        logger.error(f"Extraction error: {e}")
        return False


async def download_dumps(
    data_dir: Path,
    dumps: list[str] | None = None,
    keep_gz: bool = False,
) -> dict[str, bool]:
    """
    Download and extract OpenLibrary dumps.

    Args:
        data_dir: Directory to save files
        dumps: List of dumps to download ("authors", "wikidata") or None for all
        keep_gz: Keep the .gz files after extraction

    Returns:
        Dict mapping dump name to success status
    """
    data_dir.mkdir(parents=True, exist_ok=True)

    if dumps is None:
        dumps = list(DUMPS.keys())

    results = {}

    async with aiohttp.ClientSession() as session:
        for dump_name in dumps:
            if dump_name not in DUMPS:
                logger.warning(f"Unknown dump: {dump_name}")
                results[dump_name] = False
                continue

            dump_info = DUMPS[dump_name]
            gz_path = data_dir / (dump_info["output"] + ".gz")
            output_path = data_dir / dump_info["output"]

            logger.info(f"\n{'=' * 60}")
            logger.info(f"Processing: {dump_name}")
            logger.info(f"{'=' * 60}")

            # Download
            if not await download_file(session, dump_info["url"], gz_path):
                results[dump_name] = False
                continue

            # Extract
            if not extract_gzip(gz_path, output_path):
                results[dump_name] = False
                continue

            # Clean up .gz
            if not keep_gz:
                gz_path.unlink()
                logger.info(f"Removed: {gz_path}")

            results[dump_name] = True
            logger.info(f"✓ {dump_name} complete")

    return results


async def main(
    data_dir: str = "data/openlibrary",
    dumps: list[str] | None = None,
    keep_gz: bool = False,
):
    """Main entry point."""
    results = await download_dumps(
        data_dir=Path(data_dir),
        dumps=dumps,
        keep_gz=keep_gz,
    )

    logger.info("\n" + "=" * 60)
    logger.info("Summary:")
    for dump_name, success in results.items():
        status = "✓" if success else "✗"
        logger.info(f"  {status} {dump_name}")

    return all(results.values())


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Download OpenLibrary dump files")
    parser.add_argument(
        "--data-dir",
        default="data/openlibrary",
        help="Directory to save files",
    )
    parser.add_argument(
        "--dumps",
        nargs="+",
        choices=["authors", "wikidata"],
        help="Which dumps to download (default: all)",
    )
    parser.add_argument(
        "--keep-gz",
        action="store_true",
        help="Keep .gz files after extraction",
    )
    args = parser.parse_args()

    success = asyncio.run(
        main(
            data_dir=args.data_dir,
            dumps=args.dumps,
            keep_gz=args.keep_gz,
        )
    )
    sys.exit(0 if success else 1)
