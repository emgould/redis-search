"""
Fetch books from OpenLibrary Search API by author key.

This ETL utility:
1. Reads authors from mc_authors.jsonl or Redis index
2. For each author, calls /search.json?author_key=OLxxxA
3. Converts results to MCBookItem format
4. Supports configurable concurrency for experimentation
5. Writes results to JSONL file

Usage:
    python -m api.openlibrary.bulk.fetch_books_by_author --concurrency 5 --limit 100
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from typing import Any

import aiohttp
from aiolimiter import AsyncLimiter
from redis.asyncio import Redis

from api.openlibrary.models import MCBookItem
from utils.get_logger import get_logger

# Suppress ALL verbose logging for clean progress output
logging.disable(logging.INFO)

logger = get_logger(__name__)


async def check_books_exist_batch(redis: Redis, openlibrary_keys: list[str]) -> dict[str, bool]:
    """
    Check if multiple books exist in Redis using pipeline.

    Args:
        redis: Redis connection
        openlibrary_keys: list of keys like "/works/OL123W"

    Returns:
        Dict mapping openlibrary_key -> exists (bool)
    """
    if not openlibrary_keys:
        return {}

    # Build Redis keys
    key_mapping = {}
    for ol_key in openlibrary_keys:
        work_id = ol_key.replace("/works/", "")
        redis_key = f"book:book_{work_id}"
        key_mapping[ol_key] = redis_key

    # Batch check with pipeline
    pipe = redis.pipeline()
    for redis_key in key_mapping.values():
        pipe.exists(redis_key)

    results = await pipe.execute()

    # Map back to openlibrary_keys
    return {
        ol_key: bool(exists)
        for (ol_key, _), exists in zip(key_mapping.items(), results, strict=False)
    }


def load_checkpoint(checkpoint_file: str) -> dict[str, Any]:
    """Load checkpoint from file if it exists."""
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, encoding="utf8") as f:
                checkpoint = json.load(f)
                print(
                    f"Resuming from checkpoint: {checkpoint.get('authors_processed', 0):,} authors done"
                )
                return checkpoint
        except Exception as e:
            logger.warning(f"Could not load checkpoint: {e}")
    return {"authors_processed": 0, "processed_author_keys": []}


def save_checkpoint(
    checkpoint_file: str,
    authors_processed: int,
    processed_author_keys: list[str],
    stats: dict[str, int],
) -> None:
    """Save checkpoint to file."""
    checkpoint = {
        "authors_processed": authors_processed,
        "processed_author_keys": processed_author_keys,
        "stats": stats,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    with open(checkpoint_file, "w", encoding="utf8") as f:
        json.dump(checkpoint, f)


async def get_redis_connection(
    redis_host: str = "localhost",
    redis_port: int = 6380,
    redis_password: str | None = None,
) -> Redis | None:
    """Get Redis connection for duplicate checking."""
    try:
        redis = Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=True,
        )
        await redis.ping()
        print(f"Connected to Redis at {redis_host}:{redis_port}")
        return redis
    except Exception as e:
        print(f"⚠ Could not connect to Redis: {e}")
        print("  Proceeding without duplicate check")
        return None


# OpenLibrary API configuration
BASE_URL = "https://openlibrary.org"
SEARCH_URL = f"{BASE_URL}/search.json"
COVERS_URL = "https://covers.openlibrary.org/b"

# Output file
OUTPUT_FILE = "data/openlibrary/mc_books_api.jsonl"
CHECKPOINT_FILE = "data/openlibrary/fetch_books_checkpoint.json"


def process_search_doc(doc: dict[str, Any]) -> MCBookItem | None:
    """
    Process a single book document from OpenLibrary search results.

    Args:
        doc: Raw book document from search API

    Returns:
        MCBookItem instance or None if invalid
    """
    # Skip if no key
    if "key" not in doc:
        return None

    # Process cover image URLs
    cover_available = False
    cover_urls: dict[str, str] = {}
    book_image: str | None = None
    cover_i: int | None = None

    if "cover_i" in doc:
        cover_id = doc["cover_i"]
        cover_i = cover_id
        cover_urls = {
            "small": f"{COVERS_URL}/id/{cover_id}-S.jpg",
            "medium": f"{COVERS_URL}/id/{cover_id}-M.jpg",
            "large": f"{COVERS_URL}/id/{cover_id}-L.jpg",
        }
        cover_available = True
        book_image = cover_urls["medium"]

    # Process work URL
    openlibrary_key = doc.get("key")
    openlibrary_url = f"{BASE_URL}{openlibrary_key}" if openlibrary_key else None

    # Extract ISBNs
    primary_isbn13: str | None = None
    primary_isbn10: str | None = None
    isbns: list[str] = doc.get("isbn", [])

    if isbns:
        isbn13s = [isbn for isbn in isbns if len(str(isbn)) == 13]
        isbn10s = [isbn for isbn in isbns if len(str(isbn)) == 10]
        if isbn13s:
            primary_isbn13 = isbn13s[0]
        if isbn10s:
            primary_isbn10 = isbn10s[0]

    # Format author
    author: str | None = None
    author_names = doc.get("author_name", [])
    if author_names:
        author = ", ".join(author_names) if isinstance(author_names, list) else str(author_names)

    # Extract publisher
    publisher = doc.get("publisher")
    if publisher and isinstance(publisher, list) and publisher:
        publisher = publisher[0]

    # Extract description (might be string or dict with 'value')
    description = doc.get("description")
    if isinstance(description, dict):
        description = description.get("value", "")

    # Extract first sentence
    first_sentence = doc.get("first_sentence", [])
    if isinstance(first_sentence, str):
        first_sentence = [first_sentence]

    try:
        book = MCBookItem(
            key=openlibrary_key or "",
            title=doc.get("title", "Unknown"),
            openlibrary_key=openlibrary_key,
            openlibrary_url=openlibrary_url,
            author_name=author_names if isinstance(author_names, list) else [author_names],
            author=author,
            isbn=isbns[:10],  # Limit ISBNs
            primary_isbn13=primary_isbn13,
            primary_isbn10=primary_isbn10,
            first_publish_year=doc.get("first_publish_year"),
            publisher=publisher,
            publish_year=doc.get("publish_year", []),
            description=description,
            first_sentence=first_sentence,
            cover_i=cover_i,
            cover_available=cover_available,
            cover_urls=cover_urls,
            book_image=book_image,
            subject=doc.get("subject", [])[:20],  # Limit subjects
            subjects=doc.get("subject", [])[:20],
            language=doc.get("language", []),
            ratings_average=doc.get("ratings_average"),
            ratings_count=doc.get("ratings_count"),
            readinglog_count=doc.get("readinglog_count", 0),
            want_to_read_count=doc.get("want_to_read_count"),
            currently_reading_count=doc.get("currently_reading_count"),
            already_read_count=doc.get("already_read_count"),
            number_of_pages_median=doc.get("number_of_pages_median"),
        )
        return book
    except Exception as e:
        logger.warning(f"Error creating MCBookItem: {e}")
        return None


async def fetch_books_for_author(
    session: aiohttp.ClientSession,
    rate_limiter: AsyncLimiter,
    author_key: str,
    author_name: str,
    limit: int = 100,
    max_retries: int = 3,
) -> list[MCBookItem]:
    """
    Fetch books for a single author from OpenLibrary Search API.

    Args:
        session: aiohttp session
        rate_limiter: Rate limiter
        author_key: OpenLibrary author key (e.g., "OL39307A")
        author_name: Author name for logging
        limit: Max books to fetch per author
        max_retries: Number of retries on failure

    Returns:
        List of MCBookItem objects
    """
    # Extract just the ID from the key (remove /authors/ prefix if present)
    author_id = author_key.replace("/authors/", "")

    params = {
        "author_key": author_id,
        "limit": limit,
        "fields": "key,title,author_name,cover_i,first_publish_year,isbn,publisher,"
        "subject,language,ratings_average,ratings_count,readinglog_count,"
        "want_to_read_count,currently_reading_count,already_read_count,"
        "number_of_pages_median,description,first_sentence,publish_year",
    }

    for attempt in range(max_retries):
        try:
            async with rate_limiter, session.get(SEARCH_URL, params=params, timeout=30) as response:
                if response.status == 200:
                    data = await response.json()
                    docs = data.get("docs", [])

                    books = []
                    for doc in docs:
                        book = process_search_doc(doc)
                        if book:
                            books.append(book)

                    return books

                elif response.status == 429:
                    # Rate limited - wait and retry
                    wait_time = 2 ** (attempt + 1)
                    sys.stdout.write(f"\n⚠ Rate limited: {author_name}, waiting {wait_time}s\n")
                    await asyncio.sleep(wait_time)

                else:
                    sys.stdout.write(f"\n✗ HTTP {response.status}: {author_name}\n")
                    return []

        except TimeoutError:
            if attempt == max_retries - 1:
                sys.stdout.write(f"\n✗ Timeout: {author_name}\n")
            await asyncio.sleep(1)
        except Exception as e:
            sys.stdout.write(f"\n✗ Error: {author_name} - {e}\n")
            await asyncio.sleep(1)

    return []


async def process_author_batch(
    session: aiohttp.ClientSession,
    rate_limiter: AsyncLimiter,
    authors: list[dict[str, Any]],
    books_per_author: int,
) -> list[MCBookItem]:
    """
    Process a batch of authors concurrently.

    Args:
        session: aiohttp session
        rate_limiter: Rate limiter
        authors: List of author dicts with 'openlibrary_key' and 'name'
        books_per_author: Max books to fetch per author

    Returns:
        List of all MCBookItem objects
    """
    tasks = []
    for author in authors:
        author_key = author.get("openlibrary_key", "")
        author_name = author.get("name", "Unknown")
        if author_key:
            tasks.append(
                fetch_books_for_author(
                    session, rate_limiter, author_key, author_name, limit=books_per_author
                )
            )

    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_books = []
    for result in results:
        if isinstance(result, list):
            all_books.extend(result)
        elif isinstance(result, Exception):
            logger.warning(f"Batch task failed: {result}")

    return all_books


async def run_fetch(
    input_file: str = "data/openlibrary/mc_authors.jsonl",
    output_file: str = OUTPUT_FILE,
    concurrency: int = 5,
    rate_limit: int = 3,  # requests per second
    author_limit: int | None = None,
    books_per_author: int = 100,
    batch_size: int = 10,
    redis_host: str = "localhost",
    redis_port: int = 6380,
    redis_password: str | None = None,
    skip_existing: bool = True,
) -> dict[str, int]:
    """
    Main ETL function to fetch books for all authors.

    Args:
        input_file: Path to mc_authors.jsonl
        output_file: Path to output JSONL file
        concurrency: Number of concurrent connections
        rate_limit: Max requests per second
        author_limit: Max number of authors to process (None = all)
        books_per_author: Max books to fetch per author
        batch_size: Number of authors to process in each batch
        redis_host: Redis host for duplicate check
        redis_port: Redis port
        redis_password: Redis password
        skip_existing: Skip books already in Redis index

    Returns:
        Stats dict with counts
    """
    stats = {
        "authors_processed": 0,
        "books_found": 0,
        "books_new": 0,
        "books_skipped": 0,
        "books_with_cover": 0,
        "books_with_description": 0,
        "errors": 0,
    }

    # Check input file
    if not os.path.exists(input_file):
        logger.error(f"Input file not found: {input_file}")
        return stats

    # Load authors
    print(f"Loading authors from {input_file}...")
    authors = []
    with open(input_file, encoding="utf8") as f:
        for line in f:
            if line.strip():
                try:
                    author = json.loads(line)
                    if author.get("openlibrary_key"):
                        authors.append(author)
                except json.JSONDecodeError:
                    continue

    if author_limit:
        authors = authors[:author_limit]

    print(f"Loaded {len(authors):,} authors")

    # Load checkpoint if resuming
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    processed_author_keys: set[str] = set(checkpoint.get("processed_author_keys", []))

    if processed_author_keys:
        original_count = len(authors)
        authors = [a for a in authors if a.get("openlibrary_key") not in processed_author_keys]
        print(
            f"Skipping {original_count - len(authors):,} already-processed, "
            f"{len(authors):,} remaining"
        )
        # Restore stats from checkpoint
        if checkpoint.get("stats"):
            stats.update(checkpoint["stats"])

    # Get Redis connection for duplicate checking
    redis: Redis | None = None
    if skip_existing:
        redis = await get_redis_connection(redis_host, redis_port, redis_password)

    # Set up rate limiter
    rate_limiter = AsyncLimiter(rate_limit, 1)

    # Track seen book keys to avoid duplicates within this run
    seen_keys: set[str] = set()

    # Track processed authors for checkpoint
    newly_processed_keys: list[str] = []

    # Open output file
    start_time = time.time()
    last_update = start_time

    # Use append mode if resuming, otherwise write mode
    file_mode = "a" if processed_author_keys else "w"

    connector = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        with open(output_file, file_mode, encoding="utf8") as out_f:
            # Process in batches
            for i in range(0, len(authors), batch_size):
                batch = authors[i : i + batch_size]

                books = await process_author_batch(session, rate_limiter, batch, books_per_author)

                # Batch check which books exist in Redis
                books_to_check = []
                for book in books:
                    ol_key = book.openlibrary_key or book.key
                    if ol_key and ol_key not in seen_keys:
                        books_to_check.append((book, ol_key))

                # Batch EXISTS check via pipeline
                existing_map: dict[str, bool] = {}
                if redis and books_to_check:
                    ol_keys = [ol_key for _, ol_key in books_to_check]
                    existing_map = await check_books_exist_batch(redis, ol_keys)

                # Dedupe and write
                for book, ol_key in books_to_check:
                    stats["books_found"] += 1

                    # Skip if already in Redis index
                    if existing_map.get(ol_key, False):
                        stats["books_skipped"] += 1
                        seen_keys.add(ol_key)
                        continue

                    seen_keys.add(ol_key)
                    out_f.write(book.model_dump_json() + "\n")
                    stats["books_new"] += 1

                    if book.cover_available:
                        stats["books_with_cover"] += 1
                    if book.description:
                        stats["books_with_description"] += 1

                stats["authors_processed"] += len(batch)

                # Track processed authors for checkpoint
                for author in batch:
                    author_key = author.get("openlibrary_key")
                    if author_key:
                        newly_processed_keys.append(author_key)

                # Progress update
                current_time = time.time()
                if current_time - last_update >= 0.5:
                    elapsed = current_time - start_time
                    total_authors = len(authors) + len(processed_author_keys)
                    current_processed = stats["authors_processed"] + len(processed_author_keys)
                    pct = (current_processed / total_authors * 100) if total_authors > 0 else 0
                    rate = stats["authors_processed"] / elapsed if elapsed > 0 else 0

                    # Estimate time remaining
                    remaining_authors = total_authors - current_processed
                    eta_sec = remaining_authors / rate if rate > 0 else 0
                    eta_str = (
                        f"{int(eta_sec // 3600)}h {int((eta_sec % 3600) // 60)}m"
                        if eta_sec > 0
                        else "--"
                    )

                    # Progress bar (50 chars wide)
                    bar_width = 40
                    filled = int(bar_width * pct / 100)
                    bar = "█" * filled + "░" * (bar_width - filled)

                    sys.stdout.write(
                        f"\r  [{bar}] {pct:5.1f}% | "
                        f"{current_processed:,}/{total_authors:,} | "
                        f"{rate:.1f}/s | "
                        f"New: {stats['books_new']:,} | "
                        f"ETA: {eta_str}  "
                    )
                    sys.stdout.flush()
                    last_update = current_time

                # Save checkpoint every 100 authors
                if len(newly_processed_keys) % 100 == 0 and newly_processed_keys:
                    all_processed = list(processed_author_keys) + newly_processed_keys
                    save_checkpoint(CHECKPOINT_FILE, len(all_processed), all_processed, stats)

    sys.stdout.write("\n")

    # Close Redis connection
    if redis:
        await redis.aclose()

    # Save final checkpoint
    all_processed = list(processed_author_keys) + newly_processed_keys
    save_checkpoint(CHECKPOINT_FILE, len(all_processed), all_processed, stats)

    elapsed = time.time() - start_time
    hours = int(elapsed // 3600)
    mins = int((elapsed % 3600) // 60)
    secs = int(elapsed % 60)

    print(f"\n\n✓ Completed in {hours}h {mins}m {secs}s")
    print(
        f"  Authors: {stats['authors_processed']:,} | Books: {stats['books_new']:,} new, {stats['books_skipped']:,} skipped"
    )
    print(
        f"  With cover: {stats['books_with_cover']:,} | With description: {stats['books_with_description']:,}"
    )

    return stats


async def main():
    parser = argparse.ArgumentParser(description="Fetch books from OpenLibrary Search API")
    parser.add_argument(
        "--input",
        default="data/openlibrary/mc_authors.jsonl",
        help="Input authors JSONL file",
    )
    parser.add_argument(
        "--output",
        default=OUTPUT_FILE,
        help="Output books JSONL file",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Number of concurrent connections (default: 5)",
    )
    parser.add_argument(
        "--rate-limit",
        type=int,
        default=3,
        help="Max requests per second (default: 3)",
    )
    parser.add_argument(
        "--author-limit",
        type=int,
        default=None,
        help="Max authors to process (default: all)",
    )
    parser.add_argument(
        "--books-per-author",
        type=int,
        default=100,
        help="Max books to fetch per author (default: 100)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Authors per batch (default: 10)",
    )
    parser.add_argument(
        "--redis-host",
        default=os.getenv("REDIS_HOST", "localhost"),
        help="Redis host (default: localhost)",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=int(os.getenv("REDIS_PORT", "6380")),
        help="Redis port (default: 6380)",
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Don't skip books already in Redis (fetch all)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset checkpoint and start fresh",
    )

    args = parser.parse_args()

    # Reset checkpoint if requested
    if args.reset and os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        logger.info("Checkpoint reset - starting fresh")

    print("Starting OpenLibrary Book Fetch ETL")
    print(f"  Concurrency: {args.concurrency}, Rate: {args.rate_limit}/s, Batch: {args.batch_size}")
    print(f"  Skip existing: {not args.no_skip_existing}")

    stats = await run_fetch(
        input_file=args.input,
        output_file=args.output,
        concurrency=args.concurrency,
        rate_limit=args.rate_limit,
        author_limit=args.author_limit,
        books_per_author=args.books_per_author,
        batch_size=args.batch_size,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        skip_existing=not args.no_skip_existing,
    )

    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
