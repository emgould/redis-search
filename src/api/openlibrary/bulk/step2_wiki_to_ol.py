"""
Step 2: Match Wikidata Authors to OpenLibrary and Convert to MCAuthorItem

This script:
1. Loads cleansed Wikidata authors from cleansed_wiki.json
2. For authors WITH ol_id (P648): Direct lookup via /authors/{olid}.json (fast!)
3. For authors WITHOUT ol_id: Optionally search via --search flag
4. Converts to MCAuthorItem format enriched with Wikidata metadata
5. Outputs matched authors as JSON
"""

import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import aiohttp
from aiolimiter import AsyncLimiter

from api.openlibrary.models import MCAuthorItem
from utils.get_logger import get_logger

logger = get_logger(__name__)

# Try to import rapidfuzz for better fuzzy matching, fallback to simple ratio if not available
try:
    from rapidfuzz import fuzz

    HAS_RAPIDFUZZ = True
except ImportError:
    HAS_RAPIDFUZZ = False
    logger.warning(
        "rapidfuzz not installed, using simple string matching. Install with: pip install rapidfuzz"
    )


def simple_fuzz_ratio(s1: str, s2: str) -> float:
    """Simple fuzzy matching fallback when rapidfuzz is not available."""
    if not s1 or not s2:
        return 0.0
    s1_lower = s1.lower()
    s2_lower = s2.lower()
    if s1_lower == s2_lower:
        return 100.0

    # Simple character overlap ratio
    set1 = set(s1_lower)
    set2 = set(s2_lower)
    if not set1 or not set2:
        return 0.0
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    return (intersection / union * 100) if union > 0 else 0.0


def fuzz_ratio(s1: str, s2: str) -> float:
    """Get fuzzy match ratio, using rapidfuzz if available."""
    if HAS_RAPIDFUZZ:
        return fuzz.WRatio(s1, s2)
    return simple_fuzz_ratio(s1, s2)


def load_wikidata_authors(path: str) -> list[dict[str, Any]]:
    """
    Load cleansed Wikidata authors from JSON array file.

    Args:
        path: Path to cleansed_wiki.json file

    Returns:
        List of author dictionaries
    """
    logger.info(f"Loading Wikidata authors from {path}")
    with open(path, encoding="utf8") as f:
        data = json.load(f)
    logger.info(f"Loaded {len(data)} Wikidata authors")
    return data


def score_match(wd_author: dict[str, Any], ol_author: MCAuthorItem) -> float:
    """
    Compute similarity score between Wikidata author and OpenLibrary author.
    Higher score = better match.

    Args:
        wd_author: Wikidata author dict with name, aliases, birth_year
        ol_author: MCAuthorItem from OpenLibrary

    Returns:
        Similarity score (0-100+)
    """
    wd_name = wd_author.get("name", "")
    wd_aliases = wd_author.get("aliases", [])
    wd_birth = wd_author.get("birth_year")

    ol_name = ol_author.name or ""
    ol_birth_str = ol_author.birth_date or ""

    # 1. Fuzzy name similarity (primary score)
    score = fuzz_ratio(wd_name, ol_name)

    # 2. Alias match boost - check if any Wikidata alias matches OL name
    for alias in wd_aliases:
        alias_score = fuzz_ratio(alias, ol_name)
        score = max(score, alias_score)

    # 3. Birth year proximity boost
    if wd_birth and ol_birth_str:
        # Try to extract year from birth_date string
        # Formats: "1954", "1954-11-08", "27 February 1902", "February 27, 1902", etc.
        try:
            import re

            # Look for 4-digit year in the string
            year_match = re.search(r"\b(19|20)\d{2}\b", ol_birth_str)
            if year_match:
                ol_birth = int(year_match.group())
                year_diff = abs(wd_birth - ol_birth)
                if year_diff == 0:
                    score += 20
                elif year_diff <= 1:
                    score += 15
                elif year_diff <= 3:
                    score += 10
                elif year_diff <= 5:
                    score += 5
        except (ValueError, AttributeError, TypeError):
            pass

    # 4. Work count boost (authors with more works are more likely to be correct)
    if ol_author.work_count > 0:
        score += min(ol_author.work_count / 10, 5)  # Max 5 point boost

    return round(score, 2)


async def search_openlibrary_direct(
    session: aiohttp.ClientSession,
    rate_limiter: AsyncLimiter,
    query: str,
    limit: int = 10,
    max_retries: int = 3,
) -> list[dict[str, Any]]:
    """
    Direct OpenLibrary author search - bypasses slow base client.

    Args:
        session: Shared aiohttp session
        rate_limiter: Rate limiter for API calls
        query: Search query
        limit: Max results
        max_retries: Number of retries on rate limit

    Returns:
        List of author documents from OpenLibrary
    """
    url = f"https://openlibrary.org/search/authors.json?q={quote(query)}&limit={limit}"
    headers = {"User-Agent": "mediacircle/1.0 (gould@emgtrading.net)"}

    for attempt in range(max_retries):
        async with rate_limiter:
            try:
                async with session.get(
                    url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("docs", [])
                    elif response.status == 429:
                        # Rate limited - exponential backoff
                        wait_time = 2 ** (attempt + 1)
                        logger.debug(
                            f"Rate limited on '{query}', waiting {wait_time}s (attempt {attempt + 1})"
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        return []
            except Exception as e:
                logger.debug(f"Error searching '{query}': {e}")
                return []

    logger.warning(f"Max retries exceeded for query '{query}'")
    return []


async def fetch_author_by_olid(
    session: aiohttp.ClientSession,
    rate_limiter: AsyncLimiter,
    ol_id: str,
    max_retries: int = 3,
) -> dict[str, Any] | None:
    """
    Fetch author directly by OpenLibrary ID - much faster than search!

    Args:
        session: Shared aiohttp session
        rate_limiter: Rate limiter for API calls
        ol_id: OpenLibrary author ID (e.g., "OL1234A")
        max_retries: Number of retries on rate limit

    Returns:
        Author data dict or None if not found
    """
    url = f"https://openlibrary.org/authors/{ol_id}.json"
    headers = {"User-Agent": "mediacircle/1.0 (gould@emgtrading.net)"}

    for attempt in range(max_retries):
        async with rate_limiter:
            try:
                async with session.get(
                    url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 404:
                        print(f"Author not found: {url}")
                        return None  # Author not found
                    elif response.status == 429:
                        wait_time = 2 ** (attempt + 1)
                        logger.debug(f"Rate limited on '{ol_id}', waiting {wait_time}s")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        return None
            except Exception as e:
                logger.debug(f"Error fetching '{ol_id}': {e}")
                return None

    logger.warning(f"Max retries exceeded for olid '{ol_id}'")
    return None


def ol_author_json_to_mc_author(data: dict[str, Any], ol_id: str) -> MCAuthorItem:
    """Convert OpenLibrary author JSON (from /authors/{id}.json) to MCAuthorItem."""
    key = data.get("key", f"/authors/{ol_id}")
    return MCAuthorItem(
        key=key,
        name=data.get("name", "Unknown"),
        birth_date=data.get("birth_date"),
        death_date=data.get("death_date"),
        bio=data.get("bio", {}).get("value")
        if isinstance(data.get("bio"), dict)
        else data.get("bio"),
        source_id=key,
        openlibrary_key=key,
        openlibrary_url=f"https://openlibrary.org{key}",
        remote_ids=data.get("remote_ids", {}),
        # Note: work_count not available from author endpoint, would need separate call
        work_count=0,
    )


def ol_doc_to_mc_author(doc: dict[str, Any]) -> MCAuthorItem:
    """Convert OpenLibrary doc to MCAuthorItem."""
    key = doc.get("key", "")
    return MCAuthorItem(
        key=key,
        name=doc.get("name", "Unknown"),
        birth_date=doc.get("birth_date"),
        top_subjects=doc.get("top_subjects", []),
        top_work=doc.get("top_work"),
        work_count=doc.get("work_count", 0),
        source_id=key,
        openlibrary_key=key,
        openlibrary_url=f"https://openlibrary.org{key}" if key else None,
    )


async def process_author_direct(
    session: aiohttp.ClientSession,
    rate_limiter: AsyncLimiter,
    wd_author: dict[str, Any],
) -> MCAuthorItem | None:
    """
    Process a Wikidata author with known OpenLibrary ID via direct lookup.

    Args:
        session: Shared aiohttp session
        rate_limiter: Rate limiter for API calls
        wd_author: Wikidata author dictionary (must have ol_id)

    Returns:
        MCAuthorItem enriched with Wikidata data, or None if not found
    """
    ol_id = wd_author.get("ol_id")
    if not ol_id:
        return None

    wd_id = wd_author.get("wd_id", "")

    try:
        author_data = await fetch_author_by_olid(session, rate_limiter, ol_id)
        if not author_data:
            return None

        mc_author = ol_author_json_to_mc_author(author_data, ol_id)

        # Enrich with Wikidata IDs
        if not mc_author.remote_ids:
            mc_author.remote_ids = {}

        if wd_id:
            mc_author.remote_ids["wikidata"] = wd_id

        viaf = wd_author.get("viaf")
        if viaf and "viaf" not in mc_author.remote_ids:
            mc_author.remote_ids["viaf"] = viaf

        isni = wd_author.get("isni")
        if isni and "isni" not in mc_author.remote_ids:
            mc_author.remote_ids["isni"] = isni

        lccn = wd_author.get("lccn")
        if lccn and "lccn" not in mc_author.remote_ids:
            mc_author.remote_ids["lccn"] = lccn

        return mc_author

    except Exception as e:
        logger.error(f"Error fetching '{ol_id}': {e}")
        return None


async def process_author_search(
    session: aiohttp.ClientSession,
    rate_limiter: AsyncLimiter,
    wd_author: dict[str, Any],
    min_score: float = 70.0,
) -> MCAuthorItem | None:
    """
    Process a Wikidata author by searching OpenLibrary (for authors without ol_id).

    Args:
        session: Shared aiohttp session
        rate_limiter: Rate limiter for API calls
        wd_author: Wikidata author dictionary
        min_score: Minimum score threshold for matches

    Returns:
        MCAuthorItem for the best match, enriched with Wikidata data, or None if no match found
    """
    name = wd_author.get("name", "")
    wd_id = wd_author.get("wd_id", "")

    all_matches: list[tuple[MCAuthorItem, float]] = []

    try:
        docs = await search_openlibrary_direct(session, rate_limiter, name, limit=10)

        for doc in docs:
            ol_author = ol_doc_to_mc_author(doc)
            score = score_match(wd_author, ol_author)
            if score >= min_score:
                all_matches.append((ol_author, score))

    except Exception as e:
        logger.error(f"Error processing query '{name}': {e}")
        return None

    # Remove duplicates and keep highest score
    seen_keys: set[str] = set()
    unique_matches: list[tuple[MCAuthorItem, float]] = []
    for ol_author, score in sorted(all_matches, key=lambda x: x[1], reverse=True):
        if ol_author.key not in seen_keys:
            seen_keys.add(ol_author.key)
            unique_matches.append((ol_author, score))

    if not unique_matches:
        return None

    unique_matches.sort(key=lambda x: (x[1], x[0].work_count), reverse=True)
    best_match, _ = unique_matches[0]

    # Enrich with Wikidata IDs
    if not best_match.remote_ids:
        best_match.remote_ids = {}

    if wd_id:
        best_match.remote_ids["wikidata"] = wd_id

    viaf = wd_author.get("viaf")
    if viaf and "viaf" not in best_match.remote_ids:
        best_match.remote_ids["viaf"] = viaf

    isni = wd_author.get("isni")
    if isni and "isni" not in best_match.remote_ids:
        best_match.remote_ids["isni"] = isni

    lccn = wd_author.get("lccn")
    if lccn and "lccn" not in best_match.remote_ids:
        best_match.remote_ids["lccn"] = lccn

    return best_match


async def run_pipeline(
    infile: str,
    outfile: str,
    concurrency: int = 50,
    min_score: float = 70.0,
    limit: int | None = None,
    batch_size: int = 1000,
    resume: bool = True,
    search: bool = False,
):
    """
    Main pipeline to process Wikidata authors and match to OpenLibrary.

    Args:
        infile: Path to cleansed_wiki.json
        outfile: Path to output JSONL file (or directory for batch files)
        concurrency: Number of concurrent requests
        min_score: Minimum match score threshold
        limit: Limit number of authors to process (for testing)
        batch_size: Number of authors per batch file
        resume: If True, skip already-processed batches
        search: If True, also search for authors without ol_id
    """
    # Load Wikidata authors
    wd_authors = load_wikidata_authors(infile)

    if limit:
        wd_authors = wd_authors[:limit]
        logger.info(f"Limited to {limit} authors for processing")

    # Separate authors with ol_id from those without
    authors_with_olid = [a for a in wd_authors if a.get("ol_id")]
    authors_without_olid = [a for a in wd_authors if not a.get("ol_id")]

    logger.info(f"Authors with ol_id (direct lookup): {len(authors_with_olid):,}")
    logger.info(f"Authors without ol_id: {len(authors_without_olid):,}")

    if not search and authors_without_olid:
        logger.info("Use --search to also search for authors without ol_id")

    # Determine which authors to process
    if search:
        authors_to_process = wd_authors  # Process all
        mode = "direct+search"
    else:
        authors_to_process = authors_with_olid  # Only those with ol_id
        mode = "direct"

    if not authors_to_process:
        logger.warning("No authors to process!")
        return

    # Setup batch output directory
    out_path = Path(outfile)
    if out_path.suffix:
        batch_dir = out_path.parent / out_path.stem
    else:
        batch_dir = out_path

    batch_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Writing batches to: {batch_dir}")
    logger.info(f"Mode: {mode}")

    # Rate limiter: ~5 req/sec (OpenLibrary rate limits aggressively)
    rate_limiter = AsyncLimiter(5, 1)

    # Progress tracking
    spinner_chars = ["|", "/", "-", "\\"]
    spinner_idx = 0
    processed = 0
    matched = 0
    direct_count = 0
    search_count = 0
    start_time = time.time()
    last_update_time = start_time
    update_interval = 0.2

    # Process authors in batches
    total_batches = (len(authors_to_process) + batch_size - 1) // batch_size
    logger.info(
        f"Processing {len(authors_to_process):,} authors in {total_batches} batches of {batch_size}"
    )

    # Single shared session - limit connections to match rate limiter
    connector = aiohttp.TCPConnector(limit=10, limit_per_host=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(authors_to_process))
            batch_authors = authors_to_process[start_idx:end_idx]
            batch_file = batch_dir / f"batch_{batch_num + 1:05d}.jsonl"

            # Skip if batch already exists and resume is enabled
            if resume and batch_file.exists():
                logger.info(f"Skipping batch {batch_num + 1} (already exists: {batch_file.name})")
                with batch_file.open("r", encoding="utf8") as f:
                    existing_matches = sum(1 for line in f if line.strip())
                matched += existing_matches
                processed += len(batch_authors)
                continue

            logger.info(
                f"Processing batch {batch_num + 1}/{total_batches} ({len(batch_authors)} authors)"
            )

            # Create tasks for this batch
            async def process_single(
                wd_author: dict[str, Any],
            ) -> tuple[MCAuthorItem | None, dict[str, Any], str]:
                """Process a single author, return (author, metadata, method)."""
                ol_id = wd_author.get("ol_id")
                method = "direct" if ol_id else "search"

                try:
                    if ol_id:
                        mc_author = await process_author_direct(session, rate_limiter, wd_author)
                    elif search:
                        mc_author = await process_author_search(
                            session, rate_limiter, wd_author, min_score
                        )
                    else:
                        mc_author = None

                    metadata = {
                        "wikidata_id": wd_author.get("wd_id"),
                        "wikidata_name": wd_author.get("name"),
                        "wikidata_birth_year": wd_author.get("birth_year"),
                        "ol_id": ol_id,
                        "method": method,
                        "matched": mc_author is not None,
                    }
                    return mc_author, metadata, method
                except Exception as e:
                    logger.error(f"Error processing author {wd_author.get('wd_id')}: {e}")
                    return (
                        None,
                        {
                            "wikidata_id": wd_author.get("wd_id"),
                            "error": str(e),
                            "matched": False,
                        },
                        method,
                    )

            tasks = [asyncio.create_task(process_single(author)) for author in batch_authors]

            # Process batch and write to file
            batch_matched = 0
            with batch_file.open("w", encoding="utf8") as f:
                for task in asyncio.as_completed(tasks):
                    mc_author, metadata, method = await task

                    if mc_author is not None:
                        author_dict = mc_author.model_dump(mode="json", exclude_none=True)
                        author_dict["_wikidata_metadata"] = metadata
                        f.write(json.dumps(author_dict, ensure_ascii=False) + "\n")
                        batch_matched += 1
                        matched += 1
                        if method == "direct":
                            direct_count += 1
                        else:
                            search_count += 1

                    processed += 1

                    # Update progress
                    current_time = time.time()
                    if current_time - last_update_time >= update_interval:
                        elapsed = current_time - start_time
                        rate = processed / elapsed if elapsed > 0 else 0
                        spinner_char = spinner_chars[spinner_idx % len(spinner_chars)]
                        spinner_idx += 1
                        match_pct = matched * 100 / max(processed, 1)
                        progress_msg = (
                            f"\r{spinner_char} {processed:,}/{len(authors_to_process):,} | "
                            f"{rate:.1f}/sec | "
                            f"Matched: {matched:,} ({match_pct:.1f}%) [D:{direct_count} S:{search_count}]"
                        )
                        sys.stdout.write(progress_msg)
                        sys.stdout.flush()
                        last_update_time = current_time

            logger.info(
                f"Batch {batch_num + 1} complete: {batch_matched}/{len(batch_authors)} matches"
            )

    # Clear progress line
    sys.stdout.write("\r" + " " * 100 + "\r")
    sys.stdout.flush()

    match_pct = matched * 100 / max(processed, 1)
    logger.info(f"Done! Processed {processed:,} authors, matched {matched:,} ({match_pct:.1f}%)")
    logger.info(f"  Direct lookups: {direct_count:,}")
    logger.info(f"  Search matches: {search_count:,}")
    logger.info(f"Batch files written to: {batch_dir}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Match Wikidata authors to OpenLibrary and convert to MCAuthorItem"
    )
    parser.add_argument(
        "--infile",
        default="data/openlibrary/cleansed_wiki.json",
        help="Path to cleansed_wiki.json file",
    )
    parser.add_argument(
        "--outfile",
        default="data/openlibrary/wiki_authors",
        help="Path to output directory (batches will be written as batch_00001.jsonl, etc.)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=50,
        help="Number of concurrent tasks (default: 50). BaseAPIClient will limit actual API calls to rate limit (20/sec)",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=70.0,
        help="Minimum match score threshold (default: 70.0)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of authors per batch file (default: 1000)",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Don't skip already-processed batches (reprocess everything)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of authors to process (for testing)",
    )
    parser.add_argument(
        "--search",
        action="store_true",
        help="Also search for authors without ol_id (slower, rate limited)",
    )
    args = parser.parse_args()

    asyncio.run(
        run_pipeline(
            infile=args.infile,
            outfile=args.outfile,
            concurrency=args.concurrency,
            min_score=args.min_score,
            batch_size=args.batch_size,
            resume=not args.no_resume,
            limit=args.limit,
            search=args.search,
        )
    )
