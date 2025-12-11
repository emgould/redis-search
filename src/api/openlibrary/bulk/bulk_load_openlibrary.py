"""
Bulk Load OpenLibrary Authors

Combined ETL pipeline:
1. Preprocess Wikidata dump to extract authors with ol_id
2. Extract matching authors from OpenLibrary authors dump
3. Validate OpenLibrary images (skip 43-byte placeholders)
4. Output MCAuthorItem records as JSONL

This is MUCH faster than API calls - processes all authors in minutes.
"""

import asyncio
import gzip
import json
import math
import sys
import time
from pathlib import Path
from typing import Any

import aiohttp

from api.openlibrary.models import AuthorLinks, MCAuthorItem
from utils.get_logger import get_logger

logger = get_logger(__name__)


# ============================================================================
# Step 1: Wikidata Preprocessing Helpers
# ============================================================================


def extract_year_from_claim(claim_list: list[dict]) -> int | None:
    """Extract the year from a P569/P570 time claim."""
    if not claim_list:
        return None
    try:
        value = claim_list[0]["value"]["content"]["time"]  # "+1954-11-08T00:00:00Z"
        return int(value[1:5])  # strip "+" and extract YYYY
    except Exception:
        return None


def extract_external_id(claims: dict, prop: str) -> str | None:
    """
    Extract the best external identifier (VIAF, ISNI, LCCN, OL ID, etc.).

    Prefers entries with rank='normal' or 'preferred' over 'deprecated'.
    This is critical for P648 (OpenLibrary ID) where some authors have
    multiple IDs with deprecated ones listed first.
    """
    items = claims.get(prop)
    if not items:
        return None

    # Sort by rank preference: preferred > normal > deprecated
    rank_priority = {"preferred": 0, "normal": 1, "deprecated": 2}

    best_item = None
    best_rank = 999

    for item in items:
        rank = item.get("rank", "normal")
        priority = rank_priority.get(rank, 1)
        if priority < best_rank:
            best_rank = priority
            best_item = item

    if best_item is None:
        return None

    try:
        return best_item["value"]["content"]
    except Exception:
        return None


def extract_aliases(aliases: dict) -> list[str]:
    """Flatten aliases from all languages into a simple list."""
    out = []
    for _lang, arr in aliases.items():
        for item in arr:
            if isinstance(item, str):
                out.append(item)
            elif isinstance(item, dict):
                val = item.get("value")
                if isinstance(val, str):
                    out.append(val)
    return out


def get_label(labels: dict) -> str | None:
    """Try to pick an English label; fallback to any other."""
    if "en" in labels:
        en_label = labels["en"]
        if isinstance(en_label, str):
            return en_label
        if isinstance(en_label, dict) and "value" in en_label:
            return en_label["value"]

    for _lang, obj in labels.items():
        if isinstance(obj, str):
            return obj
        if isinstance(obj, dict) and "value" in obj:
            return obj["value"]
    return None


def compute_author_quality_score(
    has_wikipedia: bool, sitelink_count: int, alias_count: int, birth_year: int | None
) -> float:
    """Combine several signals into an author quality score."""
    score = 0.0
    if has_wikipedia:
        score += 10.0
    score += 2.0 * math.log1p(sitelink_count)
    score += 0.2 * alias_count
    if birth_year:
        score += 1.0
    return round(score, 4)


def process_wikidata_line(line: str, human_qid: str = "Q5") -> dict[str, Any] | None:
    """
    Process a single line from the Wikidata dump.

    Returns:
        Author dict if line is a human author with ol_id, else None
    """
    try:
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 2:
            return None

        qid, raw_json = parts[0], parts[1]

        # Clean JSON
        if raw_json.startswith('"') and raw_json.endswith('"'):
            raw_json = raw_json[1:-1]
        if '""' in raw_json:
            raw_json = raw_json.replace('""', '"')

        entity = json.loads(raw_json)

        if entity.get("type") != "item":
            return None

        claims = entity.get("statements") or entity.get("claims") or {}

        # Check "instance of" (P31) includes Q5 (human)
        is_human = False
        for stmt in claims.get("P31", []):
            try:
                value_obj = stmt.get("value", {})
                value_content = (
                    value_obj.get("content") if isinstance(value_obj, dict) else value_obj
                )
                if value_content == human_qid:
                    is_human = True
                    break
            except Exception:
                continue

        if not is_human:
            return None

        # Extract OpenLibrary ID (P648) - required!
        ol_id = extract_external_id(claims, "P648")
        if not ol_id:
            return None  # Skip authors without OL ID

        # Extract metadata
        labels = entity.get("labels", {})
        aliases_dict = entity.get("aliases", {})
        sitelinks = entity.get("sitelinks", {})

        name = get_label(labels)
        if not name:
            return None

        flat_aliases = extract_aliases(aliases_dict)
        birth_year = extract_year_from_claim(claims.get("P569", []))
        death_year = extract_year_from_claim(claims.get("P570", []))
        viaf = extract_external_id(claims, "P214")
        isni = extract_external_id(claims, "P213")
        lccn = extract_external_id(claims, "P244")

        # Extract image from P18 (Wikimedia Commons image)
        wikidata_image = None
        for stmt in claims.get("P18", []):
            try:
                value_obj = stmt.get("value", {})
                if isinstance(value_obj, dict):
                    filename = value_obj.get("content") or value_obj.get("value")
                else:
                    filename = value_obj
                if filename:
                    # Use Wikimedia Commons Special:FilePath for easy URL
                    # Replace spaces with underscores in filename
                    filename = str(filename).replace(" ", "_")
                    wikidata_image = (
                        f"https://commons.wikimedia.org/wiki/Special:FilePath/{filename}?width=200"
                    )
                    break
            except Exception:
                continue

        sitelink_count = len(sitelinks)
        has_wikipedia = "enwiki" in sitelinks

        score = compute_author_quality_score(
            has_wikipedia=has_wikipedia,
            sitelink_count=sitelink_count,
            alias_count=len(flat_aliases),
            birth_year=birth_year,
        )

        return {
            "wd_id": qid,
            "name": name,
            "aliases": flat_aliases,
            "birth_year": birth_year,
            "death_year": death_year,
            "wikidata_image": wikidata_image,
            "viaf": viaf,
            "isni": isni,
            "lccn": lccn,
            "ol_id": ol_id,
            "sitelinks": sitelink_count,
            "has_wikipedia": has_wikipedia,
            "author_quality_score": score,
        }

    except Exception:
        return None


# ============================================================================
# Step 2: OpenLibrary Dump Extraction Helpers
# ============================================================================


def parse_ol_dump_line(line: str) -> tuple[str | None, dict[str, Any] | None]:
    """
    Parse a line from the OpenLibrary authors dump.

    Format: /type/author\t/authors/OL123A\trevision\ttimestamp\t{json}
    """
    try:
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 5:
            return None, None

        key = parts[1]
        if not key.startswith("/authors/"):
            return None, None

        ol_id = key.replace("/authors/", "")
        author_data = json.loads(parts[4])
        return ol_id, author_data
    except Exception:
        return None, None


def ol_dump_to_mc_author(data: dict[str, Any], wd_author: dict[str, Any]) -> MCAuthorItem:
    """Convert OpenLibrary dump record to MCAuthorItem, enriched with Wikidata data."""
    key = data.get("key", "")

    bio = data.get("bio")
    if isinstance(bio, dict):
        bio = bio.get("value")

    # Extract OLID for image URL
    olid = key.replace("/authors/", "") if key else ""
    # OpenLibrary author photo URL (M = medium size, 160px)
    # Returns 1x1 transparent pixel if no photo exists
    author_image = f"https://covers.openlibrary.org/a/olid/{olid}-M.jpg" if olid else None

    # Build photo_urls with OpenLibrary and Wikidata fallback
    photo_urls: dict[str, str] = {}
    if author_image:
        photo_urls["openlibrary"] = author_image
    wikidata_image = wd_author.get("wikidata_image")
    if wikidata_image:
        photo_urls["wikidata"] = wikidata_image

    # Get remote_ids from OpenLibrary data
    remote_ids = data.get("remote_ids", {})

    # Build author_links from remote_ids and known URLs
    author_links: list[AuthorLinks] = []

    # OpenLibrary link
    if key:
        author_links.append(AuthorLinks(title="OpenLibrary", url=f"https://openlibrary.org{key}"))

    # Wikidata link
    wd_id = wd_author.get("wd_id")
    if wd_id:
        author_links.append(
            AuthorLinks(title="Wikidata", url=f"https://www.wikidata.org/wiki/{wd_id}")
        )
        remote_ids["wikidata"] = wd_id

    # Goodreads link
    if remote_ids.get("goodreads"):
        author_links.append(
            AuthorLinks(
                title="Goodreads",
                url=f"https://www.goodreads.com/author/show/{remote_ids['goodreads']}",
            )
        )

    # IMDb link
    if remote_ids.get("imdb"):
        author_links.append(
            AuthorLinks(title="IMDb", url=f"https://www.imdb.com/name/{remote_ids['imdb']}")
        )

    # VIAF link
    if remote_ids.get("viaf"):
        author_links.append(
            AuthorLinks(title="VIAF", url=f"https://viaf.org/viaf/{remote_ids['viaf']}")
        )

    # LibraryThing link
    if remote_ids.get("librarything"):
        author_links.append(
            AuthorLinks(
                title="LibraryThing",
                url=f"https://www.librarything.com/author/{remote_ids['librarything']}",
            )
        )

    # Wikipedia link (from Wikidata sitelinks - we'd need to extract this)
    # For now, skip as we don't have the Wikipedia URL directly

    mc_author = MCAuthorItem(
        key=key,
        name=data.get("name", "Unknown"),
        birth_date=data.get("birth_date"),
        death_date=data.get("death_date"),
        bio=bio,
        source_id=key,
        openlibrary_key=key,
        openlibrary_url=f"https://openlibrary.org{key}" if key else None,
        remote_ids=remote_ids,
        work_count=0,
        author_image=author_image,
        photo_urls=photo_urls,
        author_links=author_links,
    )

    # Enrich with additional Wikidata IDs
    if not mc_author.remote_ids:
        mc_author.remote_ids = {}

    for field in ["viaf", "isni", "lccn"]:
        val = wd_author.get(field)
        if val and field not in mc_author.remote_ids:
            mc_author.remote_ids[field] = val

    return mc_author


# ============================================================================
# Image Validation
# ============================================================================

# OpenLibrary returns a 43-byte transparent 1x1 GIF when no image exists
OL_PLACEHOLDER_SIZE = 43


async def check_image_valid(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
    max_retries: int = 3,
) -> bool:
    """
    Check if OpenLibrary image URL returns a real image (not 43-byte placeholder).

    OpenLibrary behavior:
    - Placeholder: Returns 200 instantly with 43-byte GIF from covers.openlibrary.org (NO redirect)
    - Valid image: Redirects to archive.org which may rate-limit or timeout under load

    Key insight: Placeholders NEVER redirect, so they NEVER get 429 or timeout.
    Therefore: 429 or timeout = VALID image (it was trying to reach archive.org)
    Only a 200 response with exactly 43 bytes = placeholder.
    """
    async with semaphore:
        for attempt in range(max_retries):
            try:
                async with session.get(
                    url,
                    allow_redirects=True,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    # Rate limited = hit archive.org = VALID (placeholders never redirect)
                    if resp.status == 429:
                        return True

                    # Redirected to archive.org = VALID
                    final_url = str(resp.url)
                    if "archive.org" in final_url:
                        return True

                    # 200 from openlibrary = check if placeholder
                    if resp.status == 200:
                        content = await resp.read()
                        return len(content) > OL_PLACEHOLDER_SIZE

                    return False

            except TimeoutError:
                # Timeout = trying to reach archive.org = VALID
                return True
            except Exception:
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.5)
                    continue
                return False

        return False


async def validate_images_batch(
    authors: list[dict],
    concurrency: int = 50,
) -> tuple[int, int]:
    """
    Validate OpenLibrary images for a batch of authors.
    Removes photo_urls.openlibrary if it's a placeholder.

    Returns:
        (valid_count, placeholder_count)
    """
    # Filter to authors with openlibrary photo_urls
    to_check = [(i, a) for i, a in enumerate(authors) if a.get("photo_urls", {}).get("openlibrary")]

    if not to_check:
        return 0, 0

    semaphore = asyncio.Semaphore(concurrency)

    connector = aiohttp.TCPConnector(limit=concurrency, limit_per_host=concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for _idx, author in to_check:
            url = author["photo_urls"]["openlibrary"]
            tasks.append(check_image_valid(session, url, semaphore))

        results = await asyncio.gather(*tasks)

    valid_count = 0
    placeholder_count = 0

    for (idx, _author), is_valid in zip(to_check, results, strict=True):
        if is_valid:
            valid_count += 1
        else:
            placeholder_count += 1
            # Remove the placeholder URL
            del authors[idx]["photo_urls"]["openlibrary"]
            # If no photo_urls left, also clear author_image
            if not authors[idx]["photo_urls"]:
                authors[idx]["author_image"] = None
            elif "wikidata" in authors[idx]["photo_urls"]:
                # Use wikidata as the primary image
                authors[idx]["author_image"] = authors[idx]["photo_urls"]["wikidata"]

    return valid_count, placeholder_count


def run_image_validation(input_file: str, output_file: str) -> dict[str, int]:
    """
    Phase 3: Validate OpenLibrary images and remove placeholders.
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("Phase 3: Validating OpenLibrary images")
    logger.info("=" * 60)

    # Load all authors
    logger.info("Loading authors from JSONL...")
    authors = []
    with open(input_file, encoding="utf8") as f:
        for line in f:
            if line.strip():
                authors.append(json.loads(line))

    total = len(authors)
    logger.info(f"Loaded {total:,} authors")

    # Process in batches to show progress
    batch_size = 10000
    valid_total = 0
    placeholder_total = 0
    start_time = time.time()

    # Check if we're already in an event loop (e.g., called from FastAPI)
    try:
        asyncio.get_running_loop()
        in_async_context = True
    except RuntimeError:
        in_async_context = False

    async def process_all_batches():
        nonlocal valid_total, placeholder_total
        for i in range(0, total, batch_size):
            batch = authors[i : i + batch_size]
            valid, placeholder = await validate_images_batch(batch)
            valid_total += valid
            placeholder_total += placeholder

            elapsed = time.time() - start_time
            checked = min(i + batch_size, total)
            rate = checked / elapsed if elapsed > 0 else 0
            sys.stdout.write(
                f"\r  Checked: {checked:,}/{total:,} | "
                f"Valid: {valid_total:,} | Placeholder: {placeholder_total:,} | "
                f"{rate:.0f}/sec"
            )
            sys.stdout.flush()

    if in_async_context:
        # We're in an async context, need to use nest_asyncio or thread
        # Use a new thread to run the async code
        import concurrent.futures

        def run_in_thread():
            asyncio.run(process_all_batches())

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(run_in_thread)
            future.result()  # Wait for completion
    else:
        # Not in async context, safe to use asyncio.run()
        asyncio.run(process_all_batches())

    sys.stdout.write("\n")

    # Write updated authors
    logger.info(f"Writing validated authors to {output_file}")
    with open(output_file, "w", encoding="utf8") as f:
        for author in authors:
            f.write(json.dumps(author, ensure_ascii=False) + "\n")

    elapsed = time.time() - start_time
    logger.info(f"Phase 3 complete in {elapsed:.1f}s")
    logger.info(f"  Valid images: {valid_total:,}")
    logger.info(f"  Placeholders removed: {placeholder_total:,}")

    return {
        "valid_images": valid_total,
        "placeholder_images": placeholder_total,
    }


# ============================================================================
# Main Pipeline
# ============================================================================


def run_pipeline(
    wikidata_dump: str,
    authors_dump: str,
    output_file: str,
    validate_images: bool = True,
) -> dict[str, int]:
    """
    Run the full ETL pipeline:
    1. Scan wikidata dump for authors with ol_id
    2. Extract those authors from OL dump
    3. Validate OpenLibrary images (optional, removes 43-byte placeholders)
    4. Output MCAuthorItem JSONL

    Args:
        wikidata_dump: Path to wikidata dump file
        authors_dump: Path to OpenLibrary authors dump
        output_file: Output JSONL file
        validate_images: If True, check OL images and remove placeholders

    Returns:
        Stats dict
    """
    stats = {
        "wikidata_lines": 0,
        "authors_with_olid": 0,
        "ol_lines_scanned": 0,
        "authors_found": 0,
        "authors_not_found": 0,
        "valid_images": 0,
        "placeholder_images": 0,
    }

    # =========================================
    # Phase 1: Extract authors with ol_id from Wikidata
    # =========================================
    logger.info("=" * 60)
    logger.info("Phase 1: Scanning Wikidata dump for authors with ol_id")
    logger.info("=" * 60)

    wd_by_olid: dict[str, dict[str, Any]] = {}

    opener = gzip.open if wikidata_dump.endswith(".gz") else open
    start_time = time.time()
    last_update = start_time

    with opener(wikidata_dump, "rt", encoding="utf8", errors="ignore") as f:  # type: ignore
        for line in f:
            stats["wikidata_lines"] += 1

            author = process_wikidata_line(line)
            if author:
                ol_id = author["ol_id"]
                wd_by_olid[ol_id] = author
                stats["authors_with_olid"] += 1

            # Progress
            current_time = time.time()
            if current_time - last_update >= 0.3:
                elapsed = current_time - start_time
                rate = stats["wikidata_lines"] / elapsed
                sys.stdout.write(
                    f"\r  Lines: {stats['wikidata_lines']:,} | "
                    f"Authors with ol_id: {stats['authors_with_olid']:,} | "
                    f"{rate:,.0f}/sec"
                )
                sys.stdout.flush()
                last_update = current_time

    sys.stdout.write("\n")
    elapsed = time.time() - start_time
    logger.info(f"Phase 1 complete in {elapsed:.1f}s")
    logger.info(f"  Wikidata lines: {stats['wikidata_lines']:,}")
    logger.info(f"  Authors with ol_id: {stats['authors_with_olid']:,}")

    if not wd_by_olid:
        logger.error("No authors with ol_id found!")
        return stats

    # =========================================
    # Phase 2: Extract matching authors from OL dump
    # =========================================
    logger.info("")
    logger.info("=" * 60)
    logger.info("Phase 2: Extracting from OpenLibrary authors dump")
    logger.info("=" * 60)

    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    opener = gzip.open if authors_dump.endswith(".gz") else open
    start_time = time.time()
    last_update = start_time

    with (
        opener(authors_dump, "rt", encoding="utf8", errors="ignore") as f_in,  # type: ignore
        open(output_file, "w", encoding="utf8") as f_out,
    ):
        for line in f_in:
            stats["ol_lines_scanned"] += 1

            ol_id, author_data = parse_ol_dump_line(line)

            if ol_id and ol_id in wd_by_olid:
                wd_author = wd_by_olid[ol_id]
                mc_author = ol_dump_to_mc_author(author_data, wd_author)

                # Add metadata
                author_dict = mc_author.model_dump(mode="json", exclude_none=True)
                author_dict["_wikidata_metadata"] = {
                    "wikidata_id": wd_author.get("wd_id"),
                    "wikidata_name": wd_author.get("name"),
                    "wikidata_birth_year": wd_author.get("birth_year"),
                    "ol_id": ol_id,
                    "quality_score": wd_author.get("author_quality_score"),
                }

                f_out.write(json.dumps(author_dict, ensure_ascii=False) + "\n")
                stats["authors_found"] += 1

                # Early exit if found all
                if stats["authors_found"] >= len(wd_by_olid):
                    logger.info("Found all authors!")
                    break

            # Progress
            current_time = time.time()
            if current_time - last_update >= 0.3:
                elapsed = current_time - start_time
                rate = stats["ol_lines_scanned"] / elapsed
                pct = stats["authors_found"] * 100 / len(wd_by_olid)
                sys.stdout.write(
                    f"\r  Lines: {stats['ol_lines_scanned']:,} | "
                    f"Found: {stats['authors_found']:,}/{len(wd_by_olid):,} ({pct:.1f}%) | "
                    f"{rate:,.0f}/sec"
                )
                sys.stdout.flush()
                last_update = current_time

    sys.stdout.write("\n")
    stats["authors_not_found"] = len(wd_by_olid) - stats["authors_found"]

    elapsed = time.time() - start_time
    logger.info(f"Phase 2 complete in {elapsed:.1f}s")
    logger.info(f"  OL lines scanned: {stats['ol_lines_scanned']:,}")
    logger.info(f"  Authors found: {stats['authors_found']:,}")
    logger.info(f"  Authors not found (stale IDs): {stats['authors_not_found']:,}")
    logger.info(f"  Output: {output_file}")

    # =========================================
    # Phase 3: Validate OpenLibrary images
    # =========================================
    if validate_images:
        image_stats = run_image_validation(output_file, output_file)
        stats.update(image_stats)
    else:
        logger.info("")
        logger.info("Skipping image validation (--no-validate-images)")

    return stats


def main(
    wikidata_dump: str = "data/openlibrary/ol_dump_wikidata_latest.txt",
    authors_dump: str = "data/openlibrary/ol_dump_authors_latest.txt",
    output_file: str = "data/openlibrary/mc_authors.jsonl",
    validate_images: bool = True,
) -> dict[str, int]:
    """Main entry point."""
    logger.info("OpenLibrary Bulk Author Load")
    logger.info(f"  Wikidata dump: {wikidata_dump}")
    logger.info(f"  Authors dump: {authors_dump}")
    logger.info(f"  Output: {output_file}")
    logger.info(f"  Validate images: {validate_images}")

    return run_pipeline(
        wikidata_dump=wikidata_dump,
        authors_dump=authors_dump,
        output_file=output_file,
        validate_images=validate_images,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Bulk load OpenLibrary authors from dump files")
    parser.add_argument(
        "--wikidata-dump",
        default="data/openlibrary/ol_dump_wikidata_latest.txt",
        help="Path to Wikidata dump file",
    )
    parser.add_argument(
        "--authors-dump",
        default="data/openlibrary/ol_dump_authors_latest.txt",
        help="Path to OpenLibrary authors dump file",
    )
    parser.add_argument(
        "--output",
        default="data/openlibrary/mc_authors.jsonl",
        help="Output JSONL file",
    )
    parser.add_argument(
        "--no-validate-images",
        action="store_true",
        help="Skip OpenLibrary image validation (faster but may include placeholders)",
    )
    args = parser.parse_args()

    stats = main(
        wikidata_dump=args.wikidata_dump,
        authors_dump=args.authors_dump,
        output_file=args.output,
        validate_images=not args.no_validate_images,
    )

    success_rate = stats["authors_found"] * 100 / max(stats["authors_with_olid"], 1)
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"Final: {stats['authors_found']:,} authors ({success_rate:.1f}% success)")
    logger.info("=" * 60)
