"""
Step 2 Alternative: Extract authors from local OpenLibrary dump

This is MUCH faster than API calls - extracts all matching authors in one pass.

1. Loads ol_ids from cleansed_wiki.json
2. Extracts matching authors from ol_dump_authors file
3. Outputs MCAuthorItem records enriched with Wikidata metadata
"""

import json
import sys
import time
from pathlib import Path
from typing import Any

from api.openlibrary.models import MCAuthorItem
from utils.get_logger import get_logger

logger = get_logger(__name__)


def load_wikidata_authors(path: str) -> dict[str, dict[str, Any]]:
    """
    Load cleansed Wikidata authors and index by ol_id.

    Returns:
        Dict mapping ol_id -> wikidata author dict
    """
    logger.info(f"Loading Wikidata authors from {path}")
    with open(path, encoding="utf8") as f:
        data = json.load(f)

    # Index by ol_id (only authors that have one)
    indexed = {}
    for author in data:
        ol_id = author.get("ol_id")
        if ol_id:
            indexed[ol_id] = author

    logger.info(f"Indexed {len(indexed):,} authors with ol_id")
    return indexed


def parse_ol_dump_line(line: str) -> tuple[str | None, dict[str, Any] | None]:
    """
    Parse a line from the OpenLibrary authors dump.

    Format: /type/author\t/authors/OL123A\trevision\ttimestamp\t{json}

    Returns:
        Tuple of (ol_id, author_data) or (None, None) if parse fails
    """
    try:
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 5:
            return None, None

        key = parts[1]  # e.g., "/authors/OL123A"
        if not key.startswith("/authors/"):
            return None, None

        ol_id = key.replace("/authors/", "")  # e.g., "OL123A"
        json_data = parts[4]
        author_data = json.loads(json_data)
        return ol_id, author_data
    except Exception:
        return None, None


def ol_dump_to_mc_author(data: dict[str, Any], wd_author: dict[str, Any]) -> MCAuthorItem:
    """Convert OpenLibrary dump record to MCAuthorItem, enriched with Wikidata data."""
    key = data.get("key", "")

    # Handle bio which can be string or dict
    bio = data.get("bio")
    if isinstance(bio, dict):
        bio = bio.get("value")

    mc_author = MCAuthorItem(
        key=key,
        name=data.get("name", "Unknown"),
        birth_date=data.get("birth_date"),
        death_date=data.get("death_date"),
        bio=bio,
        source_id=key,
        openlibrary_key=key,
        openlibrary_url=f"https://openlibrary.org{key}" if key else None,
        remote_ids=data.get("remote_ids", {}),
        work_count=0,  # Not available in dump
    )

    # Enrich with Wikidata IDs
    if not mc_author.remote_ids:
        mc_author.remote_ids = {}

    wd_id = wd_author.get("wd_id")
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


def extract_authors_from_dump(
    wikidata_file: str,
    dump_file: str,
    outfile: str,
):
    """
    Extract matching authors from OpenLibrary dump in a single pass.

    Args:
        wikidata_file: Path to cleansed_wiki.json
        dump_file: Path to ol_dump_authors file
        outfile: Path to output JSONL file
    """
    # Load and index wikidata authors by ol_id
    wd_by_olid = load_wikidata_authors(wikidata_file)

    if not wd_by_olid:
        logger.warning("No authors with ol_id found!")
        return

    # Progress tracking
    spinner_chars = ["|", "/", "-", "\\"]
    spinner_idx = 0
    lines_processed = 0
    found = 0
    start_time = time.time()
    last_update = start_time
    update_interval = 0.2

    logger.info(f"Scanning dump file: {dump_file}")
    logger.info(f"Looking for {len(wd_by_olid):,} authors")

    out_path = Path(outfile)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with (
        open(dump_file, encoding="utf8", errors="ignore") as f_in,
        open(outfile, "w", encoding="utf8") as f_out,
    ):
        for line in f_in:
            lines_processed += 1

            # Parse the dump line
            ol_id, author_data = parse_ol_dump_line(line)

            if ol_id and ol_id in wd_by_olid:
                # Found a match!
                wd_author = wd_by_olid[ol_id]

                # Convert to MCAuthorItem
                mc_author = ol_dump_to_mc_author(author_data, wd_author)

                # Add metadata
                author_dict = mc_author.model_dump(mode="json", exclude_none=True)
                author_dict["_wikidata_metadata"] = {
                    "wikidata_id": wd_author.get("wd_id"),
                    "wikidata_name": wd_author.get("name"),
                    "wikidata_birth_year": wd_author.get("birth_year"),
                    "ol_id": ol_id,
                    "method": "dump",
                    "matched": True,
                }

                f_out.write(json.dumps(author_dict, ensure_ascii=False) + "\n")
                found += 1

                # Early exit if we found all
                if found >= len(wd_by_olid):
                    logger.info("Found all authors!")
                    break

            # Update progress
            current_time = time.time()
            if current_time - last_update >= update_interval:
                elapsed = current_time - start_time
                rate = lines_processed / elapsed if elapsed > 0 else 0
                pct_complete = found * 100 / len(wd_by_olid)
                spinner_char = spinner_chars[spinner_idx % len(spinner_chars)]
                spinner_idx += 1

                progress_msg = (
                    f"\r{spinner_char} Scanned {lines_processed:,} lines | "
                    f"{rate:,.0f}/sec | "
                    f"Found: {found:,}/{len(wd_by_olid):,} ({pct_complete:.1f}%)"
                )
                sys.stdout.write(progress_msg)
                sys.stdout.flush()
                last_update = current_time

    # Clear progress line
    sys.stdout.write("\r" + " " * 100 + "\r")
    sys.stdout.flush()

    elapsed = time.time() - start_time
    not_found = len(wd_by_olid) - found

    logger.info(f"Done in {elapsed:.1f}s")
    logger.info(f"Found: {found:,} ({found * 100 / len(wd_by_olid):.1f}%)")
    logger.info(f"Not found (stale IDs): {not_found:,}")
    logger.info(f"Output: {outfile}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract authors from OpenLibrary dump (fast, no API calls)"
    )
    parser.add_argument(
        "--wikidata",
        default="data/openlibrary/cleansed_wiki.json",
        help="Path to cleansed_wiki.json",
    )
    parser.add_argument(
        "--dump",
        default="data/openlibrary/ol_dump_authors_2025-11-30.txt",
        help="Path to OpenLibrary authors dump",
    )
    parser.add_argument(
        "--outfile",
        default="data/openlibrary/wiki_authors_from_dump.jsonl",
        help="Output JSONL file",
    )
    args = parser.parse_args()

    extract_authors_from_dump(
        wikidata_file=args.wikidata,
        dump_file=args.dump,
        outfile=args.outfile,
    )
