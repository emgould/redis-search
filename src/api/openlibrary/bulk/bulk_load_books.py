"""
Bulk Load OpenLibrary Works (Books)

ETL pipeline to extract works from OpenLibrary dump for authors in our index:
1. Load author OLIDs from mc_authors.jsonl
2. Scan works dump and extract works by those authors
3. Output MCBookItem records as JSONL

This is MUCH faster than API calls - processes all works in minutes.
"""

import gzip
import json
import sys
import time
from pathlib import Path
from typing import Any

from api.openlibrary.models import MCBookItem
from utils.get_logger import get_logger

logger = get_logger(__name__)


def load_author_olids(authors_jsonl: str) -> set[str]:
    """
    Load OpenLibrary author IDs from mc_authors.jsonl.

    Returns:
        Set of author OLIDs (e.g., "OL1001630A")
    """
    olids: set[str] = set()
    path = Path(authors_jsonl)

    if not path.exists():
        logger.error(f"Authors JSONL not found: {authors_jsonl}")
        return olids

    with open(authors_jsonl, encoding="utf8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                author = json.loads(line)
                # Extract OLID from key like "/authors/OL1001630A"
                key = author.get("key", "")
                if key.startswith("/authors/"):
                    olid = key.replace("/authors/", "")
                    olids.add(olid)
            except Exception:
                continue

    return olids


def parse_works_dump_line(line: str) -> tuple[str | None, dict[str, Any] | None]:
    """
    Parse a line from the OpenLibrary works dump.

    Format: /type/work\t/works/OL123W\trevision\ttimestamp\t{json}

    Returns:
        (work_key, work_data) or (None, None)
    """
    try:
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 5:
            return None, None

        record_type = parts[0]
        if record_type != "/type/work":
            return None, None

        key = parts[1]
        if not key.startswith("/works/"):
            return None, None

        work_data = json.loads(parts[4])
        return key, work_data
    except Exception:
        return None, None


def extract_author_olids_from_work(work: dict[str, Any]) -> list[str]:
    """
    Extract author OLIDs from a work record.

    Works have authors in format:
    {"authors": [{"type": {"key": "/type/author_role"}, "author": {"key": "/authors/OL123A"}}]}
    or
    {"authors": [{"type": "/type/author_role", "author": {"key": "/authors/OL123A"}}]}
    """
    olids = []
    authors = work.get("authors", [])

    for author_entry in authors:
        if isinstance(author_entry, dict):
            author_ref = author_entry.get("author", {})
            if isinstance(author_ref, dict):
                author_key = author_ref.get("key", "")
                if author_key.startswith("/authors/"):
                    olid = author_key.replace("/authors/", "")
                    olids.append(olid)

    return olids


def cleanse_description(desc: str | None) -> str | None:
    """
    Remove OpenLibrary references from description.

    Strips out:
    - "See also:" sections with openlibrary links
    - "Contained in:" sections with openlibrary links
    - Any markdown links to openlibrary.org
    - Trailing dividers (----------)
    """
    import re

    if not desc:
        return desc

    # Remove markdown links to openlibrary: [text](https://openlibrary.org/...)
    desc = re.sub(r'\[([^\]]*)\]\(https?://openlibrary\.org[^\)]*\)', '', desc)

    # Remove "See also:" section and everything after if it contained OL links
    desc = re.sub(r'\n*-{5,}\n*See also:.*', '', desc, flags=re.DOTALL | re.IGNORECASE)

    # Remove "Contained in:" lines
    desc = re.sub(r'\n*Contained in:.*', '', desc, flags=re.DOTALL | re.IGNORECASE)

    # Remove standalone "See also:" sections (without divider)
    desc = re.sub(r'\n*See also:\s*\n.*', '', desc, flags=re.DOTALL | re.IGNORECASE)

    # Remove any remaining openlibrary.org URLs
    desc = re.sub(r'https?://openlibrary\.org[^\s\)]*', '', desc)

    # Clean up multiple newlines and trailing whitespace
    desc = re.sub(r'\n{3,}', '\n\n', desc)
    desc = desc.strip()

    # If description is now empty or just whitespace/dashes, return None
    if not desc or desc.replace('-', '').replace('\n', '').strip() == '':
        return None

    return desc


def extract_description(work: dict[str, Any]) -> str | None:
    """Extract and cleanse description from work, handling different formats."""
    desc = work.get("description")
    if desc is None:
        return None
    if isinstance(desc, str):
        return cleanse_description(desc)
    if isinstance(desc, dict):
        return cleanse_description(desc.get("value"))
    return None


def extract_first_sentence(work: dict[str, Any]) -> list[str]:
    """Extract first sentence(s) from work."""
    first_sentence = work.get("first_sentence")
    if not first_sentence:
        return []
    if isinstance(first_sentence, str):
        return [first_sentence]
    if isinstance(first_sentence, dict):
        value = first_sentence.get("value")
        return [value] if value else []
    if isinstance(first_sentence, list):
        sentences = []
        for item in first_sentence:
            if isinstance(item, str):
                sentences.append(item)
            elif isinstance(item, dict):
                val = item.get("value")
                if val:
                    sentences.append(val)
        return sentences
    return []


def work_to_mc_book(
    work: dict[str, Any],
    author_names: dict[str, str],
) -> MCBookItem:
    """
    Convert OpenLibrary work record to MCBookItem.

    Args:
        work: Work data from dump
        author_names: Dict mapping OLID -> author name
    """
    key = work.get("key", "")

    # Extract author info
    author_olids = extract_author_olids_from_work(work)
    author_name_list = []
    for olid in author_olids:
        name = author_names.get(olid)
        if name:
            author_name_list.append(name)

    # Primary author (first one)
    primary_author = author_name_list[0] if author_name_list else None

    # Cover handling
    covers = work.get("covers", [])
    cover_id = covers[0] if covers else None
    cover_urls: dict[str, str] = {}
    if cover_id and cover_id > 0:  # Negative cover IDs are invalid
        cover_urls = {
            "small": f"https://covers.openlibrary.org/b/id/{cover_id}-S.jpg",
            "medium": f"https://covers.openlibrary.org/b/id/{cover_id}-M.jpg",
            "large": f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg",
        }

    # Extract subjects
    subjects = work.get("subjects", [])
    if isinstance(subjects, list):
        # Limit subjects to first 20
        subjects = subjects[:20]
    else:
        subjects = []

    # Build MCBookItem
    book = MCBookItem(
        key=key,
        title=work.get("title", "Untitled"),
        openlibrary_key=key,
        openlibrary_url=f"https://openlibrary.org{key}" if key else None,
        # Authors
        author_name=author_name_list,
        author=primary_author,
        # Content
        description=extract_description(work),
        first_sentence=extract_first_sentence(work),
        # Covers
        cover_i=cover_id,
        cover_available=bool(cover_id and cover_id > 0),
        cover_urls=cover_urls,
        book_image=cover_urls.get("medium"),
        # Subjects
        subject=subjects,
        subjects=subjects,
    )

    return book


def passes_quality_filter(work: dict[str, Any]) -> bool:
    """
    Check if a work passes the quality filter.

    Quality filter:
    1. has_cover AND (has_description OR has_subjects)
    2. NOT a split edition (title contains [1/2], [2/2], etc.)
    3. NOT an adaptation (title contains [adaptation])
    4. NOT a collection entry (description contains openlibrary.org links)

    This ensures we only include original works with visual representation
    and meaningful metadata.
    """
    title = work.get("title", "")

    # Reject split editions and adaptations
    split_patterns = [
        "[1/2]", "[2/2]", "[1/3]", "[2/3]", "[3/3]",
        "[1/4]", "[2/4]", "[3/4]", "[4/4]",
        "[adaptation]", "[Adaptation]",
    ]
    for pattern in split_patterns:
        if pattern in title:
            return False

    # Check for cover
    covers = work.get("covers", [])
    has_cover = bool(covers) and any(c > 0 for c in covers if isinstance(c, int))

    if not has_cover:
        return False

    # Check for description
    desc = work.get("description")
    desc_text = ""
    if isinstance(desc, str):
        desc_text = desc.strip()
    elif isinstance(desc, dict):
        desc_text = desc.get("value", "").strip()

    has_description = bool(desc_text)

    # Reject collection/anthology entries (descriptions that START with "Contains:" followed by OL links)
    # These are not original works, just bundles of other works
    # Pattern: "Contains: - [Book1](link) - [Book2](link)..." or "Contains:\n[Book1](link)"
    desc_lower = desc_text.lower().strip()
    if desc_lower.startswith("contains:") and "openlibrary.org/works/" in desc_text:
        return False

    # Check for subjects
    subjects = work.get("subjects", [])
    has_subjects = bool(subjects) and len(subjects) > 0

    return has_description or has_subjects


def run_pipeline(
    authors_jsonl: str,
    works_dump: str,
    output_file: str,
    apply_quality_filter: bool = True,
) -> dict[str, int]:
    """
    Run the ETL pipeline:
    1. Load author OLIDs from mc_authors.jsonl
    2. Scan works dump for works by those authors
    3. Apply quality filter (optional)
    4. Output MCBookItem JSONL

    Args:
        authors_jsonl: Path to mc_authors.jsonl
        works_dump: Path to OpenLibrary works dump
        output_file: Output JSONL file
        apply_quality_filter: If True, only include works with cover AND (description OR subjects)

    Returns:
        Stats dict
    """
    stats = {
        "authors_loaded": 0,
        "works_scanned": 0,
        "works_matched_author": 0,
        "works_passed_filter": 0,
        "works_filtered_out": 0,
        "works_found": 0,
        "works_without_authors": 0,
    }

    # =========================================
    # Phase 1: Load author OLIDs and names
    # =========================================
    logger.info("=" * 60)
    logger.info("Phase 1: Loading author OLIDs from mc_authors.jsonl")
    logger.info("=" * 60)

    author_olids: set[str] = set()
    author_names: dict[str, str] = {}

    path = Path(authors_jsonl)
    if not path.exists():
        logger.error(f"Authors JSONL not found: {authors_jsonl}")
        return stats

    with open(authors_jsonl, encoding="utf8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                author = json.loads(line)
                key = author.get("key", "")
                name = author.get("name", "")
                if key.startswith("/authors/"):
                    olid = key.replace("/authors/", "")
                    author_olids.add(olid)
                    author_names[olid] = name
                    stats["authors_loaded"] += 1
            except Exception:
                continue

    logger.info(f"Loaded {stats['authors_loaded']:,} authors")

    if not author_olids:
        logger.error("No authors loaded!")
        return stats

    # =========================================
    # Phase 2: Scan works dump
    # =========================================
    logger.info("")
    logger.info("=" * 60)
    logger.info("Phase 2: Scanning OpenLibrary works dump")
    logger.info("=" * 60)

    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    opener = gzip.open if works_dump.endswith(".gz") else open
    start_time = time.time()
    last_update = start_time

    with (
        opener(works_dump, "rt", encoding="utf8", errors="ignore") as f_in,  # type: ignore
        open(output_file, "w", encoding="utf8") as f_out,
    ):
        for line in f_in:
            stats["works_scanned"] += 1

            work_key, work_data = parse_works_dump_line(line)

            if not work_key or not work_data:
                continue

            # Check if any author matches our known authors
            work_author_olids = extract_author_olids_from_work(work_data)

            if not work_author_olids:
                stats["works_without_authors"] += 1
                continue

            # Check if any author is in our set
            matching_authors = set(work_author_olids) & author_olids
            if not matching_authors:
                continue

            stats["works_matched_author"] += 1

            # Apply quality filter if enabled
            if apply_quality_filter:
                if not passes_quality_filter(work_data):
                    stats["works_filtered_out"] += 1
                    continue
                stats["works_passed_filter"] += 1

            # Convert to MCBookItem
            book = work_to_mc_book(work_data, author_names)

            # Add metadata about which authors matched
            book_dict = book.model_dump(mode="json", exclude_none=True)
            book_dict["_matching_author_olids"] = list(matching_authors)

            f_out.write(json.dumps(book_dict, ensure_ascii=False) + "\n")
            stats["works_found"] += 1

            # Progress
            current_time = time.time()
            if current_time - last_update >= 0.3:
                elapsed = current_time - start_time
                rate = stats["works_scanned"] / elapsed if elapsed > 0 else 0
                if apply_quality_filter:
                    sys.stdout.write(
                        f"\r  Scanned: {stats['works_scanned']:,} | "
                        f"Matched: {stats['works_matched_author']:,} | "
                        f"Passed filter: {stats['works_found']:,} | "
                        f"{rate:,.0f}/sec"
                    )
                else:
                    sys.stdout.write(
                        f"\r  Scanned: {stats['works_scanned']:,} | "
                        f"Found: {stats['works_found']:,} | "
                        f"{rate:,.0f}/sec"
                    )
                sys.stdout.flush()
                last_update = current_time

    sys.stdout.write("\n")

    elapsed = time.time() - start_time
    logger.info(f"Phase 2 complete in {elapsed:.1f}s")
    logger.info(f"  Works scanned: {stats['works_scanned']:,}")
    logger.info(f"  Works without authors: {stats['works_without_authors']:,}")
    logger.info(f"  Works matched known authors: {stats['works_matched_author']:,}")
    if apply_quality_filter:
        logger.info(f"  Works passed quality filter: {stats['works_passed_filter']:,}")
        logger.info(f"  Works filtered out: {stats['works_filtered_out']:,}")
    logger.info(f"  Works written: {stats['works_found']:,}")
    logger.info(f"  Output: {output_file}")

    return stats


def main(
    authors_jsonl: str = "data/openlibrary/mc_authors.jsonl",
    works_dump: str = "data/openlibrary/ol_dump_works_latest.txt",
    output_file: str = "data/openlibrary/mc_books.jsonl",
    apply_quality_filter: bool = True,
) -> dict[str, int]:
    """Main entry point."""
    logger.info("OpenLibrary Bulk Book Load")
    logger.info(f"  Authors JSONL: {authors_jsonl}")
    logger.info(f"  Works dump: {works_dump}")
    logger.info(f"  Output: {output_file}")
    logger.info(f"  Quality filter: {apply_quality_filter}")
    if apply_quality_filter:
        logger.info("  Filter: cover AND (description OR subjects)")

    return run_pipeline(
        authors_jsonl=authors_jsonl,
        works_dump=works_dump,
        output_file=output_file,
        apply_quality_filter=apply_quality_filter,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Bulk load OpenLibrary works for known authors"
    )
    parser.add_argument(
        "--authors-jsonl",
        default="data/openlibrary/mc_authors.jsonl",
        help="Path to mc_authors.jsonl with known authors",
    )
    parser.add_argument(
        "--works-dump",
        default="data/openlibrary/ol_dump_works_latest.txt",
        help="Path to OpenLibrary works dump file",
    )
    parser.add_argument(
        "--output",
        default="data/openlibrary/mc_books.jsonl",
        help="Output JSONL file",
    )
    parser.add_argument(
        "--no-quality-filter",
        action="store_true",
        help="Disable quality filter (include all works, not just those with cover AND (description OR subjects))",
    )
    args = parser.parse_args()

    stats = main(
        authors_jsonl=args.authors_jsonl,
        works_dump=args.works_dump,
        output_file=args.output,
        apply_quality_filter=not args.no_quality_filter,
    )

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"Final: {stats['works_found']:,} books extracted")
    logger.info("=" * 60)

