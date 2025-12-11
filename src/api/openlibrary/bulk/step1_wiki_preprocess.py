import gzip
import json
import math
import sys
import time

###########################
# Utility helper functions
###########################


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
    """Extract the first external identifier (VIAF, ISNI, LCCN, etc.)."""
    items = claims.get(prop)
    if not items:
        return None
    try:
        return items[0]["value"]["content"]
    except Exception:
        return None


def extract_aliases(aliases: dict) -> list[str]:
    """Flatten aliases from all languages into a simple list."""
    out = []
    for _lang, arr in aliases.items():
        for item in arr:
            # Handle both formats: string directly or {"value": "string"}
            if isinstance(item, str):
                out.append(item)
            elif isinstance(item, dict):
                val = item.get("value")
                if isinstance(val, str):
                    out.append(val)
    return out


def get_label(labels: dict) -> str | None:
    """Try to pick an English label; fallback to any other."""
    # Handle both formats: {"en": "value"} or {"en": {"value": "value"}}
    if "en" in labels:
        en_label = labels["en"]
        if isinstance(en_label, str):
            return en_label
        if isinstance(en_label, dict) and "value" in en_label:
            return en_label["value"]

    # fallback: take first available
    for _lang, obj in labels.items():
        if isinstance(obj, str):
            return obj
        if isinstance(obj, dict) and "value" in obj:
            return obj["value"]
    return None


##################################
#  Author Quality Score
##################################


def compute_author_quality_score(
    has_wikipedia: bool, sitelink_count: int, alias_count: int, birth_year: int | None
) -> float:
    """
    Combine several signals into an author quality score:
      +10 for having Wikipedia
      +2*log(sitelinks+1)
      +0.2*alias_count
      +1 if birth_year exists
    """
    score = 0.0
    if has_wikipedia:
        score += 10.0
    score += 2.0 * math.log1p(sitelink_count)
    score += 0.2 * alias_count
    if birth_year:
        score += 1.0
    return round(score, 4)


##################################
#  Main Wikidata Ingest Function
##################################


def process_wikidata_tsv(
    infile: str,
    outfile: str,
    human_qid: str = "Q5",
):
    """
    Streams a Wikidata dump (tsv with QID, JSON-string, timestamp),
    extracts human authors, and outputs a JSON array of lean records.
    Streams both input and output for memory efficiency.
    """
    spinner_chars = ["|", "/", "-", "\\"]
    spinner_idx = 0
    last_update_time = time.time()
    update_interval = 0.1  # Update spinner every 0.1 seconds
    update_every_n_lines = 100  # Also update every N lines
    author_count = 0
    is_first_author = True

    # Choose the correct opener for gz or normal file
    opener = gzip.open if infile.endswith(".gz") else open

    print(f"Opening file: {infile}")
    with (
        opener(infile, "rt", encoding="utf8", errors="ignore") as f_in,
        open(outfile, "w", encoding="utf8") as f_out,
    ):
        print("File opened, starting to process...")
        # Write opening bracket for JSON array
        f_out.write("[\n")
        f_out.flush()

        # Show initial spinner
        sys.stdout.write("\r| Processing... Lines: 0 | Authors found: 0")
        sys.stdout.flush()

        # Stream file line by line
        line_num = 0
        while True:
            line = f_in.readline()
            if not line:
                break
            line_num += 1

            # Update spinner immediately after reading line
            spinner_char = spinner_chars[spinner_idx % len(spinner_chars)]
            spinner_idx += 1
            sys.stdout.write(
                f"\r{spinner_char} Processing... Lines: {line_num:,} | Authors found: {author_count:,}"
            )
            sys.stdout.flush()

            try:
                # Split on TAB: QID, JSON payload, timestamp
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 2:
                    continue

                qid, raw_json = parts[0], parts[1]

                # Remove surrounding quotes around the JSON-encoded string
                if raw_json.startswith('"') and raw_json.endswith('"'):
                    raw_json = raw_json[1:-1]

                # Handle double-escaped quotes ("" -> ")
                # Use a more efficient approach for large strings
                if '""' in raw_json:
                    raw_json = raw_json.replace('""', '"')

                # Load payload as JSON
                try:
                    entity = json.loads(raw_json)
                except json.JSONDecodeError as e:
                    if line_num <= 5:  # Debug first few lines
                        print(f"\nLine {line_num} JSON decode error: {e}")
                        print(f"  QID: {qid}, JSON preview: {raw_json[:100]}...")
                    continue

                # Entity type must be item
                if entity.get("type") != "item":
                    continue

                claims = entity.get("statements") or entity.get("claims") or {}

                # Check "instance of" (P31) includes Q5 (human)
                is_human = False
                p31_claims = claims.get("P31", [])
                for stmt in p31_claims:
                    try:
                        # Handle different possible structures
                        value_content = None
                        if isinstance(stmt, dict):
                            value_obj = stmt.get("value", {})
                            if isinstance(value_obj, dict):
                                value_content = value_obj.get("content")
                            else:
                                value_content = value_obj

                        if value_content == human_qid:
                            is_human = True
                            break
                    except Exception as e:
                        if line_num <= 5:
                            print(f"\n  Error checking P31: {e}, stmt: {stmt}")
                        continue

                if not is_human:
                    # Debug: show why first few entities aren't humans
                    if line_num <= 5:
                        print(f"\n  Line {line_num} (QID: {qid}): Not human")
                        print(f"    P31 claims: {len(p31_claims)}")
                        if p31_claims:
                            print(f"    First P31: {p31_claims[0]}")
                    continue  # skip non-human entities

                # Extract metadata
                labels = entity.get("labels", {})
                aliases = entity.get("aliases", {})
                sitelinks = entity.get("sitelinks", {})
                sitelink_count = len(sitelinks)
                has_wikipedia = "enwiki" in sitelinks

                # Best label
                name = get_label(labels)
                if not name:
                    continue  # require at least some label

                # Aliases
                flat_aliases = extract_aliases(aliases)

                # Birth/death years
                birth_year = extract_year_from_claim(claims.get("P569", []))
                death_year = extract_year_from_claim(claims.get("P570", []))

                # External IDs
                viaf = extract_external_id(claims, "P214")
                isni = extract_external_id(claims, "P213")
                lccn = extract_external_id(claims, "P244")
                ol_id = extract_external_id(claims, "P648")  # OpenLibrary author ID

                # Compute author quality score
                score = compute_author_quality_score(
                    has_wikipedia=has_wikipedia,
                    sitelink_count=sitelink_count,
                    alias_count=len(flat_aliases),
                    birth_year=birth_year,
                )

                # Build lean author object
                author_obj = {
                    "wd_id": qid,
                    "name": name,
                    "aliases": flat_aliases,
                    "birth_year": birth_year,
                    "death_year": death_year,
                    "viaf": viaf,
                    "isni": isni,
                    "lccn": lccn,
                    "ol_id": ol_id,  # OpenLibrary author ID (P648)
                    "sitelinks": sitelink_count,
                    "has_wikipedia": has_wikipedia,
                    "author_quality_score": score,
                }

                # Stream write to output file
                if not is_first_author:
                    f_out.write(",\n")
                f_out.write("  ")
                json.dump(author_obj, f_out, ensure_ascii=False)
                f_out.flush()  # Ensure data is written immediately
                author_count += 1
                is_first_author = False

            except Exception as e:
                # Log errors for debugging, especially on first few lines
                if line_num <= 10:
                    print(f"\nError processing line {line_num}: {type(e).__name__}: {e}")
                    import traceback

                    traceback.print_exc()
                continue

            # Update spinner progress bar (every N lines or every time interval)
            # Update more frequently for first 1000 lines to show immediate progress
            update_frequency = 10 if line_num < 1000 else update_every_n_lines
            current_time = time.time()
            should_update = (
                line_num % update_frequency == 0
                or current_time - last_update_time >= update_interval
                or line_num == 1  # Always update on first line
            )
            if should_update:
                spinner_char = spinner_chars[spinner_idx % len(spinner_chars)]
                spinner_idx += 1
                progress_msg = (
                    f"\r{spinner_char} Processing... "
                    f"Lines: {line_num:,} | "
                    f"Authors found: {author_count:,}"
                )
                sys.stdout.write(progress_msg)
                sys.stdout.flush()
                last_update_time = current_time

        # Write closing bracket for JSON array
        f_out.write("\n]")
        f_out.flush()

    # Clear the progress line and print final status
    sys.stdout.write("\r" + " " * 80 + "\r")
    sys.stdout.flush()

    print(f"Done. Wrote {author_count:,} records to: {outfile}")


##################################
# Example Usage
##################################

if __name__ == "__main__":
    from pathlib import Path

    # Assume script is run from project root
    # Paths are relative to current working directory
    project_root = Path.cwd()
    data_dir = project_root / "data" / "openlibrary"
    infile = data_dir / "ol_dump_wikidata_2025-11-30.txt"
    outfile = data_dir / "cleansed_wiki.json"

    if not infile.exists():
        print(f"Error: Input file not found: {infile}")
        print(f"Current working directory: {project_root}")
        print("Please run this script from the project root directory.")
        exit(1)

    process_wikidata_tsv(
        infile=str(infile),
        outfile=str(outfile),
    )
