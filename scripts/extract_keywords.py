"""Extract a deduplicated master keyword list from all TMDB TV and movie JSON files."""

import json
from pathlib import Path


def extract_keywords() -> dict[str, str]:
    """Iterate all TMDB data files and return a {id_str: keyword_name} map."""
    data_root = Path(__file__).resolve().parent.parent / "data" / "us"
    keywords: dict[str, str] = {}

    patterns = [
        ("tv", "tmdb_tv_*.json"),
        ("movie", "tmdb_movie_*.json"),
    ]

    files_processed = 0
    for subdir, glob_pattern in patterns:
        source_dir = data_root / subdir
        if not source_dir.exists():
            print(f"  Skipping {source_dir} (not found)")
            continue

        for filepath in sorted(source_dir.glob(glob_pattern)):
            with filepath.open("r", encoding="utf-8") as f:
                data = json.load(f)

            for result in data.get("results", []):
                for kw in result.get("keywords", []):
                    kw_id = str(kw["id"])
                    kw_name = kw["name"]
                    keywords[kw_id] = kw_name

            files_processed += 1
            if files_processed % 200 == 0:
                print(f"  Processed {files_processed} files, {len(keywords)} unique keywords so far")

    print(f"  Total files processed: {files_processed}")
    print(f"  Total unique keywords: {len(keywords)}")
    return keywords


def main() -> None:
    print("Extracting keywords from TMDB data files...")
    keywords = extract_keywords()

    # Sort by keyword id (numeric)
    sorted_keywords = dict(sorted(keywords.items(), key=lambda item: int(item[0])))

    output_path = Path(__file__).resolve().parent.parent / "data" / "keywords_12_31_2025.json"
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(sorted_keywords, f, indent=2, ensure_ascii=False)

    print(f"Wrote {len(sorted_keywords)} keywords to {output_path}")


if __name__ == "__main__":
    main()
