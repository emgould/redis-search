#!/usr/bin/env python3
"""
Verify the local PodcastIndex SQLite dump contains After-Shows feeds.

Run from repo root with venv activated:
    python scripts/verify_after_shows_source_db.py

Exit code 0 on success, 1 on any assertion failure.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import _bootstrap
from dotenv import load_dotenv

_ = _bootstrap

DEFAULT_DB_PATH = "data/podcastindex/podcastindex_feeds.db"
AFTER_SHOWS_VALUE = "after-shows"
_AFTER_SHOWS_WHERE = " OR ".join(
    [f"LOWER(TRIM(category{i})) = '{AFTER_SHOWS_VALUE}'" for i in range(1, 11)]
)


def _fail(msg: str) -> None:
    print(f"FAIL: {msg}")
    raise AssertionError(msg)


def verify_source_db(db_path: Path) -> None:
    if not db_path.exists():
        _fail(f"SQLite dump not found: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        total = cur.execute("SELECT COUNT(*) FROM podcasts").fetchone()
        after_shows = cur.execute(
            f"SELECT COUNT(*) FROM podcasts WHERE {_AFTER_SHOWS_WHERE}"
        ).fetchone()
        samples = cur.execute(
            f"""
            SELECT title
            FROM podcasts
            WHERE {_AFTER_SHOWS_WHERE}
            ORDER BY popularityScore DESC, episodeCount DESC
            LIMIT 5
            """
        ).fetchall()
    finally:
        conn.close()

    total_count = int(total[0]) if total else 0
    after_shows_count = int(after_shows[0]) if after_shows else 0
    if total_count <= 0:
        _fail("podcasts table is empty")
    if after_shows_count <= 0:
        _fail("no after-shows rows found in the local PodcastIndex SQLite dump")

    print(f"OK: podcasts table contains {total_count:,} rows")
    print(f"OK: after-shows rows found = {after_shows_count:,}")
    sample_titles = [row[0] for row in samples if row and row[0]]
    if sample_titles:
        print("OK: sample after-shows titles:")
        for title in sample_titles:
            print(f"  - {title}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify local PodcastIndex After-Shows source coverage")
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DB_PATH,
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )
    args = parser.parse_args()

    env_file = "config/local.env"
    load_dotenv(env_file)
    verify_source_db(Path(args.db_path))
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError:
        raise SystemExit(1)
