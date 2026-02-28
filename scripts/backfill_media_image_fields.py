#!/usr/bin/env python3
"""
Run a full media backfill to repopulate image-related Redis fields.

This wrapper reuses the existing TMDB reindex job and defaults to `--force`
so all movie/tv media docs are rewritten with the latest normalizer shape:
    - poster_path
    - backdrop_path
    - cast_images

Usage:
    python scripts/backfill_media_image_fields.py
    python scripts/backfill_media_image_fields.py --dry-run --scan-count 100
    python scripts/backfill_media_image_fields.py --limit 500 --concurrency 10
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Final

SCRIPT_PATH: Final[Path] = Path(__file__).resolve().with_name(
    "backfill_media_dates_and_timestamps.py"
)


def build_command(argv: list[str]) -> list[str]:
    """Build command for the shared media backfill job."""
    if "--force" in argv:
        passthrough_args = argv
    else:
        passthrough_args = ["--force", *argv]
    return [sys.executable, str(SCRIPT_PATH), *passthrough_args]


def main() -> int:
    """Execute shared backfill job with image-field defaults."""
    command = build_command(sys.argv[1:])
    completed = subprocess.run(command, check=False)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
