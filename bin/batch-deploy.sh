#!/usr/bin/env bash
# Canonical production deploy for this PR:
# - Cloud Run web (TMDB get_media_details Watchmode provider enrichment)
# - ETL VM (nightly microgenre gap-fill job + classify/skip-empty writes)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

make deploy-web
make deploy-etl
