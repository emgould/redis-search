#!/usr/bin/env bash
# Canonical production deploy for this branch.
# Deploys Cloud Run search API + ETL VM, then backfills person filmography IDs
# on public Redis so exact-match search can rewrite tv/movie from credits.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

make deploy-web
make deploy-etl

# Existing person:* docs need movie_credit_ids / tv_credit_ids.
# Requires IAP tunnel (make tunnel) and TMDB_READ_TOKEN from config/etl.dev.env.
# Override with BACKFILL_ARGS, e.g. BACKFILL_ARGS="--limit 100" or "--dry-run".
make backfill-person-credit-ids REDIS=dev ARGS="${BACKFILL_ARGS:-}"
