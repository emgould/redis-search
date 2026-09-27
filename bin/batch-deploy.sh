#!/usr/bin/env bash
# Canonical production deploy for this branch.
# Deploys the Cloud Run search API and the ETL VM, then stamps historical
# US FlixPatrol data onto existing public Redis media documents.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

make deploy-web
make deploy-etl

# Requires FLIXPATROL_USERNAME and FLIXPATROL_API_KEY in config/etl.dev.env,
# plus an IAP tunnel (make tunnel) because ENV=dev writes through localhost:6381.
# Override with BACKFILL_ARGS, e.g. BACKFILL_ARGS="--start 2025-01-01 --end 2025-01-31".
make backfill-flixpatrol ENV=dev write=1 ARGS="${BACKFILL_ARGS:-}"
