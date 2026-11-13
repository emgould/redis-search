#!/usr/bin/env bash
# Canonical production deploy for this branch (dual source-hint media search, search API/web UI).
# Cloud Run web only; no ETL or Redis backfill.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

make deploy-web
