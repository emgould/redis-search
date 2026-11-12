#!/usr/bin/env bash
# Canonical production deploy for this branch (GET /api/search skip pagination + web search UI).
# Cloud Run web only; no ETL or Redis backfill.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

make deploy-web
