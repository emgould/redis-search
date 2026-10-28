#!/usr/bin/env bash
# Canonical production deploy for this branch.
# passes_media_manager_filter is loaded by the ETL VM image (nightly Media Manager
# push) and by the Cloud Run web app (manual add-to-redis). The VM start
# scheduler does not load this module.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

make deploy-etl
make deploy-web
