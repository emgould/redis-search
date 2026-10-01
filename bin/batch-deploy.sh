#!/usr/bin/env bash
# Canonical production deploy for this branch.
# Search ranking runs in the Cloud Run API. ETL and Redis data are unchanged.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

make deploy-web
