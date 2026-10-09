#!/usr/bin/env bash
# Canonical production deploy for this branch.
# The movie runtime gate runs inside the ETL VM container (3 AM Eastern cron).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

make deploy-etl
