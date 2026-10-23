#!/usr/bin/env bash
# Canonical production deploy for this branch.
# Rebuilds the ETL VM image and installs the zone-stockout start retry window.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PROJECT_ID="${GCP_PROJECT_ID:-media-circle}"
REGION="us-central1"

make deploy-etl

if gcloud scheduler jobs describe etl-vm-start \
    --project="${PROJECT_ID}" \
    --location="${REGION}" >/dev/null 2>&1; then
    make setup-etl-schedule action=update
else
    make setup-etl-schedule action=create
fi
