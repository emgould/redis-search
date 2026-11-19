#!/usr/bin/env bash
# Canonical production deploy: Cloud Run web (search API + UI, incl. Media Manager zero-hit fallback).
# Cloud Run web only; no ETL or Redis backfill.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

make deploy-web
