#!/bin/bash
# scripts/load_secrets.sh
# Load secrets from GCP Secret Manager or local env files
#
# Usage: 
#   source scripts/load_secrets.sh [dev|prod|local] [etl|search_api]
#
# Environment Variables:
#   GCP_PROJECT_ID  - Required for GCP Secret Manager (not needed when ENV=local)

set -e

ENV=${1:-dev}
SERVICE=${2:-etl}
PROJECT_ID=${GCP_PROJECT_ID:-"media-circle"}

echo "🔐 Loading secrets for ${SERVICE} in ${ENV} environment..."

# =============================================================================
# LOCAL DEVELOPMENT MODE
# =============================================================================
# If ENV is "local", use local env file instead of GCP Secret Manager
# This allows faster iteration without hitting Secret Manager

if [ "$ENV" = "local" ]; then
    LOCAL_ENV_FILE="config/local.env"
    
    # Try relative path first, then absolute from script location
    if [ ! -f "$LOCAL_ENV_FILE" ]; then
        SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
        LOCAL_ENV_FILE="${SCRIPT_DIR}/../config/local.env"
    fi
    
    if [ -f "$LOCAL_ENV_FILE" ]; then
        echo "📁 Using local env file: ${LOCAL_ENV_FILE}"
        set -a  # auto-export all variables
        source "$LOCAL_ENV_FILE"
        set +a
        echo "✅ Loaded secrets from local file"
        return 0 2>/dev/null || exit 0
    else
        echo "⚠️  ENV=local but no local env file found at ${LOCAL_ENV_FILE}"
        echo "    Please create ${LOCAL_ENV_FILE} or use a different environment"
        return 1 2>/dev/null || exit 1
    fi
fi

# =============================================================================
# GCP SECRET MANAGER
# =============================================================================

# Validate GCP_PROJECT_ID
if [ -z "$PROJECT_ID" ]; then
    echo "❌ Error: GCP_PROJECT_ID environment variable is required"
    echo "   Set it with: export GCP_PROJECT_ID=your-project-id"
    return 1 2>/dev/null || exit 1
fi

# Check if gcloud is available
if ! command -v gcloud &> /dev/null; then
    echo "❌ Error: gcloud CLI not found"
    echo "   Install it from: https://cloud.google.com/sdk/docs/install"
    return 1 2>/dev/null || exit 1
fi

# Verify GCP authentication
if ! gcloud auth print-identity-token &>/dev/null 2>&1; then
    # Try application-default credentials
    if ! gcloud auth application-default print-access-token &>/dev/null 2>&1; then
        echo "⚠️  GCP authentication may not be configured"
        echo "   For local dev, run: gcloud auth application-default login"
        echo "   In Cloud Run, ensure service account has secretAccessor role"
    fi
fi

echo "🔑 Fetching secrets from GCP Secret Manager (project: ${PROJECT_ID})..."

# Determine config file path (relative to repo root)
CONFIG_FILE="config/${SERVICE}.${ENV}.env"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONFIG_FILE_ABS="${REPO_ROOT}/${CONFIG_FILE}"

# -----------------------------------------------------------------------------
# ETL Service - needs all secrets (full env bundle)
# -----------------------------------------------------------------------------
if [ "$SERVICE" = "etl" ]; then
    SECRET_NAME="redis-search-${ENV}-etl-env"
    
    echo "   Fetching full environment bundle: ${SECRET_NAME}"
    
    # Create temp file for the env
    TEMP_ENV=$(mktemp)
    trap "rm -f $TEMP_ENV" EXIT
    
    if ! gcloud secrets versions access latest \
        --secret="${SECRET_NAME}" \
        --project="${PROJECT_ID}" > "$TEMP_ENV" 2>/dev/null; then
        echo "❌ Failed to fetch secret: ${SECRET_NAME}"
        echo "   Ensure the secret exists and you have access:"
        echo "   gcloud secrets describe ${SECRET_NAME} --project=${PROJECT_ID}"
        return 1 2>/dev/null || exit 1
    fi
    
    # Save to config file
    mkdir -p "$(dirname "$CONFIG_FILE_ABS")"
    cp "$TEMP_ENV" "$CONFIG_FILE_ABS"
    echo "💾 Saved secrets to: ${CONFIG_FILE}"
    
    # Source the env file
    set -a
    source "$TEMP_ENV"
    set +a
    
    echo "✅ Loaded full environment bundle for ETL ($(wc -l < "$TEMP_ENV" | tr -d ' ') variables)"

# -----------------------------------------------------------------------------
# Search API - needs minimal secrets
# -----------------------------------------------------------------------------
elif [ "$SERVICE" = "api" ] || [ "$SERVICE" = "search_api" ]; then
    SECRET_NAME="redis-search-${ENV}-api-env"
    
    echo "   Fetching full environment bundle: ${SECRET_NAME}"
    
    # Create temp file for the env
    TEMP_ENV=$(mktemp)
    trap "rm -f $TEMP_ENV" EXIT
    
    if ! gcloud secrets versions access latest \
        --secret="${SECRET_NAME}" \
        --project="${PROJECT_ID}" > "$TEMP_ENV" 2>/dev/null; then
        echo "❌ Failed to fetch secret: ${SECRET_NAME}"
        echo "   Ensure the secret exists and you have access:"
        echo "   gcloud secrets describe ${SECRET_NAME} --project=${PROJECT_ID}"
        return 1 2>/dev/null || exit 1
    fi
    
    # Save to config file
    mkdir -p "$(dirname "$CONFIG_FILE_ABS")"
    cp "$TEMP_ENV" "$CONFIG_FILE_ABS"
    echo "💾 Saved secrets to: ${CONFIG_FILE}"
    
    # Source the env file
    set -a
    source "$TEMP_ENV"
    set +a
    
    echo "✅ Loaded full environment bundle for Search API ($(wc -l < "$TEMP_ENV" | tr -d ' ') variables)"

# -----------------------------------------------------------------------------
# Unknown service
# -----------------------------------------------------------------------------
else
    echo "❌ Unknown service: ${SERVICE}"
    echo "   Supported services: etl, api, search_api"
    return 1 2>/dev/null || exit 1
fi

echo "🎉 Secrets loaded successfully"

