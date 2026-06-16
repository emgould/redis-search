#!/bin/bash
# scripts/setup_gcp_secrets.sh
# One-time script to migrate secrets from local env files to GCP Secret Manager
#
# Usage: ./scripts/setup_gcp_secrets.sh [dev|prod]
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - GCP_PROJECT_ID environment variable set
#   - Local env files exist in config/ directory

set -e

ENV=${1:-dev}
PROJECT_ID=${GCP_PROJECT_ID:-"media-circle"}

echo "🚀 Setting up GCP secrets for ${ENV} environment"
echo "=============================================="

# Validate inputs
if [ -z "$PROJECT_ID" ]; then
    echo "❌ Error: GCP_PROJECT_ID environment variable is required"
    echo "   Set it with: export GCP_PROJECT_ID=your-project-id"
    exit 1
fi

ENV_FILE="config/etl.${ENV}.env"
if [ ! -f "$ENV_FILE" ]; then
    echo "❌ Error: Environment file not found: ${ENV_FILE}"
    exit 1
fi

cleanup_old_secret_versions() {
    local secret_name="$1"
    local latest_version
    local version

    latest_version=$(gcloud secrets versions list "${secret_name}" \
        --filter="state!=DESTROYED" \
        --sort-by="~createTime" \
        --limit=1 \
        --format="value(name)" \
        --project="${PROJECT_ID}")

    if [ -z "${latest_version}" ]; then
        echo "   ⚠️  No active versions found for ${secret_name}; skipping cleanup"
        return
    fi

    echo "   Cleaning up older versions for ${secret_name} (keeping version ${latest_version})..."

    while IFS= read -r version; do
        if [ -z "${version}" ] || [ "${version}" = "${latest_version}" ]; then
            continue
        fi

        echo "      Destroying old version ${version}"
        gcloud secrets versions destroy "${version}" \
            --secret="${secret_name}" \
            --project="${PROJECT_ID}" \
            --quiet
    done < <(gcloud secrets versions list "${secret_name}" \
        --filter="state!=DESTROYED" \
        --sort-by="~createTime" \
        --format="value(name)" \
        --project="${PROJECT_ID}")
}

echo "📋 Project: ${PROJECT_ID}"
echo "📁 Source:  ${ENV_FILE}"
echo ""

# Confirm before proceeding
read -p "⚠️  This will create/update secrets in GCP. Continue? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

# =============================================================================
# Create API Environment Bundle (minimal secrets for API)
# =============================================================================
echo ""
echo "📦 Creating API environment bundle..."
echo "   API Environment File: ${API_ENV_FILE}"
echo "   The is the fast api service in front of redis"
API_ENV_FILE="config/api.${ENV}.env"
API_SECRET_NAME="redis-search-${ENV}-api-env"

if [ ! -f "$API_ENV_FILE" ]; then
    echo "   ⚠️  Skipping API bundle (file not found: ${API_ENV_FILE})"
else
    # Create the secret if it doesn't exist
    if ! gcloud secrets describe "${API_SECRET_NAME}" --project="${PROJECT_ID}" &>/dev/null; then
        echo "   Creating secret: ${API_SECRET_NAME}"
        gcloud secrets create "${API_SECRET_NAME}" \
            --replication-policy="automatic" \
            --project="${PROJECT_ID}"
    else
        echo "   Secret exists: ${API_SECRET_NAME}"
    fi

    # Add new version with current env file contents
    echo "   Adding new version from ${API_ENV_FILE}..."
    gcloud secrets versions add "${API_SECRET_NAME}" \
        --data-file="${API_ENV_FILE}" \
        --project="${PROJECT_ID}"
    cleanup_old_secret_versions "${API_SECRET_NAME}"
    
    echo "   ✅ API bundle created: ${API_SECRET_NAME}"
fi

# =============================================================================
# Create ETL Environment Bundle (minimal secrets for API)
# =============================================================================
echo ""
echo "📦 Creating ETL environment bundle..."
echo "   ETL Environment File: ${ETL_ENV_FILE}"
echo "   The is the etl service that loads the data into redis"
ETL_ENV_FILE="config/etl.${ENV}.env"
ETL_SECRET_NAME="redis-search-${ENV}-etl-env"

if [ ! -f "$ETL_ENV_FILE" ]; then
    echo "   ⚠️  Skipping ETL bundle (file not found: ${ETL_ENV_FILE})"
else
    # Create the secret if it doesn't exist
    if ! gcloud secrets describe "${ETL_SECRET_NAME}" --project="${PROJECT_ID}" &>/dev/null; then
        echo "   Creating secret: ${ETL_SECRET_NAME}"
        gcloud secrets create "${ETL_SECRET_NAME}" \
            --replication-policy="automatic" \
            --project="${PROJECT_ID}"
    else
        echo "   Secret exists: ${ETL_SECRET_NAME}"
    fi

    # Add new version with current env file contents
    echo "   Adding new version from ${ETL_ENV_FILE}..."
    gcloud secrets versions add "${ETL_SECRET_NAME}" \
        --data-file="${ETL_ENV_FILE}" \
        --project="${PROJECT_ID}"
    cleanup_old_secret_versions "${ETL_SECRET_NAME}"
    
    echo "   ✅ ETL bundle created: ${ETL_SECRET_NAME}"
fi


# =============================================================================
# Summary
# =============================================================================
echo ""
echo "=============================================="
echo "✅ GCP Secret Manager setup complete!"
echo ""
echo "Secrets created:"
echo "  • ${ETL_SECRET_NAME} (full env bundle for ETL)"
echo "  • ${API_SECRET_NAME} (full env bundle for API)"
echo ""
echo "📋 Next steps:"
echo ""
echo "1. Grant access to your service accounts:"
echo ""
echo "   # For ETL service:"
echo "   gcloud secrets add-iam-policy-binding ${ETL_SECRET_NAME} \\"
echo "     --member='serviceAccount:YOUR_SA@${PROJECT_ID}.iam.gserviceaccount.com' \\"
echo "     --role='roles/secretmanager.secretAccessor' \\"
echo "     --project='${PROJECT_ID}'"
echo ""
echo "   # For Search API:"
echo "   gcloud secrets add-iam-policy-binding redis-search-${ENV}-redis-host \\"
echo "     --member='serviceAccount:YOUR_SA@${PROJECT_ID}.iam.gserviceaccount.com' \\"
echo "     --role='roles/secretmanager.secretAccessor' \\"
echo "     --project='${PROJECT_ID}'"
echo ""
echo "2. Test locally:"
echo "   export GCP_PROJECT_ID=${PROJECT_ID}"
echo "   source scripts/load_secrets.sh ${ENV} etl"
echo ""
echo "3. For quick local dev (skip Secret Manager):"
echo "   LOCAL_DEV=true source scripts/load_secrets.sh ${ENV} etl"

