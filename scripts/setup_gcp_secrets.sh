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
PROJECT_ID=${GCP_PROJECT_ID:-""}

echo "🚀 Setting up GCP secrets for ${ENV} environment"
echo "=============================================="

# Validate inputs
if [ -z "$PROJECT_ID" ]; then
    echo "❌ Error: GCP_PROJECT_ID environment variable is required"
    echo "   Set it with: export GCP_PROJECT_ID=your-project-id"
    exit 1
fi

ENV_FILE="config/${ENV}.env"
if [ ! -f "$ENV_FILE" ]; then
    echo "❌ Error: Environment file not found: ${ENV_FILE}"
    exit 1
fi

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
# Create ETL Environment Bundle (all secrets as one file)
# =============================================================================
echo ""
echo "📦 Creating ETL environment bundle..."

ETL_SECRET_NAME="redis-search-${ENV}-etl-env"

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
echo "   Adding new version from ${ENV_FILE}..."
gcloud secrets versions add "${ETL_SECRET_NAME}" \
    --data-file="${ENV_FILE}" \
    --project="${PROJECT_ID}"

echo "   ✅ ETL bundle created: ${ETL_SECRET_NAME}"

# =============================================================================
# Create Individual Secrets for Search API (minimal)
# =============================================================================
echo ""
echo "🔑 Creating individual secrets for Search API..."

# Function to create/update individual secret
create_secret() {
    local secret_name=$1
    local secret_value=$2
    
    if [ -z "$secret_value" ]; then
        echo "   ⚠️  Skipping ${secret_name} (empty value)"
        return
    fi
    
    # Create the secret if it doesn't exist
    if ! gcloud secrets describe "${secret_name}" --project="${PROJECT_ID}" &>/dev/null; then
        echo "   Creating: ${secret_name}"
        gcloud secrets create "${secret_name}" \
            --replication-policy="automatic" \
            --project="${PROJECT_ID}"
    fi
    
    # Add new version
    echo "   Updating: ${secret_name}"
    echo -n "${secret_value}" | gcloud secrets versions add "${secret_name}" \
        --data-file=- \
        --project="${PROJECT_ID}"
}

# Extract values from env file
REDIS_HOST=$(grep -E "^REDIS_HOST=" "${ENV_FILE}" | cut -d'=' -f2- | tr -d '"' | tr -d "'")
REDIS_PORT=$(grep -E "^REDIS_PORT=" "${ENV_FILE}" | cut -d'=' -f2- | tr -d '"' | tr -d "'")

# Create individual secrets
create_secret "redis-search-${ENV}-redis-host" "${REDIS_HOST}"
create_secret "redis-search-${ENV}-redis-port" "${REDIS_PORT}"

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "=============================================="
echo "✅ GCP Secret Manager setup complete!"
echo ""
echo "Secrets created:"
echo "  • ${ETL_SECRET_NAME} (full env bundle for ETL)"
echo "  • redis-search-${ENV}-redis-host"
echo "  • redis-search-${ENV}-redis-port"
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

