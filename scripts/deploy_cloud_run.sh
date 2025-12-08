#!/bin/bash
# Deploy Redis Search Web App to Google Cloud Run
#
# This deploys the unified web app which includes:
#   - Search API (/autocomplete, /search)
#   - ETL endpoints (/api/etl/trigger, /api/etl/status)
#   - Management UI (/etl, /management)
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - VPC connector 'mc-vpc-connector' exists in us-central1
#   - Redis Stack VM running (scripts/create_redis_vm.sh)
#   - Secrets created (scripts/setup_gcp_secrets.sh)
#
# Usage:
#   ./scripts/deploy_cloud_run.sh
#   make deploy

set -e

PROJECT_ID=${GCP_PROJECT_ID:-"media-circle"}
REGION="us-central1"
VPC_CONNECTOR="mc-vpc-connector"

SERVICE_NAME="redis-search-api-dev"
SECRET_NAME="redis-search-dev-etl-env"

IMAGE="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"

# Service account (Cloud Run default compute SA)
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
SERVICE_ACCOUNT="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

echo "============================================================"
echo "🚀 Deploying Redis Search Web App"
echo "============================================================"
echo ""
echo "   Project:          ${PROJECT_ID}"
echo "   Region:           ${REGION}"
echo "   Service:          ${SERVICE_NAME}"
echo "   Image:            ${IMAGE}"
echo "   Secret:           ${SECRET_NAME}"
echo "   Service Account:  ${SERVICE_ACCOUNT}"
echo ""

# Check gcloud auth
if ! gcloud auth print-identity-token &>/dev/null; then
    echo "❌ Not authenticated. Run: gcloud auth login"
    exit 1
fi

gcloud config set project "$PROJECT_ID" 2>/dev/null

# Grant secret access to service account
echo "🔐 Ensuring secret access..."
if gcloud secrets get-iam-policy "${SECRET_NAME}" --project="${PROJECT_ID}" 2>/dev/null | grep -q "${SERVICE_ACCOUNT}"; then
    echo "   ✅ Secret access already granted"
else
    echo "   Granting secret access..."
    gcloud secrets add-iam-policy-binding "${SECRET_NAME}" \
        --member="serviceAccount:${SERVICE_ACCOUNT}" \
        --role="roles/secretmanager.secretAccessor" \
        --project="${PROJECT_ID}" --quiet || {
        echo "   ⚠️  Failed to grant secret access. You may need to do this manually."
    }
fi

# Grant GCS access for ETL metadata
GCS_BUCKET="mc-redis-etl"
echo "🪣 Ensuring GCS bucket access..."
gsutil iam ch "serviceAccount:${SERVICE_ACCOUNT}:objectAdmin" "gs://${GCS_BUCKET}" 2>/dev/null || {
    echo "   ⚠️  Failed to grant GCS access. ETL metadata may not persist."
}
echo "   ✅ GCS access configured"

# Build the image
echo ""
echo "📦 Building Docker image..."
gcloud builds submit \
    --config=cloudbuild.yaml \
    --substitutions="_IMAGE=${IMAGE},_DOCKERFILE=src/search_api/Dockerfile" \
    .

# Deploy to Cloud Run
echo ""
echo "🚀 Deploying to Cloud Run..."
gcloud run deploy "${SERVICE_NAME}" \
    --image "${IMAGE}" \
    --region "${REGION}" \
    --platform managed \
    --vpc-connector "${VPC_CONNECTOR}" \
    --vpc-egress all-traffic \
    --set-secrets "ETL_ENV=${SECRET_NAME}:latest" \
    --allow-unauthenticated \
    --memory 1Gi \
    --timeout 3600 \
    --min-instances 0 \
    --max-instances 10

# Get the URL
URL=$(gcloud run services describe "${SERVICE_NAME}" --region="${REGION}" --format="value(status.url)")

echo ""
echo "============================================================"
echo "✅ Deployment Complete!"
echo "============================================================"
echo ""
echo "   Service URL:  ${URL}"
echo ""
echo "   Endpoints:"
echo "   • Health:      ${URL}/health"
echo "   • Search:      ${URL}/autocomplete?q=star"
echo "   • ETL Trigger: ${URL}/api/etl/trigger (POST)"
echo "   • ETL UI:      ${URL}/etl"
echo ""
echo "   Test ETL trigger:"
echo "   curl -X POST '${URL}/api/etl/trigger' \\"
echo "     -H 'X-API-Key: dev-etl-trigger-key-2024' \\"
echo "     -H 'Content-Type: application/json'"
echo ""
