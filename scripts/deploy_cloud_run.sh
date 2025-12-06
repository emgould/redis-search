#!/bin/bash
# Deploy Redis Search API to Google Cloud Run
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - VPC connector 'mc-vpc-connector' exists in us-central1
#   - Redis Stack VM created (run scripts/create_redis_vm.sh first)
#   - Secrets created with setup_gcp_secrets.sh (contains REDIS_HOST, etc.)
#
# Usage:
#   ./scripts/deploy_cloud_run.sh [api|etl] [prod|dev]

set -e

SERVICE_TYPE=${1:-api}
ENVIRONMENT=${2:-prod}

PROJECT_ID=${GCP_PROJECT_ID:-"media-circle"}
REGION="us-central1"
VPC_CONNECTOR="mc-vpc-connector"

# Optional: GCS bucket for ETL reads
GCS_BUCKET=${GCS_BUCKET:-""}

# Secret bundle for ETL env (created by setup_gcp_secrets.sh)
ETL_SECRET_NAME=${ETL_SECRET_NAME:-"redis-search-${ENVIRONMENT}-etl-env"}
# Secret bundle for API env
API_SECRET_NAME=${API_SECRET_NAME:-"redis-search-${ENVIRONMENT}-api-env"}

# Service account (Cloud Run default compute SA unless overridden)
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
SERVICE_ACCOUNT=${SERVICE_ACCOUNT:-"${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"}

echo "Service Account: ${SERVICE_ACCOUNT}"
echo "ETL Secret:      ${ETL_SECRET_NAME}"
echo "API Secret:      ${API_SECRET_NAME}"

# Ensure service account has access to the appropriate secret
grant_secret_access() {
    local secret_name=$1
    local sa=$2
    
    # Check if SA already has access
    if gcloud secrets get-iam-policy "${secret_name}" --project="${PROJECT_ID}" 2>/dev/null | grep -q "${sa}"; then
        echo "   ✅ ${sa} already has access to ${secret_name}"
    else
        echo "   🔐 Granting secret access to ${sa} for ${secret_name}..."
        gcloud secrets add-iam-policy-binding "${secret_name}" \
            --member="serviceAccount:${sa}" \
            --role="roles/secretmanager.secretAccessor" \
            --project="${PROJECT_ID}" --quiet || {
            echo "   ⚠️  Failed to grant secret access. You may need to do this manually."
        }
    fi
}

if [ "$SERVICE_TYPE" = "api" ]; then
    grant_secret_access "${API_SECRET_NAME}" "${SERVICE_ACCOUNT}"
elif [ "$SERVICE_TYPE" = "etl" ]; then
    grant_secret_access "${ETL_SECRET_NAME}" "${SERVICE_ACCOUNT}"
fi

if [ -n "$GCS_BUCKET" ]; then
    echo "GCS Bucket:      ${GCS_BUCKET}"
    echo "Granting objectViewer on gs://${GCS_BUCKET} to ${SERVICE_ACCOUNT}..."
    gsutil iam ch serviceAccount:${SERVICE_ACCOUNT}:objectViewer gs://${GCS_BUCKET} || {
        echo "⚠️  Failed to grant access to bucket ${GCS_BUCKET}. Please check permissions."
    }
else
    echo "GCS Bucket:      (not set; skipping bucket IAM grant)"
fi

if [ "$ENVIRONMENT" = "prod" ]; then
    API_SERVICE="redis-search-api"
    ETL_SERVICE="redis-search-etl"
else
    API_SERVICE="redis-search-api-${ENVIRONMENT}"
    ETL_SERVICE="redis-search-etl-${ENVIRONMENT}"
fi

API_IMAGE="gcr.io/${PROJECT_ID}/${API_SERVICE}"
ETL_IMAGE="gcr.io/${PROJECT_ID}/${ETL_SERVICE}"

echo "🚀 Deploying ${SERVICE_TYPE} to Cloud Run (${ENVIRONMENT})"
echo "   Project: ${PROJECT_ID}"
echo "   Region:  ${REGION}"

# Check gcloud auth
if ! gcloud auth print-identity-token &>/dev/null 2>&1; then
    echo "❌ Not authenticated. Run: gcloud auth login"
    exit 1
fi

gcloud config set project "$PROJECT_ID" 2>/dev/null

case "$SERVICE_TYPE" in
    api)
        # Redis connection info is in the secret bundle (API_ENV)
        echo "📦 Building ${API_IMAGE}..."
        gcloud builds submit \
            --config=cloudbuild.yaml \
            --substitutions=_IMAGE="${API_IMAGE}",_DOCKERFILE="src/search_api/Dockerfile" \
            .

        echo "🚀 Deploying ${API_SERVICE}..."
        gcloud run deploy "${API_SERVICE}" \
            --image "${API_IMAGE}" \
            --region "${REGION}" \
            --platform managed \
            --vpc-connector "${VPC_CONNECTOR}" \
            --vpc-egress all-traffic \
            --set-secrets "API_ENV=${API_SECRET_NAME}:latest" \
            --allow-unauthenticated \
            --memory 512Mi \
            --min-instances 0 \
            --max-instances 10

        URL=$(gcloud run services describe "${API_SERVICE}" --region="${REGION}" --format="value(status.url)")
        echo ""
        echo "✅ Deployed: ${URL}"
        echo "   Health:      ${URL}/health"
        echo "   Autocomplete: ${URL}/autocomplete?q=star"
        ;;

    etl)
        echo "📦 Building ${ETL_IMAGE}..."
        gcloud builds submit \
            --config=cloudbuild.yaml \
            --substitutions=_IMAGE="${ETL_IMAGE}",_DOCKERFILE="src/etl/Dockerfile" \
            .

        echo "🚀 Creating Cloud Run Job ${ETL_SERVICE}..."
        gcloud run jobs deploy "${ETL_SERVICE}" \
            --image "${ETL_IMAGE}" \
            --region "${REGION}" \
            --vpc-connector "${VPC_CONNECTOR}" \
            --vpc-egress all-traffic \
            --set-secrets "ETL_ENV=${ETL_SECRET_NAME}:latest" \
            --memory 1Gi \
            --task-timeout 30m

        echo ""
        echo "✅ ETL job created: ${ETL_SERVICE}"
        echo "   Run with: gcloud run jobs execute ${ETL_SERVICE} --region=${REGION}"
        ;;

    *)
        echo "❌ Unknown service: ${SERVICE_TYPE}"
        echo "   Usage: $0 [api|etl] [prod|dev]"
        exit 1
        ;;
esac

