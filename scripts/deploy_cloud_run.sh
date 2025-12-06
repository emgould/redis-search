#!/bin/bash
# Deploy Redis Search API to Google Cloud Run
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - VPC connector 'mc-vpc-connector' exists in us-central1
#   - Redis Stack VM created (run scripts/create_redis_vm.sh first)
#   - REDIS_HOST set to the VM's internal IP
#
# Usage:
#   REDIS_HOST=10.x.x.x ./scripts/deploy_cloud_run.sh [api|etl] [prod|dev]
#
# Or set REDIS_HOST in your environment before running.

set -e

SERVICE_TYPE=${1:-api}
ENVIRONMENT=${2:-prod}

PROJECT_ID=${GCP_PROJECT_ID:-"media-circle"}
REGION="us-central1"
VPC_CONNECTOR="mc-vpc-connector"

# Redis Stack VM - get from create_redis_vm.sh output
REDIS_HOST=${REDIS_HOST:-""}
REDIS_PORT=${REDIS_PORT:-"6379"}
REDIS_PASSWORD=${REDIS_PASSWORD:-""}

# Optional: GCS bucket for ETL reads
GCS_BUCKET=${GCS_BUCKET:-""}

# Service account (Cloud Run default compute SA unless overridden)
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
SERVICE_ACCOUNT=${SERVICE_ACCOUNT:-"${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"}

if [ -z "$REDIS_HOST" ] || [ -z "$REDIS_PASSWORD" ]; then
    echo "❌ REDIS_HOST and REDIS_PASSWORD must be set."
    echo ""
    echo "   First, create the Redis Stack VM:"
    echo "     ./scripts/create_redis_vm.sh"
    echo ""
    echo "   Then deploy with the VM's internal IP and password:"
    echo "     REDIS_HOST=10.x.x.x REDIS_PASSWORD=xxx ./scripts/deploy_cloud_run.sh ${SERVICE_TYPE} ${ENVIRONMENT}"
    echo ""
    exit 1
fi

echo "Service Account: ${SERVICE_ACCOUNT}"
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
        echo "📦 Building ${API_IMAGE}..."
        gcloud builds submit --tag "${API_IMAGE}" .

        echo "🚀 Deploying ${API_SERVICE}..."
        gcloud run deploy "${API_SERVICE}" \
            --image "${API_IMAGE}" \
            --region "${REGION}" \
            --platform managed \
            --vpc-connector "${VPC_CONNECTOR}" \
            --vpc-egress all-traffic \
            --set-env-vars "REDIS_HOST=${REDIS_HOST},REDIS_PORT=${REDIS_PORT},REDIS_PASSWORD=${REDIS_PASSWORD},GCS_BUCKET=${GCS_BUCKET}" \
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
        gcloud builds submit --tag "${ETL_IMAGE}" .

        echo "🚀 Creating Cloud Run Job ${ETL_SERVICE}..."
        gcloud run jobs deploy "${ETL_SERVICE}" \
            --image "${ETL_IMAGE}" \
            --region "${REGION}" \
            --vpc-connector "${VPC_CONNECTOR}" \
            --vpc-egress all-traffic \
            --set-env-vars "REDIS_HOST=${REDIS_HOST},REDIS_PORT=${REDIS_PORT},REDIS_PASSWORD=${REDIS_PASSWORD},GCS_BUCKET=${GCS_BUCKET}" \
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

