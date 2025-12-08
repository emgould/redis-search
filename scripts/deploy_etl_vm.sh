#!/bin/bash
# Deploy ETL service to ETL VM
#
# This script:
#   1. Builds the ETL Docker image
#   2. Pushes to Google Container Registry
#   3. SSHs to VM and deploys
#
# Prerequisites:
#   - gcloud CLI authenticated
#   - Docker running locally
#   - ETL VM already created (scripts/create_etl_vm.sh)
#
# Usage:
#   ./scripts/deploy_vm.sh

set -e

PROJECT_ID=${GCP_PROJECT_ID:-"media-circle"}
REGION="us-central1"
ZONE="${REGION}-a"
VM_NAME="etl-runner-vm"
REDIS_HOST="10.128.0.2"  # Redis Stack VM internal IP

IMAGE_NAME="gcr.io/${PROJECT_ID}/redis-search-etl"
IMAGE_TAG="latest"

echo "=============================================="
echo " Deploying ETL Service to VM"
echo "=============================================="
echo " Project:  ${PROJECT_ID}"
echo " VM:       ${VM_NAME}"
echo " Image:    ${IMAGE_NAME}:${IMAGE_TAG}"
echo "=============================================="
echo ""

# Ensure gcloud is configured
gcloud config set project "$PROJECT_ID" 2>/dev/null

# -----------------------------------------------------------------------------
# 1. Build Docker Image (for linux/amd64 - VM architecture)
# -----------------------------------------------------------------------------
echo "📦 Building ETL Docker image for linux/amd64..."

docker build \
    --platform linux/amd64 \
    -f docker/etl.Dockerfile \
    -t "${IMAGE_NAME}:${IMAGE_TAG}" \
    .

echo "   ✅ Image built"

# -----------------------------------------------------------------------------
# 2. Push to GCR
# -----------------------------------------------------------------------------
echo ""
echo "🚀 Pushing image to GCR..."

# Configure docker for GCR
gcloud auth configure-docker gcr.io --quiet 2>/dev/null

docker push "${IMAGE_NAME}:${IMAGE_TAG}"

echo "   ✅ Image pushed"

# -----------------------------------------------------------------------------
# 3. Copy docker-compose and env files to VM
# -----------------------------------------------------------------------------
echo ""
echo "📄 Copying configuration to VM..."

# Create deployment directory on VM
gcloud compute ssh "${VM_NAME}" --zone="${ZONE}" --tunnel-through-iap --command="
    mkdir -p /home/\$(whoami)/etl-deploy
"

# Copy docker-compose file
gcloud compute scp docker/vm-compose.yml "${VM_NAME}:~/etl-deploy/docker-compose.yml" \
    --zone="${ZONE}" --tunnel-through-iap

# Copy environment file (secrets)
gcloud compute scp config/etl.dev.env "${VM_NAME}:~/etl-deploy/.env" \
    --zone="${ZONE}" --tunnel-through-iap

echo "   ✅ Configuration copied"

# -----------------------------------------------------------------------------
# 4. Deploy on VM
# -----------------------------------------------------------------------------
echo ""
echo "🔄 Deploying on VM..."

gcloud compute ssh "${VM_NAME}" --zone="${ZONE}" --tunnel-through-iap --command="
    cd ~/etl-deploy
    
    # Authenticate with GCR
    gcloud auth configure-docker gcr.io --quiet 2>/dev/null || true
    
    # Pull the ETL image
    echo 'Pulling ETL image...'
    docker pull ${IMAGE_NAME}:${IMAGE_TAG}
    
    # Stop and remove existing ETL container if it exists
    docker stop etl-runner 2>/dev/null || true
    docker rm etl-runner 2>/dev/null || true
    
    # Load environment variables
    set -a
    source .env
    set +a
    
    # Start ETL container with cron
    # Note: Connects to Redis Stack VM at ${REDIS_HOST}:6379
    echo 'Starting ETL container...'
    docker run -d \\
        --name etl-runner \\
        --restart always \\
        -e REDIS_HOST=${REDIS_HOST} \\
        -e REDIS_PORT=6379 \\
        -e REDIS_PASSWORD=\${REDIS_PASSWORD} \\
        -e TMDB_READ_TOKEN=\${TMDB_READ_TOKEN} \\
        -e TMDB_API_KEY=\${TMDB_API_KEY} \\
        -e GCS_BUCKET=\${GCS_BUCKET} \\
        -e GCS_ETL_PREFIX=\${GCS_ETL_PREFIX} \\
        -e ETL_CONFIG_PATH=/app/config/etl_jobs.yaml \\
        -e ETL_NOTIFICATION_EMAIL=\${ETL_NOTIFICATION_EMAIL} \\
        -e SENDGRID_SERVER=\${SENDGRID_SERVER} \\
        -e SENDGRID_PORT=\${SENDGRID_PORT} \\
        -e SENDGRID_USERNAME=\${SENDGRID_USERNAME} \\
        -e SENDGRID_PASSWORD=\${SENDGRID_PASSWORD} \\
        -e SENDGRID_FROM_EMAIL=\${SENDGRID_FROM_EMAIL} \\
        ${IMAGE_NAME}:${IMAGE_TAG} \\
        cron
    
    # Show status
    echo ''
    echo 'Container status:'
    docker ps --format 'table {{.Names}}\t{{.Status}}'
"

echo ""
echo "=============================================="
echo " ✅ Deployment Complete"
echo "=============================================="
echo ""
echo " ETL VM:   ${VM_NAME} (${REDIS_HOST} for Redis)"
echo " Redis VM: redis-stack-vm (10.128.0.2)"
echo ""
echo " View ETL logs:"
echo "   gcloud compute ssh ${VM_NAME} --zone=${ZONE} --tunnel-through-iap -- docker logs -f etl-runner"
echo ""
echo " Run ETL manually:"
echo "   gcloud compute ssh ${VM_NAME} --zone=${ZONE} --tunnel-through-iap -- docker exec etl-runner python -m etl.run_nightly_etl"
echo ""
echo " SSH into ETL VM:"
echo "   gcloud compute ssh ${VM_NAME} --zone=${ZONE} --tunnel-through-iap"
echo ""

