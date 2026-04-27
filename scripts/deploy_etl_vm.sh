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
# 0. Ensure ETL VM is running (it's usually terminated to save costs)
# -----------------------------------------------------------------------------
echo "🔍 Checking ETL VM status..."

VM_STATUS=$(gcloud compute instances describe "${VM_NAME}" \
    --zone="${ZONE}" \
    --format="value(status)" 2>/dev/null || echo "NOT_FOUND")

if [ "$VM_STATUS" = "TERMINATED" ] || [ "$VM_STATUS" = "STOPPED" ]; then
    echo "   VM is ${VM_STATUS}, starting it..."
    gcloud compute instances start "${VM_NAME}" --zone="${ZONE}" --quiet
    
    echo "   Waiting for VM to boot and SSH to become available..."
    # Wait up to 60 seconds for SSH to be ready
    for i in {1..12}; do
        if gcloud compute ssh "${VM_NAME}" --zone="${ZONE}" --tunnel-through-iap \
            --command="echo 'SSH ready'" 2>/dev/null; then
            echo "   ✅ VM is running and SSH is ready"
            break
        fi
        if [ $i -eq 12 ]; then
            echo "   ❌ Timeout waiting for SSH. Try again in a minute."
            exit 1
        fi
        echo "   Waiting... ($i/12)"
        sleep 5
    done
elif [ "$VM_STATUS" = "RUNNING" ]; then
    echo "   ✅ VM is already running"
elif [ "$VM_STATUS" = "NOT_FOUND" ]; then
    echo "   ❌ VM '${VM_NAME}' not found. Run scripts/create_etl_vm.sh first."
    exit 1
else
    echo "   ⚠️  VM status: ${VM_STATUS}. Attempting to continue..."
fi

echo ""

# -----------------------------------------------------------------------------
# 1. Build Docker Image (for linux/amd64 - VM architecture)
# -----------------------------------------------------------------------------
echo "📦 Building ETL Docker image for linux/amd64..."

DOCKER_BUILD_EXTRA_ARGS=""
if [ "${DOCKER_NO_CACHE:-0}" = "1" ]; then
    DOCKER_BUILD_EXTRA_ARGS="--no-cache"
    echo "   ⚠️  Cache disabled (DOCKER_NO_CACHE=1)"
fi

docker build \
    --platform linux/amd64 \
    ${DOCKER_BUILD_EXTRA_ARGS} \
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
    
    # Persistent log directory (survives container replacements)
    mkdir -p /var/log/etl
    
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
        --network host \\
        -v /var/log/etl:/var/log/etl \\
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
        -e MEDIA_MANAGER_API_URL=\${MEDIA_MANAGER_API_URL} \\
        -e MEDIA_MANAGER_INTERNAL_TOKEN=\${MEDIA_MANAGER_INTERNAL_TOKEN} \\
        -e PODCASTINDEX_API_KEY=\${PODCASTINDEX_API_KEY} \\
        -e PODCASTINDEX_API_SECRET=\${PODCASTINDEX_API_SECRET} \\
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

