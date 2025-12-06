#!/bin/bash
# Create Redis Stack VM on GCE
#
# Creates:
#   - e2-standard-2 VM (2 vCPU, 8GB RAM)
#   - 50GB persistent SSD
#   - Auto snapshot policy (daily, 7 day retention)
#   - Internal-only access (no external IP)
#   - Firewall rule for Redis port 6379 (internal VPC access)
#   - Firewall rule for IAP tunneling (local dev via `make tunnel`)
#
# Usage:
#   ./scripts/create_redis_vm.sh

set -e

PROJECT_ID=${GCP_PROJECT_ID:-"media-circle"}
REGION="us-central1"
ZONE="${REGION}-a"
NETWORK="default"

VM_NAME="redis-stack-vm"
MACHINE_TYPE="e2-standard-2"
DISK_SIZE="50GB"
DISK_TYPE="pd-ssd"

SNAPSHOT_POLICY="redis-daily-backup"
FIREWALL_RULE="allow-redis-internal"
IAP_FIREWALL_RULE="allow-iap-redis"

# Redis authentication
REDIS_PASSWORD="rCrwd3xMFhfoKhUF9by9"

echo "=============================================="
echo " Redis Stack VM Setup"
echo "=============================================="
echo " Project:  ${PROJECT_ID}"
echo " Zone:     ${ZONE}"
echo " VM:       ${VM_NAME}"
echo " Machine:  ${MACHINE_TYPE}"
echo " Disk:     ${DISK_SIZE} ${DISK_TYPE}"
echo "=============================================="
echo ""

# Set project
gcloud config set project "$PROJECT_ID"

# -----------------------------------------------------------------------------
# 1. Create Snapshot Policy (for automatic backups)
# -----------------------------------------------------------------------------
echo "📸 Creating snapshot policy..."

if gcloud compute resource-policies describe "$SNAPSHOT_POLICY" --region="$REGION" &>/dev/null; then
    echo "   Snapshot policy already exists: ${SNAPSHOT_POLICY}"
else
    gcloud compute resource-policies create snapshot-schedule "$SNAPSHOT_POLICY" \
        --region="$REGION" \
        --max-retention-days=7 \
        --on-source-disk-delete=keep-auto-snapshots \
        --daily-schedule \
        --start-time=04:00
    echo "   ✅ Created snapshot policy: ${SNAPSHOT_POLICY}"
fi

# -----------------------------------------------------------------------------
# 2. Create Firewall Rule (allow Redis from VPC)
# -----------------------------------------------------------------------------
echo ""
echo "🔥 Creating firewall rule..."

if gcloud compute firewall-rules describe "$FIREWALL_RULE" &>/dev/null; then
    echo "   Firewall rule already exists: ${FIREWALL_RULE}"
else
    gcloud compute firewall-rules create "$FIREWALL_RULE" \
        --network="$NETWORK" \
        --allow=tcp:6379 \
        --source-ranges="10.0.0.0/8" \
        --target-tags="redis-server" \
        --description="Allow Redis access from VPC (Cloud Run via connector)"
    echo "   ✅ Created firewall rule: ${FIREWALL_RULE}"
fi

# IAP tunnel firewall rule (for local development via `make tunnel`)
echo ""
echo "🔥 Creating IAP tunnel firewall rule..."

if gcloud compute firewall-rules describe "$IAP_FIREWALL_RULE" &>/dev/null; then
    echo "   IAP firewall rule already exists: ${IAP_FIREWALL_RULE}"
else
    gcloud compute firewall-rules create "$IAP_FIREWALL_RULE" \
        --network="$NETWORK" \
        --allow=tcp:6379 \
        --source-ranges="35.235.240.0/20" \
        --target-tags="redis-server" \
        --description="Allow IAP tunnel to Redis (for local dev via make tunnel)"
    echo "   ✅ Created IAP firewall rule: ${IAP_FIREWALL_RULE}"
fi

# -----------------------------------------------------------------------------
# 3. Create the VM
# -----------------------------------------------------------------------------
echo ""
echo "🖥️  Creating VM..."

if gcloud compute instances describe "$VM_NAME" --zone="$ZONE" &>/dev/null; then
    echo "   VM already exists: ${VM_NAME}"
else
    # Build startup script into a temp file to avoid metadata parsing issues
    STARTUP_FILE=$(mktemp)
    cat > "${STARTUP_FILE}" <<'EOF'
#!/bin/bash
# Redis Stack startup script (Container-Optimized OS)
CONTAINER_NAME=redis-stack
REDIS_PASSWORD='rCrwd3xMFhfoKhUF9by9'

if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo 'Redis container exists, starting...'
    docker start ${CONTAINER_NAME}
else
    echo 'Creating Redis container...'
    mkdir -p /var/lib/redis-data
    # Note: redis-stack-server uses REDIS_ARGS env var for config, not CLI args
    docker run -d \
        --name ${CONTAINER_NAME} \
        --restart always \
        -p 6379:6379 \
        -v /var/lib/redis-data:/data \
        -e REDIS_ARGS="--requirepass ${REDIS_PASSWORD} --appendonly yes --save 60 1" \
        redis/redis-stack-server:latest
fi
EOF

    echo "   Creating VM with startup script ${STARTUP_FILE} ..."
    gcloud compute instances create "$VM_NAME" \
        --zone="$ZONE" \
        --machine-type="$MACHINE_TYPE" \
        --network="$NETWORK" \
        --no-address \
        --tags="redis-server" \
        --boot-disk-size="$DISK_SIZE" \
        --boot-disk-type="$DISK_TYPE" \
        --boot-disk-auto-delete \
        --image-family=cos-stable \
        --image-project=cos-cloud \
        --metadata-from-file startup-script="${STARTUP_FILE}"

    # cleanup temp file
    rm -f "${STARTUP_FILE}"

    echo "   ✅ Created VM: ${VM_NAME}"
fi

# -----------------------------------------------------------------------------
# 4. Attach Snapshot Policy to Boot Disk
# -----------------------------------------------------------------------------
echo ""
echo "📎 Attaching snapshot policy to disk..."

gcloud compute disks add-resource-policies "$VM_NAME" \
    --zone="$ZONE" \
    --resource-policies="$SNAPSHOT_POLICY" 2>/dev/null || echo "   Policy may already be attached"

echo "   ✅ Snapshot policy attached"

# -----------------------------------------------------------------------------
# 5. Wait for VM and get Internal IP
# -----------------------------------------------------------------------------
echo ""
echo "⏳ Waiting for VM to start..."

# Poll status until RUNNING (timeout ~90s)
for i in {1..18}; do
    STATUS=$(gcloud compute instances describe "$VM_NAME" --zone="$ZONE" --format="value(status)")
    echo "   Status: ${STATUS}"
    if [ "$STATUS" = "RUNNING" ]; then
        break
    fi
    sleep 5
done

INTERNAL_IP=$(gcloud compute instances describe "$VM_NAME" \
    --zone="$ZONE" \
    --format="value(networkInterfaces[0].networkIP)")

echo ""
echo "=============================================="
echo " ✅ Redis Stack VM Ready"
echo "=============================================="
echo ""
echo " Internal IP: ${INTERNAL_IP}"
echo " Port:        6379"
echo " Password:    ${REDIS_PASSWORD}"
echo ""
echo " Deploy with:"
echo "   REDIS_HOST=${INTERNAL_IP} REDIS_PASSWORD=${REDIS_PASSWORD} make deploy-api"
echo ""
echo " To SSH into the VM (via IAP tunnel):"
echo "   gcloud compute ssh ${VM_NAME} --zone=${ZONE} --tunnel-through-iap"
echo ""
echo " To check Redis status:"
echo "   gcloud compute ssh ${VM_NAME} --zone=${ZONE} --tunnel-through-iap -- docker logs redis-stack"
echo ""
echo " For local development (access Redis from your machine):"
echo "   make tunnel"
echo "   # Then use PUBLIC_REDIS_HOST=localhost PUBLIC_REDIS_PORT=6381"
echo ""
echo " ⚠️  Note: Redis Stack may take 1-2 minutes to fully start."
echo "    The VM uses Container-Optimized OS and pulls the Docker image on first boot."
echo ""

