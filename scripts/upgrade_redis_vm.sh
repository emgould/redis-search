#!/bin/bash
# Upgrade Redis Stack VM machine type (in-place resize)
#
# Performs a safe in-place upgrade of the Redis VM:
#   1. Verifies current VM state and takes a safety snapshot
#   2. Stops the VM
#   3. Changes the machine type
#   4. Updates the startup-script metadata with new maxmemory
#   5. Starts the VM
#   6. Recreates the Redis container with updated REDIS_ARGS
#   7. Verifies Redis is healthy and data is intact
#
# Data safety:
#   - The persistent SSD boot disk is never detached or deleted
#   - A pre-upgrade snapshot is taken before any changes
#   - Redis AOF + RDB persistence ensures data survives container recreation
#   - The script verifies key count after upgrade
#
# Usage:
#   ./scripts/upgrade_redis_vm.sh                          # default: e2-highmem-2, 12gb maxmemory
#   ./scripts/upgrade_redis_vm.sh e2-standard-4 14gb       # custom machine type + maxmemory
#   ./scripts/upgrade_redis_vm.sh --dry-run                # show what would happen

set -e

PROJECT_ID=${GCP_PROJECT_ID:-"media-circle"}
REGION="us-central1"
ZONE="${REGION}-a"
VM_NAME="redis-stack-vm"

NEW_MACHINE_TYPE="${1:-e2-highmem-2}"
NEW_MAXMEMORY="${2:-12gb}"

REDIS_PASSWORD="rCrwd3xMFhfoKhUF9by9"
CONTAINER_NAME="redis-stack"

DRY_RUN=false
if [ "$1" = "--dry-run" ]; then
    DRY_RUN=true
fi

# ─────────────────────────────────────────────────────────────────────────────
# 0. Gather current state
# ─────────────────────────────────────────────────────────────────────────────
echo "=============================================="
echo " Redis VM Upgrade"
echo "=============================================="

gcloud config set project "$PROJECT_ID" 2>/dev/null

CURRENT_MACHINE=$(gcloud compute instances describe "$VM_NAME" \
    --zone="$ZONE" --format="value(machineType)" | awk -F/ '{print $NF}')
CURRENT_STATUS=$(gcloud compute instances describe "$VM_NAME" \
    --zone="$ZONE" --format="value(status)")
CURRENT_DISK_SIZE=$(gcloud compute disks describe "$VM_NAME" \
    --zone="$ZONE" --format="value(sizeGb)")

echo ""
echo " Current state:"
echo "   Machine type:  ${CURRENT_MACHINE}"
echo "   Status:        ${CURRENT_STATUS}"
echo "   Disk size:     ${CURRENT_DISK_SIZE} GB"
echo ""
echo " Target state:"
echo "   Machine type:  ${NEW_MACHINE_TYPE}"
echo "   Max memory:    ${NEW_MAXMEMORY}"
echo ""

if [ "$CURRENT_MACHINE" = "$NEW_MACHINE_TYPE" ]; then
    echo "⚠️  VM is already ${NEW_MACHINE_TYPE}."
    echo "   If you only need to update maxmemory, SSH in and recreate the container."
    exit 0
fi

if [ "$DRY_RUN" = true ]; then
    echo "🔍 DRY RUN — no changes will be made."
    echo ""
    echo " Would perform:"
    echo "   1. Snapshot disk '${VM_NAME}'"
    echo "   2. Stop VM"
    echo "   3. Change machine type: ${CURRENT_MACHINE} → ${NEW_MACHINE_TYPE}"
    echo "   4. Update startup-script maxmemory → ${NEW_MAXMEMORY}"
    echo "   5. Start VM"
    echo "   6. Recreate Redis container with --maxmemory ${NEW_MAXMEMORY}"
    echo "   7. Verify Redis health + key count"
    echo ""
    exit 0
fi

# ─────────────────────────────────────────────────────────────────────────────
# 1. Get pre-upgrade key count (if VM is running)
# ─────────────────────────────────────────────────────────────────────────────
PRE_KEY_COUNT="unknown"
if [ "$CURRENT_STATUS" = "RUNNING" ]; then
    echo "📊 Getting pre-upgrade key count..."
    PRE_KEY_COUNT=$(gcloud compute ssh "$VM_NAME" --zone="$ZONE" --tunnel-through-iap --command="
        docker exec ${CONTAINER_NAME} redis-cli -a '${REDIS_PASSWORD}' --no-auth-warning DBSIZE 2>/dev/null | awk '{print \$NF}'
    " 2>/dev/null || echo "unknown")
    echo "   Keys before upgrade: ${PRE_KEY_COUNT}"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 2. Take a safety snapshot
# ─────────────────────────────────────────────────────────────────────────────
echo ""
SNAPSHOT_NAME="redis-pre-upgrade-$(date +%Y%m%d-%H%M%S)"
echo "📸 Creating safety snapshot: ${SNAPSHOT_NAME}..."

gcloud compute disks snapshot "$VM_NAME" \
    --zone="$ZONE" \
    --snapshot-names="$SNAPSHOT_NAME" \
    --description="Pre-upgrade snapshot before ${CURRENT_MACHINE} → ${NEW_MACHINE_TYPE}"

echo "   ✅ Snapshot created"

# ─────────────────────────────────────────────────────────────────────────────
# 3. Stop the VM
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "🛑 Stopping VM..."

if [ "$CURRENT_STATUS" = "RUNNING" ]; then
    gcloud compute instances stop "$VM_NAME" --zone="$ZONE" --quiet
    echo "   ✅ VM stopped"
else
    echo "   VM is already ${CURRENT_STATUS}"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 4. Change machine type
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "🔧 Changing machine type: ${CURRENT_MACHINE} → ${NEW_MACHINE_TYPE}..."

gcloud compute instances set-machine-type "$VM_NAME" \
    --zone="$ZONE" \
    --machine-type="$NEW_MACHINE_TYPE"

echo "   ✅ Machine type updated"

# ─────────────────────────────────────────────────────────────────────────────
# 5. Update startup-script metadata with new maxmemory
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "📝 Updating startup-script with maxmemory=${NEW_MAXMEMORY}..."

STARTUP_FILE=$(mktemp)
cat > "${STARTUP_FILE}" <<EOF
#!/bin/bash
# Redis Stack startup script (Container-Optimized OS)
CONTAINER_NAME=redis-stack
REDIS_PASSWORD='${REDIS_PASSWORD}'

if docker ps -a --format '{{.Names}}' | grep -q "^\${CONTAINER_NAME}\$"; then
    echo 'Redis container exists, starting...'
    docker start \${CONTAINER_NAME}
else
    echo 'Creating Redis container...'
    mkdir -p /var/lib/redis-data
    docker run -d \\
        --name \${CONTAINER_NAME} \\
        --restart always \\
        -p 6379:6379 \\
        -v /var/lib/redis-data:/data \\
        -e REDIS_ARGS="--requirepass \${REDIS_PASSWORD} --appendonly yes --save 60 1 --maxmemory ${NEW_MAXMEMORY} --maxmemory-policy volatile-lru" \\
        redis/redis-stack-server:7.4.0-v8
fi
EOF

gcloud compute instances add-metadata "$VM_NAME" \
    --zone="$ZONE" \
    --metadata-from-file startup-script="${STARTUP_FILE}"

rm -f "${STARTUP_FILE}"
echo "   ✅ Startup script updated"

# ─────────────────────────────────────────────────────────────────────────────
# 6. Start the VM
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "🚀 Starting VM..."

gcloud compute instances start "$VM_NAME" --zone="$ZONE" --quiet

# Wait for VM to be RUNNING
for i in {1..18}; do
    STATUS=$(gcloud compute instances describe "$VM_NAME" --zone="$ZONE" --format="value(status)")
    echo "   Status: ${STATUS}"
    if [ "$STATUS" = "RUNNING" ]; then
        break
    fi
    sleep 5
done

# Wait for SSH to be ready
echo "   Waiting for SSH..."
for i in {1..12}; do
    if gcloud compute ssh "$VM_NAME" --zone="$ZONE" --tunnel-through-iap \
        --command="echo 'SSH ready'" 2>/dev/null; then
        break
    fi
    if [ $i -eq 12 ]; then
        echo "   ❌ Timeout waiting for SSH."
        exit 1
    fi
    sleep 5
done

echo "   ✅ VM is running"

# ─────────────────────────────────────────────────────────────────────────────
# 7. Recreate the Redis container with new REDIS_ARGS
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "🔄 Recreating Redis container with maxmemory=${NEW_MAXMEMORY}..."

gcloud compute ssh "$VM_NAME" --zone="$ZONE" --tunnel-through-iap --command="
    echo 'Waiting for Docker daemon...'
    for i in \$(seq 1 12); do
        docker info >/dev/null 2>&1 && break
        sleep 5
    done

    echo 'Stopping old container...'
    docker stop ${CONTAINER_NAME} 2>/dev/null || true

    echo 'Removing old container (data volume preserved)...'
    docker rm ${CONTAINER_NAME} 2>/dev/null || true

    echo 'Starting new container with maxmemory=${NEW_MAXMEMORY}...'
    docker run -d \
        --name ${CONTAINER_NAME} \
        --restart always \
        -p 6379:6379 \
        -v /var/lib/redis-data:/data \
        -e REDIS_ARGS='--requirepass ${REDIS_PASSWORD} --appendonly yes --save 60 1 --maxmemory ${NEW_MAXMEMORY} --maxmemory-policy volatile-lru' \
        redis/redis-stack-server:7.4.0-v8

    echo 'Waiting for Redis to load data...'
    for i in \$(seq 1 30); do
        if docker exec ${CONTAINER_NAME} redis-cli -a '${REDIS_PASSWORD}' --no-auth-warning ping 2>/dev/null | grep -q PONG; then
            echo 'Redis is responding'
            break
        fi
        sleep 5
    done
"

echo "   ✅ Container recreated"

# ─────────────────────────────────────────────────────────────────────────────
# 8. Verify
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "🔍 Verifying upgrade..."

VERIFY_OUTPUT=$(gcloud compute ssh "$VM_NAME" --zone="$ZONE" --tunnel-through-iap --command="
    docker exec ${CONTAINER_NAME} redis-cli -a '${REDIS_PASSWORD}' --no-auth-warning INFO memory 2>/dev/null | grep -E 'used_memory_human|maxmemory_human|total_system_memory_human'
    echo '---'
    docker exec ${CONTAINER_NAME} redis-cli -a '${REDIS_PASSWORD}' --no-auth-warning DBSIZE 2>/dev/null
" 2>/dev/null)

echo "$VERIFY_OUTPUT"

POST_KEY_COUNT=$(echo "$VERIFY_OUTPUT" | grep -oP '\d+' | tail -1)

VERIFIED_MACHINE=$(gcloud compute instances describe "$VM_NAME" \
    --zone="$ZONE" --format="value(machineType)" | awk -F/ '{print $NF}')

echo ""
echo "=============================================="
echo " ✅ Redis VM Upgrade Complete"
echo "=============================================="
echo ""
echo " Machine type:    ${VERIFIED_MACHINE}"
echo " Max memory:      ${NEW_MAXMEMORY}"
echo " Keys before:     ${PRE_KEY_COUNT}"
echo " Keys after:      ${POST_KEY_COUNT}"
echo " Safety snapshot: ${SNAPSHOT_NAME}"
echo ""

if [ "$PRE_KEY_COUNT" != "unknown" ] && [ "$POST_KEY_COUNT" != "$PRE_KEY_COUNT" ]; then
    echo " ⚠️  Key count mismatch! Before=${PRE_KEY_COUNT} After=${POST_KEY_COUNT}"
    echo "    This may be normal if keys expired during the upgrade window."
    echo "    If significantly different, restore from snapshot: ${SNAPSHOT_NAME}"
fi

echo ""
echo " To clean up the safety snapshot later:"
echo "   gcloud compute snapshots delete ${SNAPSHOT_NAME}"
echo ""
