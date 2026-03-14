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

REDIS_PASSWORD="rCrwd3xMFhfoKhUF9by9"
CONTAINER_NAME="redis-stack"

to_bytes() {
    local value
    value=$(echo "$1" | tr '[:upper:]' '[:lower:]')

    case "$value" in
        *gb) echo $(( ${value%gb} * 1024 * 1024 * 1024 )) ;;
        *mb) echo $(( ${value%mb} * 1024 * 1024 )) ;;
        *kb) echo $(( ${value%kb} * 1024 )) ;;
        *b) echo "${value%b}" ;;
        *) echo "$value" ;;
    esac
}

DRY_RUN=false
if [ "$1" = "--dry-run" ]; then
    DRY_RUN=true
fi

if [ "$DRY_RUN" = true ]; then
    NEW_MACHINE_TYPE="e2-highmem-2"
    NEW_MAXMEMORY="12gb"
else
    NEW_MACHINE_TYPE="${1:-e2-highmem-2}"
    NEW_MAXMEMORY="${2:-12gb}"
fi

DESIRED_MAXMEMORY_BYTES=$(to_bytes "$NEW_MAXMEMORY")

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

NEEDS_MACHINE_CHANGE=true
if [ "$CURRENT_MACHINE" = "$NEW_MACHINE_TYPE" ]; then
    NEEDS_MACHINE_CHANGE=false
fi

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

# ─────────────────────────────────────────────────────────────────────────────
# 1. Get pre-upgrade key count (if VM is running)
# ─────────────────────────────────────────────────────────────────────────────
PRE_KEY_COUNT="unknown"
CURRENT_MAXMEMORY_BYTES="unknown"
if [ "$CURRENT_STATUS" = "RUNNING" ]; then
    echo "📊 Getting pre-upgrade key count..."
    PRE_KEY_COUNT=$(gcloud compute ssh "$VM_NAME" --zone="$ZONE" --tunnel-through-iap --command="
        docker exec ${CONTAINER_NAME} redis-cli -a '${REDIS_PASSWORD}' --no-auth-warning DBSIZE 2>/dev/null | awk '{print \$NF}'
    " 2>/dev/null || echo "unknown")
    echo "   Keys before upgrade: ${PRE_KEY_COUNT}"

    CURRENT_MAXMEMORY_BYTES=$(gcloud compute ssh "$VM_NAME" --zone="$ZONE" --tunnel-through-iap --command="
        docker exec ${CONTAINER_NAME} redis-cli -a '${REDIS_PASSWORD}' --no-auth-warning CONFIG GET maxmemory 2>/dev/null | awk 'NR == 2 {print \$1}'
    " 2>/dev/null || echo "unknown")
fi

NEEDS_CONTAINER_REFRESH=true
if [ "$CURRENT_MAXMEMORY_BYTES" = "$DESIRED_MAXMEMORY_BYTES" ]; then
    NEEDS_CONTAINER_REFRESH=false
fi

if [ "$NEEDS_MACHINE_CHANGE" = false ] && [ "$NEEDS_CONTAINER_REFRESH" = false ]; then
    echo "✅ Redis VM is already at the desired machine type and maxmemory."
    echo "   No changes required."
    exit 0
fi

if [ "$DRY_RUN" = true ]; then
    echo "🔍 DRY RUN — no changes will be made."
    echo ""
    echo " Would perform:"
    echo "   1. Snapshot disk '${VM_NAME}'"
    if [ "$NEEDS_MACHINE_CHANGE" = true ]; then
        echo "   2. Stop VM"
        echo "   3. Change machine type: ${CURRENT_MACHINE} → ${NEW_MACHINE_TYPE}"
        echo "   4. Start VM"
    else
        echo "   2. Keep machine type unchanged (${CURRENT_MACHINE})"
        echo "   3. Ensure VM is running"
    fi
    echo "   5. Update startup-script maxmemory → ${NEW_MAXMEMORY}"
    if [ "$NEEDS_CONTAINER_REFRESH" = true ]; then
        echo "   6. Recreate Redis container with --maxmemory ${NEW_MAXMEMORY}"
    else
        echo "   6. Keep existing Redis container config (already at desired maxmemory)"
    fi
    echo "   7. Verify Redis health + key count"
    echo ""
    exit 0
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

if [ "$NEEDS_MACHINE_CHANGE" = true ] && [ "$CURRENT_STATUS" = "RUNNING" ]; then
    gcloud compute instances stop "$VM_NAME" --zone="$ZONE" --quiet
    echo "   ✅ VM stopped"
elif [ "$NEEDS_MACHINE_CHANGE" = false ]; then
    echo "   Machine type already matches, no stop required"
else
    echo "   VM is already ${CURRENT_STATUS}"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 4. Change machine type
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "🔧 Changing machine type: ${CURRENT_MACHINE} → ${NEW_MACHINE_TYPE}..."

if [ "$NEEDS_MACHINE_CHANGE" = true ]; then
    gcloud compute instances set-machine-type "$VM_NAME" \
        --zone="$ZONE" \
        --machine-type="$NEW_MACHINE_TYPE"
    echo "   ✅ Machine type updated"
else
    echo "   Machine type already correct, skipping"
fi

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
echo "🚀 Ensuring VM is running..."

if [ "$CURRENT_STATUS" != "RUNNING" ] || [ "$NEEDS_MACHINE_CHANGE" = true ]; then
    gcloud compute instances start "$VM_NAME" --zone="$ZONE" --quiet
else
    echo "   VM already running"
fi

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
if [ "$NEEDS_CONTAINER_REFRESH" = true ]; then
    echo ""
    echo "🔄 Recreating Redis container with maxmemory=${NEW_MAXMEMORY}..."

    gcloud compute ssh "$VM_NAME" --zone="$ZONE" --tunnel-through-iap --command="
        set -e

        echo 'Waiting for Docker daemon...'
        docker_ready=false
        for i in \$(seq 1 12); do
            if docker info >/dev/null 2>&1; then
                docker_ready=true
                break
            fi
            sleep 5
        done

        if [ \"\$docker_ready\" != true ]; then
            echo 'ERROR: Docker daemon never became ready'
            exit 1
        fi

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

        echo 'Waiting for Redis to load data (up to 5 min for large AOF)...'
        redis_ready=false
        for i in \$(seq 1 60); do
            if docker exec ${CONTAINER_NAME} redis-cli -a '${REDIS_PASSWORD}' --no-auth-warning ping 2>/dev/null | grep -q PONG; then
                redis_ready=true
                echo 'Redis is responding'
                break
            fi
            sleep 5
        done

        if [ \"\$redis_ready\" != true ]; then
            echo 'ERROR: Redis did not become ready within timeout'
            echo 'Container status:'
            docker ps -a --filter 'name=^/${CONTAINER_NAME}$'
            echo ''
            echo 'Recent Redis logs:'
            docker logs --tail 50 ${CONTAINER_NAME} 2>&1 || true
            exit 1
        fi
    "

    echo "   ✅ Container recreated"
else
    echo ""
    echo "ℹ️  Redis container already has maxmemory=${NEW_MAXMEMORY}; skipping recreate."
fi

# ─────────────────────────────────────────────────────────────────────────────
# 8. Verify
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "🔍 Verifying upgrade..."

VERIFY_OUTPUT=$(gcloud compute ssh "$VM_NAME" --zone="$ZONE" --tunnel-through-iap --command="
    docker exec ${CONTAINER_NAME} redis-cli -a '${REDIS_PASSWORD}' --no-auth-warning INFO memory 2>/dev/null | grep -E 'used_memory_human|maxmemory_human|total_system_memory_human'
    echo '---'
    echo post_key_count=\$(docker exec ${CONTAINER_NAME} redis-cli -a '${REDIS_PASSWORD}' --no-auth-warning DBSIZE 2>/dev/null)
" 2>/dev/null)

echo "$VERIFY_OUTPUT"

POST_KEY_COUNT=$(echo "$VERIFY_OUTPUT" | awk -F= '/^post_key_count=/{print $2}')

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
