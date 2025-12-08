#!/bin/bash
# Create ETL VM on GCE
#
# Creates:
#   - n2-standard-2 VM (2 vCPU, 8GB RAM) with Ubuntu 22.04 LTS
#   - 20GB persistent SSD
#   - Internal-only access (no external IP)
#   - Docker and docker-compose pre-installed
#   - Cron configured for ETL
#
# Usage:
#   ./scripts/create_etl_vm.sh

set -e

PROJECT_ID=${GCP_PROJECT_ID:-"media-circle"}
REGION="us-central1"
ZONE="${REGION}-a"
NETWORK="default"

VM_NAME="etl-runner-vm"
MACHINE_TYPE="n2-standard-2"
DISK_SIZE="20GB"
DISK_TYPE="pd-ssd"

echo "=============================================="
echo " ETL VM Setup"
echo "=============================================="
echo " Project:  ${PROJECT_ID}"
echo " Zone:     ${ZONE}"
echo " VM:       ${VM_NAME}"
echo " Machine:  ${MACHINE_TYPE}"
echo " Disk:     ${DISK_SIZE} ${DISK_TYPE}"
echo " OS:       Ubuntu 22.04 LTS"
echo "=============================================="
echo ""

# Set project
gcloud config set project "$PROJECT_ID"

# -----------------------------------------------------------------------------
# 1. Create the VM
# -----------------------------------------------------------------------------
echo "🖥️  Creating VM..."

if gcloud compute instances describe "$VM_NAME" --zone="$ZONE" &>/dev/null; then
    echo "   VM already exists: ${VM_NAME}"
else
    # Create startup script file
    STARTUP_FILE=$(mktemp)
    cat > "${STARTUP_FILE}" <<'STARTUP_EOF'
#!/bin/bash
set -e

# Install Docker
apt-get update
apt-get install -y ca-certificates curl gnupg

install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Enable Docker for all users
for user in $(ls /home); do
    usermod -aG docker "$user" 2>/dev/null || true
done

# Create ETL directory
mkdir -p /opt/etl
chmod 777 /opt/etl

echo "Docker installation complete" > /var/log/startup-complete.txt
STARTUP_EOF

    gcloud compute instances create "$VM_NAME" \
        --zone="$ZONE" \
        --machine-type="$MACHINE_TYPE" \
        --network="$NETWORK" \
        --no-address \
        --boot-disk-size="$DISK_SIZE" \
        --boot-disk-type="$DISK_TYPE" \
        --boot-disk-auto-delete \
        --image-family=ubuntu-2204-lts \
        --image-project=ubuntu-os-cloud \
        --scopes=cloud-platform \
        --metadata-from-file startup-script="${STARTUP_FILE}"

    rm -f "${STARTUP_FILE}"
    echo "   ✅ Created VM: ${VM_NAME}"
fi

# -----------------------------------------------------------------------------
# 2. Wait for VM to start
# -----------------------------------------------------------------------------
echo ""
echo "⏳ Waiting for VM to start..."

for i in {1..30}; do
    STATUS=$(gcloud compute instances describe "$VM_NAME" --zone="$ZONE" --format="value(status)")
    echo "   Status: ${STATUS}"
    if [ "$STATUS" = "RUNNING" ]; then
        break
    fi
    sleep 5
done

# Wait for startup script to complete
echo ""
echo "⏳ Waiting for startup script to complete (Docker install)..."
for i in {1..24}; do
    if gcloud compute ssh "$VM_NAME" --zone="$ZONE" --tunnel-through-iap --command="test -f /var/log/startup-complete.txt" 2>/dev/null; then
        echo "   ✅ Startup script complete"
        break
    fi
    echo "   Waiting... ($i/24)"
    sleep 10
done

INTERNAL_IP=$(gcloud compute instances describe "$VM_NAME" \
    --zone="$ZONE" \
    --format="value(networkInterfaces[0].networkIP)")

echo ""
echo "=============================================="
echo " ✅ ETL VM Ready"
echo "=============================================="
echo ""
echo " Internal IP: ${INTERNAL_IP}"
echo ""
echo " To SSH into the VM:"
echo "   gcloud compute ssh ${VM_NAME} --zone=${ZONE} --tunnel-through-iap"
echo ""
echo " To deploy ETL:"
echo "   make deploy-etl-vm"
echo ""
echo " Redis Stack VM IP: 10.128.0.2 (connect via internal network)"
echo ""

