#!/bin/bash
# Setup scheduled ETL on GCP
#
# This script:
# 1. Creates an Instance Schedule to start the ETL VM at 2 AM UTC
# 2. Creates an Instance Schedule to stop the ETL VM at 5 AM UTC
# 3. The cron daemon inside the container handles the actual ETL at 3 AM UTC
#
# Timeline:
#   2 AM UTC: VM starts (Instance Schedule)
#   3 AM UTC: ETL runs (Cron inside container)
#   5 AM UTC: VM stops (Instance Schedule)
#
# Benefits:
#   - Manual VM starts don't trigger ETL (cron only fires at 3 AM)
#   - Easy debugging - can start VM anytime without ETL running
#   - Cost savings - VM only runs 3 hours per day
#
# Usage:
#   ./scripts/setup_etl_schedule.sh
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - ETL VM already created and deployed with cron mode

set -e

PROJECT_ID=${GCP_PROJECT_ID:-"media-circle"}
REGION="us-central1"
ZONE="${REGION}-a"
VM_NAME="etl-runner-vm"
SCHEDULE_NAME="etl-daily-schedule"

echo "=============================================="
echo " Setting up Scheduled ETL"
echo "=============================================="
echo " Project:  ${PROJECT_ID}"
echo " VM:       ${VM_NAME}"
echo " Start:    2 AM UTC (Instance Schedule)"
echo " ETL:      3 AM UTC (Cron inside container)"
echo " Stop:     5 AM UTC (Instance Schedule)"
echo "=============================================="
echo ""

# Set project
gcloud config set project "$PROJECT_ID"

# -----------------------------------------------------------------------------
# 1. Create Resource Policy for Instance Schedule
# -----------------------------------------------------------------------------
echo "📅 Creating instance schedule..."

# Check if schedule already exists
if gcloud compute resource-policies describe "$SCHEDULE_NAME" --region="$REGION" &>/dev/null; then
    echo "   Schedule already exists, deleting to recreate..."
    
    # First remove from VM if attached
    gcloud compute instances remove-resource-policies "$VM_NAME" \
        --zone="$ZONE" \
        --resource-policies="$SCHEDULE_NAME" 2>/dev/null || true
    
    # Then delete the policy
    gcloud compute resource-policies delete "$SCHEDULE_NAME" --region="$REGION" --quiet
fi

# Create schedule:
#   - Start VM at 2 AM UTC (1 hour before cron)
#   - Stop VM at 5 AM UTC (2 hours after cron, plenty of buffer)
gcloud compute resource-policies create instance-schedule "$SCHEDULE_NAME" \
    --region="$REGION" \
    --vm-start-schedule="0 2 * * *" \
    --vm-stop-schedule="0 5 * * *" \
    --timezone="UTC" \
    --description="ETL schedule: Start 2AM, Stop 5AM UTC"

echo "   ✅ Schedule created"
echo "      Start: 2 AM UTC"
echo "      Stop:  5 AM UTC"

# -----------------------------------------------------------------------------
# 2. Attach schedule to VM
# -----------------------------------------------------------------------------
echo ""
echo "🔗 Attaching schedule to VM..."

gcloud compute instances add-resource-policies "$VM_NAME" \
    --zone="$ZONE" \
    --resource-policies="$SCHEDULE_NAME"

echo "   ✅ Schedule attached to VM"

# -----------------------------------------------------------------------------
# 3. Verify cron is running in the container
# -----------------------------------------------------------------------------
echo ""
echo "🔍 Verifying ETL container cron setup..."

# Check if VM is running
VM_STATUS=$(gcloud compute instances describe "$VM_NAME" --zone="$ZONE" --format="value(status)")

if [ "$VM_STATUS" = "RUNNING" ]; then
    echo "   VM is running, checking cron..."
    
    # Check cron in container
    CRON_CHECK=$(gcloud compute ssh "$VM_NAME" --zone="$ZONE" --tunnel-through-iap \
        --command "docker exec etl-runner crontab -l 2>/dev/null || echo 'Container not running'" 2>/dev/null || echo "SSH failed")
    
    if echo "$CRON_CHECK" | grep -q "etl.run_nightly_etl"; then
        echo "   ✅ Cron is configured: 3 AM UTC"
    else
        echo "   ⚠️  Cron may not be configured. Run: make deploy-etl"
    fi
else
    echo "   VM is ${VM_STATUS}, skipping cron check"
fi

echo ""
echo "=============================================="
echo " ✅ Scheduled ETL Setup Complete"
echo "=============================================="
echo ""
echo " Daily Timeline (UTC):"
echo "   2:00 AM - VM starts automatically"
echo "   3:00 AM - Cron triggers ETL"
echo "   ~3:30 AM - ETL completes, email sent"
echo "   5:00 AM - VM stops automatically"
echo ""
echo " Cost: ~\$1.50/month (3 hours/day × 30 days × \$0.017/hr)"
echo ""
echo " Manual Operations:"
echo "   Start VM:    gcloud compute instances start ${VM_NAME} --zone=${ZONE}"
echo "   Stop VM:     gcloud compute instances stop ${VM_NAME} --zone=${ZONE}"
echo "   Run ETL now: gcloud compute ssh ${VM_NAME} --zone=${ZONE} --tunnel-through-iap -- docker exec etl-runner python -m etl.run_nightly_etl"
echo ""
echo " View Schedule:"
echo "   gcloud compute resource-policies describe ${SCHEDULE_NAME} --region=${REGION}"
echo ""
echo " Disable Schedule:"
echo "   gcloud compute instances remove-resource-policies ${VM_NAME} --zone=${ZONE} --resource-policies=${SCHEDULE_NAME}"
echo ""

