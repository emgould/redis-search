#!/usr/bin/env bash
# Manage Cloud Scheduler jobs that start/stop the ETL VM on a daily schedule.
#
# Two jobs are created:
#   etl-vm-start  -- starts the VM at 2 AM ET (30 min before warmup, 1 hr before ETL)
#   etl-vm-stop   -- stops the VM at 7 AM ET (buffer after up to 3 hr ETL run)
#
# The VM's Docker container uses --restart=always, so the etl-runner container
# (and its internal cron) comes back automatically when the VM starts.
#
# The scheduler service account needs compute.instances.start and
# compute.instances.stop on the target instance.

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration (matches deploy_etl_vm.sh)
# ---------------------------------------------------------------------------

PROJECT_ID="${GCP_PROJECT_ID:-media-circle}"
REGION="us-central1"
ZONE="${REGION}-a"
VM_NAME="etl-runner-vm"

# Schedule defaults (America/New_York)
#   Start at 2 AM ET — 30 min before warmup, 1 hr before ETL cron
#   Stop at 7 AM ET — buffer for up to 3 hr ETL run
START_SCHEDULE="${ETL_VM_START_SCHEDULE:-0 2 * * *}"
STOP_SCHEDULE="${ETL_VM_STOP_SCHEDULE:-0 7 * * *}"
TIME_ZONE="${ETL_VM_TIME_ZONE:-America/New_York}"

# Scheduler job names
START_JOB_NAME="etl-vm-start"
STOP_JOB_NAME="etl-vm-stop"

# Service account
SCHEDULER_SA_NAME="cloud-scheduler-sa"
SCHEDULER_SA_EMAIL="${SCHEDULER_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# Compute Engine API endpoints
COMPUTE_API="https://compute.googleapis.com/compute/v1"
INSTANCE_URL="${COMPUTE_API}/projects/${PROJECT_ID}/zones/${ZONE}/instances/${VM_NAME}"

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

usage() {
    cat <<'EOF'
Usage: ./scripts/setup_etl_vm_scheduler.sh [action]

Actions:
  create   Create the scheduler jobs (default)
  update   Update existing scheduler jobs
  test     Manually trigger both jobs
  delete   Delete the scheduler jobs
  status   Show current status of scheduler jobs and VM

Environment variables (optional overrides):
  GCP_PROJECT_ID            GCP project (default: media-circle)
  ETL_VM_START_SCHEDULE     Cron for start (default: 0 21 * * *)
  ETL_VM_STOP_SCHEDULE      Cron for stop  (default: 0 18 * * *)
  ETL_VM_TIME_ZONE          Timezone (default: America/New_York)
EOF
}

ACTION="${1:-create}"

if [[ "${ACTION}" != "create" && "${ACTION}" != "update" && \
      "${ACTION}" != "test" && "${ACTION}" != "delete" && \
      "${ACTION}" != "status" ]]; then
    usage
    exit 1
fi

if ! command -v gcloud >/dev/null 2>&1; then
    echo "gcloud CLI is required." >&2
    exit 1
fi

gcloud config set project "${PROJECT_ID}" 2>/dev/null

# ---------------------------------------------------------------------------
# Enable APIs
# ---------------------------------------------------------------------------

echo "Enabling required APIs..."
gcloud services enable cloudscheduler.googleapis.com --project="${PROJECT_ID}" 2>/dev/null || true
gcloud services enable compute.googleapis.com --project="${PROJECT_ID}" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Service account setup
# ---------------------------------------------------------------------------

setup_service_account() {
    echo "Setting up service account ${SCHEDULER_SA_EMAIL}..."

    if ! gcloud iam service-accounts describe "${SCHEDULER_SA_EMAIL}" \
        --project="${PROJECT_ID}" >/dev/null 2>&1; then
        echo "  Creating service account..."
        gcloud iam service-accounts create "${SCHEDULER_SA_NAME}" \
            --display-name="Cloud Scheduler Service Account" \
            --project="${PROJECT_ID}" \
            --description="Used by Cloud Scheduler to start/stop VMs and manage Cloud Run services"
    else
        echo "  Service account already exists."
    fi

    echo "  Granting compute.instanceAdmin.v1 on ${VM_NAME}..."
    gcloud compute instances add-iam-policy-binding "${VM_NAME}" \
        --project="${PROJECT_ID}" \
        --zone="${ZONE}" \
        --member="serviceAccount:${SCHEDULER_SA_EMAIL}" \
        --role="roles/compute.instanceAdmin.v1" 2>/dev/null || {
        echo "  Note: Permission may already be granted."
    }
}

# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------

create_scheduler_jobs() {
    echo ""
    echo "Creating Cloud Scheduler jobs..."
    echo "  VM:       ${VM_NAME} (${ZONE})"
    echo "  Start:    ${START_SCHEDULE} ${TIME_ZONE}"
    echo "  Stop:     ${STOP_SCHEDULE} ${TIME_ZONE}"
    echo ""

    # --- START job ---
    if gcloud scheduler jobs describe "${START_JOB_NAME}" \
        --project="${PROJECT_ID}" --location="${REGION}" >/dev/null 2>&1; then
        echo "  ${START_JOB_NAME} already exists. Use 'update' to modify." >&2
        exit 1
    fi

    gcloud scheduler jobs create http "${START_JOB_NAME}" \
        --project="${PROJECT_ID}" \
        --location="${REGION}" \
        --schedule="${START_SCHEDULE}" \
        --time-zone="${TIME_ZONE}" \
        --uri="${INSTANCE_URL}/start" \
        --http-method=POST \
        --oauth-service-account-email="${SCHEDULER_SA_EMAIL}" \
        --oauth-token-scope="https://www.googleapis.com/auth/compute" \
        --description="Start ETL VM (${VM_NAME}) at ${START_SCHEDULE} ${TIME_ZONE}" \
        --attempt-deadline=120s \
        --max-retry-attempts=2 \
        --min-backoff=30s \
        --max-backoff=60s

    echo "  Created ${START_JOB_NAME}"

    # --- STOP job ---
    if gcloud scheduler jobs describe "${STOP_JOB_NAME}" \
        --project="${PROJECT_ID}" --location="${REGION}" >/dev/null 2>&1; then
        echo "  ${STOP_JOB_NAME} already exists. Use 'update' to modify." >&2
        exit 1
    fi

    gcloud scheduler jobs create http "${STOP_JOB_NAME}" \
        --project="${PROJECT_ID}" \
        --location="${REGION}" \
        --schedule="${STOP_SCHEDULE}" \
        --time-zone="${TIME_ZONE}" \
        --uri="${INSTANCE_URL}/stop" \
        --http-method=POST \
        --oauth-service-account-email="${SCHEDULER_SA_EMAIL}" \
        --oauth-token-scope="https://www.googleapis.com/auth/compute" \
        --description="Stop ETL VM (${VM_NAME}) at ${STOP_SCHEDULE} ${TIME_ZONE}" \
        --attempt-deadline=120s \
        --max-retry-attempts=2 \
        --min-backoff=30s \
        --max-backoff=60s

    echo "  Created ${STOP_JOB_NAME}"

    echo ""
    echo "ETL VM scheduler created:"
    echo "  Start: ${START_JOB_NAME} → ${START_SCHEDULE} ${TIME_ZONE}"
    echo "  Stop:  ${STOP_JOB_NAME} → ${STOP_SCHEDULE} ${TIME_ZONE}"
    echo ""
    echo "To test:  ./scripts/setup_etl_vm_scheduler.sh test"
    echo "Console:  https://console.cloud.google.com/cloudscheduler?project=${PROJECT_ID}"
}

# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------

update_scheduler_jobs() {
    echo "Updating Cloud Scheduler jobs..."

    for job_name in "${START_JOB_NAME}" "${STOP_JOB_NAME}"; do
        if ! gcloud scheduler jobs describe "${job_name}" \
            --project="${PROJECT_ID}" --location="${REGION}" >/dev/null 2>&1; then
            echo "  ${job_name} does not exist. Use 'create' first." >&2
            exit 1
        fi
    done

    gcloud scheduler jobs update http "${START_JOB_NAME}" \
        --project="${PROJECT_ID}" \
        --location="${REGION}" \
        --schedule="${START_SCHEDULE}" \
        --time-zone="${TIME_ZONE}" \
        --uri="${INSTANCE_URL}/start" \
        --http-method=POST \
        --oauth-service-account-email="${SCHEDULER_SA_EMAIL}" \
        --oauth-token-scope="https://www.googleapis.com/auth/compute" \
        --description="Start ETL VM (${VM_NAME}) at ${START_SCHEDULE} ${TIME_ZONE}" \
        --attempt-deadline=120s \
        --max-retry-attempts=2

    echo "  Updated ${START_JOB_NAME}"

    gcloud scheduler jobs update http "${STOP_JOB_NAME}" \
        --project="${PROJECT_ID}" \
        --location="${REGION}" \
        --schedule="${STOP_SCHEDULE}" \
        --time-zone="${TIME_ZONE}" \
        --uri="${INSTANCE_URL}/stop" \
        --http-method=POST \
        --oauth-service-account-email="${SCHEDULER_SA_EMAIL}" \
        --oauth-token-scope="https://www.googleapis.com/auth/compute" \
        --description="Stop ETL VM (${VM_NAME}) at ${STOP_SCHEDULE} ${TIME_ZONE}" \
        --attempt-deadline=120s \
        --max-retry-attempts=2

    echo "  Updated ${STOP_JOB_NAME}"
    echo ""
    echo "Schedules updated."
}

# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

test_scheduler_jobs() {
    echo "Manually triggering ETL VM scheduler jobs..."
    echo ""

    for job_name in "${START_JOB_NAME}" "${STOP_JOB_NAME}"; do
        if ! gcloud scheduler jobs describe "${job_name}" \
            --project="${PROJECT_ID}" --location="${REGION}" >/dev/null 2>&1; then
            echo "  ${job_name} does not exist. Create it first." >&2
            exit 1
        fi
    done

    read -rp "Trigger START (${START_JOB_NAME})? [y/N] " ans
    if [[ "${ans}" =~ ^[Yy]$ ]]; then
        gcloud scheduler jobs run "${START_JOB_NAME}" \
            --project="${PROJECT_ID}" --location="${REGION}"
        echo "  Triggered ${START_JOB_NAME}"
    fi

    echo ""
    read -rp "Trigger STOP (${STOP_JOB_NAME})? [y/N] " ans
    if [[ "${ans}" =~ ^[Yy]$ ]]; then
        gcloud scheduler jobs run "${STOP_JOB_NAME}" \
            --project="${PROJECT_ID}" --location="${REGION}"
        echo "  Triggered ${STOP_JOB_NAME}"
    fi
}

# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------

delete_scheduler_jobs() {
    echo "Deleting ETL VM scheduler jobs..."
    read -rp "Are you sure? [y/N] " ans
    if [[ ! "${ans}" =~ ^[Yy]$ ]]; then
        echo "Cancelled."
        exit 0
    fi

    for job_name in "${START_JOB_NAME}" "${STOP_JOB_NAME}"; do
        if gcloud scheduler jobs describe "${job_name}" \
            --project="${PROJECT_ID}" --location="${REGION}" >/dev/null 2>&1; then
            gcloud scheduler jobs delete "${job_name}" \
                --project="${PROJECT_ID}" --location="${REGION}" --quiet
            echo "  Deleted ${job_name}"
        else
            echo "  ${job_name} not found (skipped)"
        fi
    done
}

# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

show_status() {
    echo "ETL VM Scheduler Status"
    echo "======================="
    echo ""

    # VM status
    local vm_status
    vm_status=$(gcloud compute instances describe "${VM_NAME}" \
        --zone="${ZONE}" --format="value(status)" 2>/dev/null || echo "NOT_FOUND")
    echo "  VM: ${VM_NAME} (${ZONE}) → ${vm_status}"
    echo ""

    # Scheduler jobs
    for job_name in "${START_JOB_NAME}" "${STOP_JOB_NAME}"; do
        if ! gcloud scheduler jobs describe "${job_name}" \
            --project="${PROJECT_ID}" --location="${REGION}" >/dev/null 2>&1; then
            echo "  ${job_name}: NOT FOUND"
        else
            local state schedule last_run
            state=$(gcloud scheduler jobs describe "${job_name}" \
                --project="${PROJECT_ID}" --location="${REGION}" \
                --format="value(state)" 2>/dev/null || echo "?")
            schedule=$(gcloud scheduler jobs describe "${job_name}" \
                --project="${PROJECT_ID}" --location="${REGION}" \
                --format="value(schedule)" 2>/dev/null || echo "?")
            last_run=$(gcloud scheduler jobs describe "${job_name}" \
                --project="${PROJECT_ID}" --location="${REGION}" \
                --format="value(lastAttemptTime)" 2>/dev/null || echo "never")
            echo "  ${job_name}: state=${state}  schedule=\"${schedule}\"  last_run=${last_run}"
        fi
    done

    echo ""
    echo "Console: https://console.cloud.google.com/cloudscheduler?project=${PROJECT_ID}"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

case "${ACTION}" in
    create)
        setup_service_account
        create_scheduler_jobs
        ;;
    update)
        setup_service_account
        update_scheduler_jobs
        ;;
    test)
        test_scheduler_jobs
        ;;
    delete)
        delete_scheduler_jobs
        ;;
    status)
        show_status
        ;;
esac
