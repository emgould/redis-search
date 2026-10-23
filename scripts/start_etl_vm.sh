#!/usr/bin/env bash
# Start the ETL VM with automatic retries on GCE zone resource exhaustion
# (ZONE_RESOURCE_POOL_EXHAUSTED / ZONE_RESOURCE_AVAILABILITY / stockout).
#
# Usage:
#   ./scripts/start_etl_vm.sh
#
# Environment variables (optional):
#   GCP_PROJECT_ID               GCP project (default: media-circle)
#   ETL_VM_NAME                  VM instance name (default: etl-runner-vm)
#   ETL_VM_ZONE                  GCE zone (default: us-central1-a)
#   ETL_VM_START_RETRY_INTERVAL  Seconds between retries (default: 30)
#   ETL_VM_START_MAX_ATTEMPTS    Max attempts, 0 = infinite (default: 0)

set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-media-circle}"
VM_NAME="${ETL_VM_NAME:-etl-runner-vm}"
ZONE="${ETL_VM_ZONE:-us-central1-a}"
RETRY_INTERVAL="${ETL_VM_START_RETRY_INTERVAL:-30}"
MAX_ATTEMPTS="${ETL_VM_START_MAX_ATTEMPTS:-0}"

is_zone_exhaustion_error() {
    local output="$1"
    if echo "${output}" | grep -Eiq \
        "ZONE_RESOURCE_POOL_EXHAUSTED|ZONE_RESOURCE_AVAILABILITY|stockout|does not have enough resources available|currently unavailable in the .* zone"; then
        return 0
    fi
    return 1
}

get_vm_status() {
    gcloud compute instances describe "${VM_NAME}" \
        --zone="${ZONE}" \
        --project="${PROJECT_ID}" \
        --format="value(status)" 2>/dev/null || echo "NOT_FOUND"
}

STATUS="$(get_vm_status)"
if [[ "${STATUS}" == "RUNNING" ]]; then
    echo "✅ ${VM_NAME} (${ZONE}) is already RUNNING."
    exit 0
fi

if [[ "${STATUS}" == "NOT_FOUND" ]]; then
    echo "❌ VM '${VM_NAME}' not found in zone '${ZONE}' (project '${PROJECT_ID}')." >&2
    exit 1
fi

attempt=1
while true; do
    if [[ "${MAX_ATTEMPTS}" -gt 0 ]]; then
        echo "🚀 Starting ${VM_NAME} in ${ZONE} (attempt ${attempt}/${MAX_ATTEMPTS})..."
    else
        echo "🚀 Starting ${VM_NAME} in ${ZONE} (attempt ${attempt})..."
    fi

    set +e
    START_OUTPUT=$(gcloud compute instances start "${VM_NAME}" \
        --zone="${ZONE}" \
        --project="${PROJECT_ID}" \
        --quiet 2>&1)
    START_RC=$?
    set -e

    if [[ ${START_RC} -eq 0 ]]; then
        for _ in {1..6}; do
            STATUS="$(get_vm_status)"
            if [[ "${STATUS}" == "RUNNING" ]]; then
                echo "✅ ${VM_NAME} is now RUNNING."
                exit 0
            fi
            if [[ "${STATUS}" != "STAGING" && "${STATUS}" != "PROVISIONING" ]]; then
                break
            fi
            sleep 5
        done
        echo "⚠️  Start command returned 0 but VM status is ${STATUS}; retrying..."
    else
        if ! is_zone_exhaustion_error "${START_OUTPUT}"; then
            echo "❌ Failed to start ${VM_NAME} due to non-retryable error:" >&2
            echo "${START_OUTPUT}" >&2
            exit "${START_RC}"
        fi
        echo "⏳ Zone resource exhaustion in ${ZONE} on attempt ${attempt}:"
        echo "   $(echo "${START_OUTPUT}" | grep -Ei 'ZONE_RESOURCE|unavailable|enough resources|ERROR' | head -n 2 | tr '\n' ' ')"
    fi

    if [[ "${MAX_ATTEMPTS}" -gt 0 && "${attempt}" -ge "${MAX_ATTEMPTS}" ]]; then
        echo "❌ Reached max attempts (${MAX_ATTEMPTS}) waiting for ${VM_NAME} in ${ZONE}." >&2
        exit 1
    fi

    echo "   Retrying in ${RETRY_INTERVAL}s... (Ctrl+C to cancel)"
    sleep "${RETRY_INTERVAL}"
    attempt=$((attempt + 1))
done
