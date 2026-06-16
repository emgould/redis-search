#!/bin/bash
# Destroy old Google Secret Manager versions, keeping only the newest non-destroyed
# version for each secret in a project.

set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-media-circle}"
APPLY=0
YES=0
SECRET_FILTER=""

usage() {
    cat <<'EOF'
Usage:
  scripts/cleanup_gcp_secret_versions.sh [--project PROJECT_ID] [--secret SECRET_NAME] [--apply] [--yes]

Options:
  --project PROJECT_ID  GCP project to scan. Defaults to GCP_PROJECT_ID or media-circle.
  --secret SECRET_NAME  Limit cleanup to one secret. Defaults to all secrets in the project.
  --apply              Destroy old versions. Without this flag, prints a dry-run plan.
  --yes                Skip the confirmation prompt when used with --apply.
  -h, --help           Show this help.

Default mode is dry-run. Destroyed versions cannot be recovered.
EOF
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --project)
            PROJECT_ID="${2:-}"
            shift 2
            ;;
        --secret)
            SECRET_FILTER="${2:-}"
            shift 2
            ;;
        --apply)
            APPLY=1
            shift
            ;;
        --yes)
            YES=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage
            exit 1
            ;;
    esac
done

if [ -z "${PROJECT_ID}" ]; then
    echo "PROJECT_ID is required. Pass --project or set GCP_PROJECT_ID." >&2
    exit 1
fi

if ! command -v gcloud >/dev/null 2>&1; then
    echo "gcloud CLI not found." >&2
    exit 1
fi

normalize_resource_id() {
    local resource="$1"
    echo "${resource##*/}"
}

list_secrets() {
    if [ -n "${SECRET_FILTER}" ]; then
        echo "${SECRET_FILTER}"
        return
    fi

    gcloud secrets list \
        --project="${PROJECT_ID}" \
        --format="value(name)"
}

list_active_versions() {
    local secret_name="$1"

    gcloud secrets versions list "${secret_name}" \
        --project="${PROJECT_ID}" \
        --filter="state!=DESTROYED" \
        --sort-by="~createTime" \
        --format="value(name)"
}

echo "Secret Manager version cleanup"
echo "Project: ${PROJECT_ID}"
if [ -n "${SECRET_FILTER}" ]; then
    echo "Secret: ${SECRET_FILTER}"
else
    echo "Secret: all secrets"
fi

if [ "${APPLY}" -eq 1 ]; then
    echo "Mode: APPLY"
    if [ "${YES}" -ne 1 ]; then
        read -r -p "This will permanently destroy old secret versions. Continue? (y/N) " reply
        if [[ ! "${reply}" =~ ^[Yy]$ ]]; then
            echo "Aborted."
            exit 0
        fi
    fi
else
    echo "Mode: DRY-RUN"
fi
echo ""

secrets_seen=0
versions_to_destroy=0

while IFS= read -r secret_resource; do
    if [ -z "${secret_resource}" ]; then
        continue
    fi

    secret_name=$(normalize_resource_id "${secret_resource}")
    secrets_seen=$((secrets_seen + 1))

    version_count=0
    keep_version=""

    while IFS= read -r version_resource; do
        if [ -z "${version_resource}" ]; then
            continue
        fi

        version=$(normalize_resource_id "${version_resource}")
        version_count=$((version_count + 1))

        if [ "${version_count}" -eq 1 ]; then
            keep_version="${version}"
            echo "${secret_name}: keeping version ${keep_version}"
            continue
        fi

        versions_to_destroy=$((versions_to_destroy + 1))

        if [ "${APPLY}" -eq 1 ]; then
            echo "  destroying version ${version}"
            gcloud secrets versions destroy "${version}" \
                --secret="${secret_name}" \
                --project="${PROJECT_ID}" \
                --quiet
        else
            echo "  would destroy version ${version}"
        fi
    done < <(list_active_versions "${secret_name}")

    if [ "${version_count}" -eq 0 ]; then
        echo "${secret_name}: no non-destroyed versions"
    fi
done < <(list_secrets)

echo ""
echo "Scanned secrets: ${secrets_seen}"
if [ "${APPLY}" -eq 1 ]; then
    echo "Destroyed old versions: ${versions_to_destroy}"
else
    echo "Old versions that would be destroyed: ${versions_to_destroy}"
fi
