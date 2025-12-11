#!/bin/bash
# ETL Entrypoint Script
#
# This script handles two modes:
# 1. "run" (default): Run ETL once and exit
# 2. "cron": Start cron daemon for scheduled ETL runs at 3 AM UTC
#
# When running in cron mode, environment variables are exported to
# /etc/environment so cron jobs can access them.

set -e

# Export all current environment variables to /etc/environment
# This makes them available to cron jobs
export_env_for_cron() {
    echo "Exporting environment variables for cron..."
    
    # Write all relevant env vars to a file that cron job will source
    cat > /app/.env << EOF
# Auto-generated environment for cron jobs
export PATH=/usr/local/bin:/usr/bin:/bin
export PYTHONPATH=/app:/app/src
export REDIS_HOST=${REDIS_HOST:-localhost}
export REDIS_PORT=${REDIS_PORT:-6379}
export REDIS_PASSWORD=${REDIS_PASSWORD:-}
export TMDB_READ_TOKEN=${TMDB_READ_TOKEN:-}
export TMDB_API_KEY=${TMDB_API_KEY:-}
export GCS_BUCKET=${GCS_BUCKET:-}
export GCS_ETL_PREFIX=${GCS_ETL_PREFIX:-}
export ETL_CONFIG_PATH=${ETL_CONFIG_PATH:-/app/config/etl_jobs.yaml}
export ETL_NOTIFICATION_EMAIL=${ETL_NOTIFICATION_EMAIL:-}
export SENDGRID_SERVER=${SENDGRID_SERVER:-}
export SENDGRID_PORT=${SENDGRID_PORT:-}
export SENDGRID_USERNAME=${SENDGRID_USERNAME:-}
export SENDGRID_PASSWORD=${SENDGRID_PASSWORD:-}
export SENDGRID_FROM_EMAIL=${SENDGRID_FROM_EMAIL:-}
export GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS:-}
EOF
    chmod 600 /app/.env
}

# Setup cron job to run at 3 AM UTC
setup_cron() {
    echo "Setting up cron job for 3 AM UTC..."
    
    # Create the cron job that sources env vars before running
    # Output goes to stdout/stderr (Docker logs) via /proc/1/fd
    cat > /etc/cron.d/etl-cron << EOF
# Run ETL at 3 AM UTC daily
0 3 * * * root . /app/.env && cd /app && python -m etl.run_nightly_etl >> /proc/1/fd/1 2>> /proc/1/fd/2

# Empty line required by cron
EOF
    
    chmod 0644 /etc/cron.d/etl-cron
    crontab /etc/cron.d/etl-cron
    
    echo "Cron job installed:"
    crontab -l
}

case "${1:-run}" in
    run)
        echo "Running ETL once..."
        exec python -m etl.run_nightly_etl "${@:2}"
        ;;
    cron)
        export_env_for_cron
        setup_cron
        
        echo "Starting cron daemon..."
        echo "ETL will run daily at 3 AM UTC"
        echo "To run manually: docker exec etl-runner python -m etl.run_nightly_etl"
        echo ""
        
        # Start cron in foreground and tail logs
        # cron -f runs in foreground
        exec cron -f
        ;;
    test)
        # Test mode: verify env vars and cron setup without starting daemon
        echo "=== Test Mode ==="
        export_env_for_cron
        setup_cron
        
        echo ""
        echo "=== Environment Variables ==="
        echo "REDIS_HOST: ${REDIS_HOST}"
        echo "REDIS_PORT: ${REDIS_PORT}"
        echo "TMDB_READ_TOKEN: ${TMDB_READ_TOKEN:0:20}..."
        echo "GCS_BUCKET: ${GCS_BUCKET}"
        echo "ETL_CONFIG_PATH: ${ETL_CONFIG_PATH}"
        
        echo ""
        echo "=== Testing ETL dry-run ==="
        python -m etl.run_nightly_etl --dry-run
        ;;
    scheduled)
        # Scheduled mode: Run ETL once then shutdown the VM
        # Used with GCP Instance Schedules - VM starts at 2 AM, runs ETL, shuts down
        echo "=============================================="
        echo "🕐 SCHEDULED ETL MODE"
        echo "=============================================="
        echo "Running ETL, then VM will shut down..."
        echo ""
        
        # Run the ETL
        python -m etl.run_nightly_etl "${@:2}"
        ETL_EXIT_CODE=$?
        
        echo ""
        echo "=============================================="
        echo "ETL completed with exit code: ${ETL_EXIT_CODE}"
        echo "Initiating VM shutdown..."
        echo "=============================================="
        
        # Give a few seconds for logs to flush
        sleep 5
        
        # Shutdown the VM using the metadata server
        # This tells GCE to stop the instance gracefully
        curl -X POST -H "Metadata-Flavor: Google" \
            "http://metadata.google.internal/computeMetadata/v1/instance/guest-attributes/shutdown-requested" \
            -d "true" 2>/dev/null || true
        
        # Alternative: Direct shutdown command (requires privileged container)
        # The VM's startup script will handle actual shutdown
        echo "SHUTDOWN_REQUESTED" > /tmp/shutdown_flag
        
        exit ${ETL_EXIT_CODE}
        ;;
    *)
        # Pass through any other command
        exec "$@"
        ;;
esac

