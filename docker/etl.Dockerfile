# ETL Service Dockerfile
# Runs the nightly ETL process for TMDB changes
#
# Build: docker build -f docker/etl.Dockerfile -t redis-search-etl .
# Run:   docker run --env-file config/etl.env redis-search-etl
#
# For cron scheduling, use the entrypoint script which:
# 1. Writes environment variables to /etc/environment for cron to read
# 2. Starts the cron daemon in foreground mode

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (gcc for compiling, cron for scheduling)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY web/ ./web/
COPY config/etl_jobs.yaml ./config/

# Copy entrypoint script
COPY docker/etl-entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Set Python path
ENV PYTHONPATH=/app:/app/src

# Create log file for cron output
RUN touch /var/log/cron.log

# Default: run ETL once (for manual/testing invocation)
# Use ENTRYPOINT with "cron" argument to start cron scheduler
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["run"]

