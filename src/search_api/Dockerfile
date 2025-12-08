FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

# Set Python path to find modules
ENV PYTHONPATH=/app:/app/src

# Cloud Run sets PORT env var
ENV PORT=8080

# Source secret file if it exists (Cloud Run mounts secrets as files)
# This makes all env vars from the secret bundle available to the process
CMD ["sh", "-c", "export PYTHONPATH=/app:/app/src && if [ -f \"${ETL_ENV}\" ]; then set -a && source \"${ETL_ENV}\" && set +a; fi && uvicorn web.app:app --host 0.0.0.0 --port ${PORT}"]
