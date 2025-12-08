FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

# Source secret file if it exists (Cloud Run mounts secrets as files)
# This makes all env vars from the secret bundle available to the process
CMD ["sh", "-c", "if [ -f \"${ETL_ENV}\" ]; then set -a && source \"${ETL_ENV}\" && set +a; fi && python -m src.etl.bulk_loader"]
