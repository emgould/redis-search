FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

# Cloud Run sets PORT env var
ENV PORT=8080

# Source secret file if it exists (Cloud Run mounts secrets as files)
# This makes all env vars from the secret bundle available to the process
CMD ["sh", "-c", "if [ -f \"${API_ENV}\" ]; then set -a && source \"${API_ENV}\" && set +a; fi && uvicorn src.search_api.main:app --host 0.0.0.0 --port ${PORT}"]
