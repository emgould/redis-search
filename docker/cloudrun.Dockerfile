FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ARG INSTALL_DEV_TOOLS=false
RUN if [ "$INSTALL_DEV_TOOLS" = "true" ]; then \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        curl gnupg openssh-client && \
    curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
        gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
        > /etc/apt/sources.list.d/google-cloud-sdk.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends google-cloud-cli && \
    rm -rf /var/lib/apt/lists/*; \
    fi

COPY . ./

# Set Python path to find modules
ENV PYTHONPATH=/app:/app/src

# Cloud Run sets PORT env var
ENV PORT=8080

# Source secret file if it exists (Cloud Run mounts secrets as files at /secrets/env)
# This makes all env vars from the secret bundle available to the process
CMD ["sh", "-c", "export PYTHONPATH=/app:/app/src && if [ -f /secrets/env ]; then set -a && . /secrets/env && set +a; fi && uvicorn web.app:app --host 0.0.0.0 --port ${PORT}"]
