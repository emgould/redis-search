FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

# Cloud Run sets PORT env var
ENV PORT=8080

CMD ["sh", "-c", "uvicorn src.search_api.main:app --host 0.0.0.0 --port ${PORT}"]
