#!/bin/bash
set -e

SERVICE_NAME="redis-etl"
REGION="us-central1"
PROJECT_ID=$(gcloud config get-value project)
REPO="redis-services"
IMAGE="us-central1-docker.pkg.dev/$PROJECT_ID/$REPO/$SERVICE_NAME"
JOB_NAME="redis-etl-job"

echo "▶ Ensuring Artifact Registry repository exists..."
gcloud artifacts repositories describe $REPO \
  --location=us-central1 || \
gcloud artifacts repositories create $REPO \
  --repository-format=docker \
  --location=us-central1

echo "▶ Building ETL Docker image..."
docker build -t $IMAGE -f src/etl/Dockerfile .

echo "▶ Pushing image to Artifact Registry..."
docker push $IMAGE

echo "▶ Deploying ETL Cloud Run service (private)..."
gcloud run deploy $SERVICE_NAME \
  --image=$IMAGE \
  --platform=managed \
  --region=$REGION \
  --no-allow-unauthenticated \
  --cpu=1 \
  --memory=512Mi \
  --timeout=900 \
  --max-instances=1 \
  --set-env-vars="$(cat config/prod.env | xargs | sed 's/ /,/g')"

echo "▶ Checking if Cloud Scheduler job exists..."
if gcloud scheduler jobs describe $JOB_NAME --location=$REGION >/dev/null 2>&1; then
    echo "▶ Scheduler job already exists — updating..."
    gcloud scheduler jobs update http $JOB_NAME \
      --schedule="0 */6 * * *" \
      --uri="$(gcloud run services describe $SERVICE_NAME --platform=managed --region=$REGION --format='value(status.url)')" \
      --oidc-service-account-email="$(gcloud iam service-accounts list --filter='Compute Engine default service account' --format='value(email)')" \
      --http-method=POST
else
    echo "▶ Creating Cloud Scheduler job..."
    gcloud scheduler jobs create http $JOB_NAME \
      --schedule="0 */6 * * *" \
      --uri="$(gcloud run services describe $SERVICE_NAME --platform=managed --region=$REGION --format='value(status.url)')" \
      --oidc-service-account-email="$(gcloud iam service-accounts list --filter='Compute Engine default service account' --format='value(email)')" \
      --http-method=POST
fi

echo "✔ ETL service + scheduler configured successfully!"