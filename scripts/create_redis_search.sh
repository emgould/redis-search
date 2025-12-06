#!/bin/bash
set -e

SERVICE_NAME="redis-search-api"
REGION="us-central1"
PROJECT_ID=$(gcloud config get-value project)
REPO="redis-services"
IMAGE="us-central1-docker.pkg.dev/$PROJECT_ID/$REPO/$SERVICE_NAME"

echo "▶ Ensuring Artifact Registry repository exists..."
gcloud artifacts repositories describe $REPO \
  --location=us-central1 || \
gcloud artifacts repositories create $REPO \
  --repository-format=docker \
  --location=us-central1

echo "▶ Building Docker image..."
docker build -t $IMAGE -f src/search_api/Dockerfile .

echo "▶ Pushing image to Artifact Registry..."
docker push $IMAGE

echo "▶ Deploying Cloud Run service: $SERVICE_NAME..."
gcloud run deploy $SERVICE_NAME \
  --image=$IMAGE \
  --platform=managed \
  --region=$REGION \
  --cpu=1 \
  --memory=512Mi \
  --allow-unauthenticated \
  --max-instances=10 \
  --set-env-vars="$(cat config/prod.env | xargs | sed 's/ /,/g')"

echo "✔ Deployment complete!"