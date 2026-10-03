#!/usr/bin/env bash
# Deploy to Cloud Run from Cloud Shell. Run it inside this folder:  ./deploy.sh
# It creates a service account, a bucket for the control plane's state, and the service itself.
set -euo pipefail
PROJECT="${PROJECT:-$(gcloud config get-value project)}"
REGION="${REGION:-us-central1}"
SERVICE="${SERVICE:-medallion-control-plane}"
SA="medallion-cp@${PROJECT}.iam.gserviceaccount.com"
STATE_BUCKET="${STATE_BUCKET:-${PROJECT}-medallion-state}"
APP_TOKEN="${APP_TOKEN:-$(openssl rand -hex 24)}"

echo "Project ${PROJECT}, region ${REGION}"
gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com \
  bigquery.googleapis.com storage.googleapis.com secretmanager.googleapis.com --project "${PROJECT}"

gcloud iam service-accounts describe "${SA}" --project "${PROJECT}" >/dev/null 2>&1 || \
  gcloud iam service-accounts create medallion-cp --display-name "Medallion control plane" --project "${PROJECT}"
for ROLE in roles/bigquery.dataEditor roles/bigquery.jobUser roles/storage.objectViewer roles/secretmanager.secretAccessor; do
  gcloud projects add-iam-policy-binding "${PROJECT}" --member "serviceAccount:${SA}" --role "${ROLE}" --condition=None --quiet >/dev/null
done

gcloud storage buckets describe "gs://${STATE_BUCKET}" >/dev/null 2>&1 || \
  gcloud storage buckets create "gs://${STATE_BUCKET}" --location "${REGION}" --uniform-bucket-level-access --project "${PROJECT}"
gcloud storage buckets add-iam-policy-binding "gs://${STATE_BUCKET}" --member "serviceAccount:${SA}" --role roles/storage.objectAdmin >/dev/null

# One instance, always on, with CPU outside requests: runs and the scheduler work in the background.
gcloud run deploy "${SERVICE}" --source . --project "${PROJECT}" --region "${REGION}" --service-account "${SA}" \
  --allow-unauthenticated --min-instances 1 --max-instances 1 --no-cpu-throttling --memory 1Gi --timeout 3600 \
  --set-env-vars "STATE_URI=gs://${STATE_BUCKET}/state.json,APP_TOKEN=${APP_TOKEN},GCP_PROJECT=${PROJECT},ENVIRONMENT=dev"

URL="$(gcloud run services describe "${SERVICE}" --project "${PROJECT}" --region "${REGION}" --format 'value(status.url)')"
echo
echo "Open:          ${URL}"
echo "Access token:  ${APP_TOKEN}"
echo "Keep the token. The site asks for it once per browser session."
