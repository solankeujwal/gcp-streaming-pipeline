#!/usr/bin/env bash
# Stops the job and deletes everything. A streaming job bills continuously - always run this when done.
set -uo pipefail
source "$(dirname "$0")/config.env"

JOB_ID=$(gcloud dataflow jobs list --region="$REGION" --status=active \
  --filter="name=${JOB_NAME}" --format="value(JOB_ID)" | head -n1)
[ -n "$JOB_ID" ] && gcloud dataflow jobs drain "$JOB_ID" --region="$REGION"

gcloud pubsub subscriptions delete "$SUBSCRIPTION" --quiet
gcloud pubsub topics delete "$TOPIC" --quiet
bq rm -r -f -d "${PROJECT_ID}:${DATASET}"
echo "Wait for the Dataflow drain to finish (check console), then run:"
echo "  gcloud storage rm -r gs://${BUCKET}"
echo "  gcloud iam service-accounts delete ${SA_EMAIL} --quiet"
