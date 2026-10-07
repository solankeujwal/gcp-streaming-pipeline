#!/usr/bin/env bash
# Provisions all GCP resources: APIs, bucket, Pub/Sub, BigQuery, service account + IAM.
set -euo pipefail
source "$(dirname "$0")/config.env"

gcloud config set project "$PROJECT_ID"

echo ">> Enabling APIs"
gcloud services enable pubsub.googleapis.com dataflow.googleapis.com bigquery.googleapis.com \
  compute.googleapis.com storage.googleapis.com

echo ">> Creating GCS bucket for Dataflow temp/staging"
gcloud storage buckets create "gs://${BUCKET}" --location="$REGION" --uniform-bucket-level-access || true

echo ">> Creating Pub/Sub topic + subscription"
gcloud pubsub topics create "$TOPIC" || true
gcloud pubsub subscriptions create "$SUBSCRIPTION" --topic="$TOPIC" --ack-deadline=60 || true

echo ">> Creating BigQuery dataset + tables"
bq --location="$REGION" mk -d "${PROJECT_ID}:${DATASET}" || true

bq mk --table \
  --time_partitioning_type=DAY --time_partitioning_field=event_ts \
  --clustering_fields=event_type,user_id \
  "${PROJECT_ID}:${DATASET}.events" \
  event_id:STRING,user_id:STRING,event_type:STRING,product_id:STRING,amount:FLOAT,currency:STRING,event_ts:TIMESTAMP,ingested_at:TIMESTAMP || true

bq mk --table "${PROJECT_ID}:${DATASET}.events_agg_1min" \
  window_start:TIMESTAMP,window_end:TIMESTAMP,event_type:STRING,event_count:INTEGER,total_amount:FLOAT || true

bq mk --table "${PROJECT_ID}:${DATASET}.events_dead_letter" \
  raw_payload:STRING,error:STRING,ingested_at:TIMESTAMP || true

echo ">> Creating least-privilege worker service account"
gcloud iam service-accounts create "$SA_NAME" --display-name="Dataflow streaming worker" || true
for ROLE in roles/dataflow.worker roles/pubsub.subscriber roles/pubsub.viewer \
            roles/bigquery.dataEditor roles/bigquery.jobUser roles/storage.objectAdmin; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" --role="$ROLE" --condition=None --quiet >/dev/null
done

echo "Setup complete."
