#!/usr/bin/env bash
# Launches the streaming job on Dataflow.
set -euo pipefail
source "$(dirname "$0")/config.env"

python pipeline.py \
  --runner=DataflowRunner \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --job_name="$JOB_NAME" \
  --temp_location="gs://${BUCKET}/temp" \
  --staging_location="gs://${BUCKET}/staging" \
  --service_account_email="$SA_EMAIL" \
  --subscription="projects/${PROJECT_ID}/subscriptions/${SUBSCRIPTION}" \
  --dataset="${PROJECT_ID}:${DATASET}" \
  --streaming \
  --enable_streaming_engine \
  --max_num_workers=3 \
  --num_workers=1 \
  --machine_type=e2-standard-2
