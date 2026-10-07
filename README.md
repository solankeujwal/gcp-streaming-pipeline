# Real-Time Streaming Pipeline: Pub/Sub → Dataflow → BigQuery

Set `PROJECT_ID` in `config.env` first. Default region: `asia-south1` (Mumbai)

```
publisher.py ──► Pub/Sub topic ──► subscription ──► Dataflow (Beam, Streaming Engine)
                                                        │
                      ┌─────────────────────────────────┼──────────────────────────┐
                      ▼                                 ▼                          ▼
              BigQuery: events               BigQuery: events_agg_1min   BigQuery: events_dead_letter
          (raw, day-partitioned, clustered)    (1-min tumbling windows)      (malformed messages)
```

## Run it (Cloud Shell recommended)

```bash
chmod +x *.sh
gcloud auth login && gcloud auth application-default login   # skip in Cloud Shell

./setup.sh                                  # APIs, bucket, Pub/Sub, BigQuery, service account
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
./run_pipeline.sh                           # submits streaming job (takes ~3-5 min to start)

# in a second terminal, once the job shows "Running" in the console:
python publisher.py --project $PROJECT_ID --topic events-topic --rate 10 --duration 300
```

Then run the queries in `queries.sql` in the BigQuery console. Rows appear within seconds.

## Design notes
- **Dead-letter branch**: bad JSON / missing fields / unknown types never block the pipeline.
- **Raw table** is partitioned by `event_ts` and clustered by `event_type, user_id` to cut query cost.
- **Windowing**: 60s fixed windows on Pub/Sub publish time (swap in `timestamp_attribute` for true event-time).
- **Least privilege**: workers run as a dedicated service account, not the default Compute SA.
- **Delivery semantics**: Dataflow gives exactly-once processing within the pipeline, but `STREAMING_INSERTS`
  from Beam can duplicate on retries. For stricter guarantees, switch to
  `Method.STORAGE_WRITE_API` (needs `triggering_frequency`) or dedupe on `event_id` in a view.

## Cost warning
A streaming Dataflow job bills **continuously** until stopped. When finished, run `./teardown.sh`
(it drains the job so in-flight data is flushed).
