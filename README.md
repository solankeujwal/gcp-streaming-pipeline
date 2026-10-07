
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

<img width="1920" height="1080" alt="6" src="https://github.com/user-attachments/assets/8bbfcb80-aece-4db2-b868-9f2af27a0e62" />
<img width="1920" height="1080" alt="5" src="https://github.com/user-attachments/assets/7c23c405-4a76-47f2-884d-689bd0b29bbc" />
<img width="1920" height="1080" alt="4" src="https://github.com/user-attachments/assets/25b42def-e194-41d1-a9ba-d90e39c0e058" />
<img width="1920" height="1080" alt="3" src="https://github.com/user-attachments/assets/e3a9276e-307d-45bc-9e08-ff46f30d46a2" />
<img width="1920" height="1080" alt="2" src="https://github.com/user-attachments/assets/3e750f8f-be84-4a98-a8f1-94e725b9a5a4" />
<img width="1920" height="1080" alt="1" src="https://github.com/user-attachments/assets/07fb02a7-0b7a-483f-924c-93b9284d8e40" />
