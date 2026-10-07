-- Latest raw events
SELECT * FROM `<your-project-id>.streaming_demo.events`
ORDER BY ingested_at DESC LIMIT 50;

-- Per-minute aggregates
SELECT window_start, event_type, event_count, total_amount
FROM `<your-project-id>.streaming_demo.events_agg_1min`
ORDER BY window_start DESC, event_type LIMIT 50;

-- End-to-end latency (event time -> BigQuery), last 10 minutes
SELECT AVG(TIMESTAMP_DIFF(ingested_at, event_ts, MILLISECOND)) AS avg_latency_ms,
       APPROX_QUANTILES(TIMESTAMP_DIFF(ingested_at, event_ts, MILLISECOND), 100)[OFFSET(95)] AS p95_ms
FROM `<your-project-id>.streaming_demo.events`
WHERE ingested_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE);

-- Rejected messages
SELECT * FROM `<your-project-id>.streaming_demo.events_dead_letter`
ORDER BY ingested_at DESC LIMIT 20;
