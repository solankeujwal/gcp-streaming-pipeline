"""Streaming pipeline: Pub/Sub -> Dataflow (Apache Beam) -> BigQuery.

Branches:
  1. Valid events        -> <dataset>.events            (raw, partitioned)
  2. 1-minute aggregates -> <dataset>.events_agg_1min   (count + revenue per event_type)
  3. Invalid messages    -> <dataset>.events_dead_letter
"""
import argparse
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows

REQUIRED_FIELDS = ("event_id", "user_id", "event_type", "event_ts")
VALID_EVENT_TYPES = {"page_view", "add_to_cart", "purchase", "refund"}

EVENTS_SCHEMA = (
    "event_id:STRING,user_id:STRING,event_type:STRING,product_id:STRING,"
    "amount:FLOAT,currency:STRING,event_ts:TIMESTAMP,ingested_at:TIMESTAMP"
)
AGG_SCHEMA = (
    "window_start:TIMESTAMP,window_end:TIMESTAMP,event_type:STRING,"
    "event_count:INTEGER,total_amount:FLOAT"
)
DEAD_LETTER_SCHEMA = "raw_payload:STRING,error:STRING,ingested_at:TIMESTAMP"


class ParseAndValidate(beam.DoFn):
    DEAD_LETTER = "dead_letter"

    def process(self, element):
        now = datetime.now(timezone.utc).isoformat()
        try:
            e = json.loads(element.decode("utf-8"))
            missing = [f for f in REQUIRED_FIELDS if f not in e]
            if missing:
                raise ValueError(f"missing fields: {missing}")
            if e["event_type"] not in VALID_EVENT_TYPES:
                raise ValueError(f"unknown event_type: {e['event_type']}")
            amount = float(e.get("amount") or 0.0)
            if amount < 0:
                raise ValueError("negative amount")
            event_ts = datetime.fromisoformat(str(e["event_ts"]).replace("Z", "+00:00"))
            yield {
                "event_id": str(e["event_id"]),
                "user_id": str(e["user_id"]),
                "event_type": e["event_type"],
                "product_id": e.get("product_id"),
                "amount": amount,
                "currency": e.get("currency", "INR"),
                "event_ts": event_ts.isoformat(),
                "ingested_at": now,
            }
        except Exception as ex:  # noqa: BLE001 - any bad record goes to dead letter
            yield beam.pvalue.TaggedOutput(
                self.DEAD_LETTER,
                {
                    "raw_payload": element.decode("utf-8", errors="replace"),
                    "error": str(ex),
                    "ingested_at": now,
                },
            )


class CountAndSum(beam.CombineFn):
    def create_accumulator(self):
        return (0, 0.0)

    def add_input(self, acc, value):
        return (acc[0] + 1, acc[1] + value)

    def merge_accumulators(self, accumulators):
        count, total = 0, 0.0
        for c, t in accumulators:
            count += c
            total += t
        return (count, total)

    def extract_output(self, acc):
        return acc


class FormatAggregate(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        event_type, (count, total) = element
        yield {
            "window_start": window.start.to_utc_datetime().isoformat(),
            "window_end": window.end.to_utc_datetime().isoformat(),
            "event_type": event_type,
            "event_count": count,
            "total_amount": round(total, 2),
        }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--subscription", required=True, help="projects/<p>/subscriptions/<s>")
    parser.add_argument("--dataset", required=True, help="<project>:<dataset>")
    parser.add_argument("--window_seconds", type=int, default=60)
    args, beam_args = parser.parse_known_args(argv)

    options = PipelineOptions(beam_args)
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True

    def table(name):
        return f"{args.dataset}.{name}"

    def bq_write(name, schema):
        return WriteToBigQuery(
            table=table(name),
            schema=schema,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
        )

    with beam.Pipeline(options=options) as p:
        parsed = (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=args.subscription)
            | "Parse" >> beam.ParDo(ParseAndValidate()).with_outputs(
                ParseAndValidate.DEAD_LETTER, main="valid"
            )
        )

        parsed.valid | "WriteEvents" >> bq_write("events", EVENTS_SCHEMA)
        parsed[ParseAndValidate.DEAD_LETTER] | "WriteDeadLetter" >> bq_write(
            "events_dead_letter", DEAD_LETTER_SCHEMA
        )

        (
            parsed.valid
            | "KeyByType" >> beam.Map(lambda r: (r["event_type"], r["amount"]))
            | "Window" >> beam.WindowInto(FixedWindows(args.window_seconds))
            | "Aggregate" >> beam.CombinePerKey(CountAndSum())
            | "FormatAgg" >> beam.ParDo(FormatAggregate())
            | "WriteAgg" >> bq_write("events_agg_1min", AGG_SCHEMA)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
