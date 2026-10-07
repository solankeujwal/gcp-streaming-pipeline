"""Simulates an event stream into Pub/Sub (~3% intentionally malformed to exercise dead-lettering)."""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from google.cloud import pubsub_v1

EVENT_TYPES = ["page_view"] * 6 + ["add_to_cart"] * 3 + ["purchase"] * 2 + ["refund"]
PRODUCTS = ["P100", "P200", "P300", "P400", "P500"]


def make_event():
    etype = random.choice(EVENT_TYPES)
    amount = round(random.uniform(199, 4999), 2) if etype in ("purchase", "refund") else 0.0
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 500)}",
        "event_type": etype,
        "product_id": random.choice(PRODUCTS),
        "amount": amount,
        "currency": "INR",
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--rate", type=float, default=10, help="events per second")
    ap.add_argument("--duration", type=int, default=300, help="seconds to run")
    args = ap.parse_args()

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project, args.topic)
    end, sent = time.time() + args.duration, 0

    while time.time() < end:
        evt = make_event()
        r = random.random()
        if r < 0.015:
            payload = b"this is not json"
        elif r < 0.03:
            evt.pop("user_id")
            payload = json.dumps(evt).encode()
        else:
            payload = json.dumps(evt).encode()
        publisher.publish(topic_path, payload)
        sent += 1
        if sent % 100 == 0:
            print(f"published {sent} messages")
        time.sleep(1.0 / args.rate)
    print(f"Done. Published {sent} messages.")


if __name__ == "__main__":
    main()
