"""Forward live Wikipedia change events from Wikimedia EventStreams to Kafka.

The Wikimedia "recentchange" stream is a Server-Sent Events (SSE) feed:
every edit, page creation or log action on any Wikimedia wiki is pushed as
one JSON document. This script reads that feed and publishes each event,
unchanged, to a Kafka topic. Filtering is left to the Spark streaming
pipeline, so the raw topic can be replayed with other filters.

Ethics and scraping policy: Wikimedia asks automated clients to send a
descriptive User-Agent with a contact point, and to resume a dropped
connection with the Last-Event-ID header instead of hammering the server.
Both are implemented below.

Usage (inside the imdb_spark container):
    python -m src.streaming.wikipedia_producer --duration 1800
"""
import argparse
import json
import os
import time

import requests
from kafka import KafkaProducer

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = (
    "ECE-BigData-IMDb-Project/1.0 "
    "(https://github.com/Alexandre-50/Big_Data_IMDB_Project; "
    "student project) python-requests"
)
DEFAULT_TOPIC = "wikipedia-changes"
DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
RECONNECT_DELAY_SECONDS = 5
REPORT_EVERY = 1000


def parse_args():
    """Read the command-line options."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument(
        "--duration", type=int, default=0,
        help="Stop after this many seconds (0 = run until Ctrl+C).",
    )
    parser.add_argument(
        "--max-events", type=int, default=0,
        help="Stop after this many events (0 = no limit).",
    )
    return parser.parse_args()


def read_sse(response):
    """Yield (event_id, data) pairs from a streaming SSE HTTP response.

    An SSE message is a block of "field: value" lines ended by a blank
    line. Only the "id" and "data" fields are needed here; comment lines
    (starting with ":") are keep-alive pings and are ignored.
    """
    event_id, data_lines = None, []
    for raw_line in response.iter_lines(decode_unicode=True):
        if raw_line is None:
            continue
        if raw_line == "":
            if data_lines:
                yield event_id, "\n".join(data_lines)
            event_id, data_lines = None, []
            continue
        if raw_line.startswith(":"):
            continue
        field, _, value = raw_line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if field == "id":
            event_id = value
        elif field == "data":
            data_lines.append(value)


def create_producer(bootstrap):
    """Create a Kafka producer (keys and values are sent as UTF-8 bytes)."""
    return KafkaProducer(bootstrap_servers=bootstrap, linger_ms=50, acks=1)


def run(args):
    """Stream events from Wikimedia to Kafka until a stop condition."""
    producer = create_producer(args.bootstrap)
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    deadline = time.time() + args.duration if args.duration else None
    last_event_id, sent, skipped = None, 0, 0
    print(f"Sending {STREAM_URL} to topic '{args.topic}' "
          f"on {args.bootstrap}", flush=True)

    while True:
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id
        try:
            with requests.get(STREAM_URL, headers=headers, stream=True,
                              timeout=(10, 60)) as response:
                response.raise_for_status()
                for event_id, data in read_sse(response):
                    last_event_id = event_id or last_event_id
                    try:
                        event = json.loads(data)
                    except json.JSONDecodeError:
                        skipped += 1
                        continue
                    # The wiki name as key keeps one wiki in one partition.
                    wiki = event.get("wiki") or ""
                    producer.send(args.topic, key=wiki.encode("utf-8"),
                                  value=data.encode("utf-8"))
                    sent += 1
                    if sent % REPORT_EVERY == 0:
                        print(f"{sent} events sent ({skipped} skipped)",
                              flush=True)
                    if args.max_events and sent >= args.max_events:
                        return sent
                    if deadline and time.time() >= deadline:
                        return sent
        except (requests.RequestException, ValueError) as error:
            print(f"Connection lost ({error}), retrying in "
                  f"{RECONNECT_DELAY_SECONDS}s", flush=True)
            time.sleep(RECONNECT_DELAY_SECONDS)
        finally:
            producer.flush()


def main():
    """Entry point: run the producer and print the final count."""
    args = parse_args()
    try:
        sent = run(args)
    except KeyboardInterrupt:
        sent = None
    print(f"Producer stopped ({sent if sent is not None else 'interrupted'})",
          flush=True)


if __name__ == "__main__":
    main()
