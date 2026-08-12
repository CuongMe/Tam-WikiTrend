from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import time
from collections.abc import Iterable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds
from kafka import KafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from wikitrend.config import get_settings
from wikitrend.logging_utils import configure_logging

LOGGER = logging.getLogger("wikitrend.kafka_replay")
EVENT_SCHEMA_VERSION = 1
EVENT_ID_FIELDS = ("date", "hour", "source_project", "access_mode", "page_title")


def parse_args() -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        description="Deterministically replay Silver pageviews into Kafka."
    )
    parser.add_argument("--silver", type=Path, default=settings.silver_dir)
    parser.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--topic", default=settings.kafka_pageviews_topic)
    parser.add_argument("--max-events", type=int)
    parser.add_argument("--sleep-ms", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--delivery-timeout-seconds", type=int, default=60)
    return parser.parse_args()


def _json_default(value: Any) -> str:
    if isinstance(value, date | datetime):
        return value.isoformat()
    raise TypeError(f"Cannot serialize {type(value).__name__} to JSON")


def normalize_event(row: dict[str, Any]) -> dict[str, Any]:
    """Convert Arrow scalar values into a stable JSON-compatible event."""
    return json.loads(json.dumps(row, default=_json_default, sort_keys=True))


def deterministic_event_id(event: dict[str, Any]) -> str:
    """Identify one immutable source page-hour record across repeated replays."""
    missing = [field for field in EVENT_ID_FIELDS if event.get(field) is None]
    if missing:
        raise ValueError(f"Cannot identify event with missing fields: {missing}")
    identity = [event[field] for field in EVENT_ID_FIELDS]
    encoded = json.dumps(identity, ensure_ascii=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def event_envelope(row: dict[str, Any]) -> dict[str, Any]:
    event = normalize_event(row)
    return {
        "schema_version": EVENT_SCHEMA_VERSION,
        "event_id": deterministic_event_id(event),
        "emitted_at_utc": datetime.now(UTC).isoformat(),
        "event": event,
    }


def iter_silver_batches(dataset: ds.Dataset, batch_size: int) -> Iterable[Any]:
    """Yield physical Parquet batches in path order for reproducible replay."""
    fragments = sorted(dataset.get_fragments(), key=lambda fragment: fragment.path)
    for fragment in fragments:
        yield from fragment.to_batches(batch_size=batch_size)


def main() -> None:
    configure_logging()
    args = parse_args()
    dataset = ds.dataset(args.silver, format="parquet", partitioning="hive")
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=True, separators=(",", ":")
        ).encode("utf-8"),
        key_serializer=lambda value: value.encode("ascii"),
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
        linger_ms=50,
    )

    sent = 0
    pending = []
    try:
        for batch in iter_silver_batches(dataset, args.batch_size):
            for row in batch.to_pylist():
                envelope = event_envelope(row)
                pending.append(
                    producer.send(
                        args.topic,
                        key=envelope["event_id"],
                        value=envelope,
                    )
                )
                sent += 1
                if args.sleep_ms:
                    time.sleep(args.sleep_ms / 1000)
                if len(pending) >= args.batch_size:
                    for future in pending:
                        future.get(timeout=args.delivery_timeout_seconds)
                    pending.clear()
                if args.max_events and sent >= args.max_events:
                    break
            if args.max_events and sent >= args.max_events:
                break

        for future in pending:
            future.get(timeout=args.delivery_timeout_seconds)
        producer.flush(timeout=args.delivery_timeout_seconds)
        LOGGER.info("replay complete sent=%s topic=%s", sent, args.topic)
    finally:
        producer.close(timeout=args.delivery_timeout_seconds)


if __name__ == "__main__":
    main()
