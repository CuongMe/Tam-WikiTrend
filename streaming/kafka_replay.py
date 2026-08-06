from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import pyarrow.dataset as ds
from kafka import KafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from wikitrend.config import get_settings
from wikitrend.logging_utils import configure_logging

LOGGER = logging.getLogger("wikitrend.kafka_replay")


def parse_args() -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Replay Silver pageviews into Kafka.")
    parser.add_argument("--silver", type=Path, default=settings.silver_dir)
    parser.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--topic", default=settings.kafka_pageviews_topic)
    parser.add_argument("--max-events", type=int)
    parser.add_argument("--sleep-ms", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=5000)
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    dataset = ds.dataset(args.silver, format="parquet", partitioning="hive")
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        linger_ms=50,
    )

    sent = 0
    scanner = dataset.scanner(batch_size=args.batch_size)
    for batch in scanner.to_batches():
        rows = batch.to_pylist()
        for row in rows:
            producer.send(args.topic, key=row["page_title"], value=row)
            sent += 1
            if args.sleep_ms:
                time.sleep(args.sleep_ms / 1000)
            if args.max_events and sent >= args.max_events:
                producer.flush()
                LOGGER.info("replay complete sent=%s", sent)
                return
    producer.flush()
    LOGGER.info("replay complete sent=%s", sent)


if __name__ == "__main__":
    main()
