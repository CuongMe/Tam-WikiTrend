from __future__ import annotations

import json
import time
import uuid

import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from streaming.kafka_replay import event_envelope

pytestmark = [pytest.mark.integration, pytest.mark.kafka_integration]
BOOTSTRAP_SERVERS = "localhost:9094"


def build_ready_producer(timeout_seconds: int = 60) -> KafkaProducer:
    """Create a producer after the broker listener is ready for API negotiation."""
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                key_serializer=str.encode,
                value_serializer=lambda value: json.dumps(value).encode(),
                acks="all",
                request_timeout_ms=10000,
                max_block_ms=10000,
            )
        except (KafkaError, OSError, ValueError) as exc:
            last_error = exc
            time.sleep(2)
    raise AssertionError("Kafka broker did not become ready for producer traffic") from last_error


def test_event_envelope_round_trips_through_kafka() -> None:
    topic = f"wikitrend-contract-{uuid.uuid4().hex}"
    event = {
        "date": "2026-08-01",
        "hour": 0,
        "source_project": "en",
        "project": "en",
        "language": "en",
        "project_family": "wikipedia",
        "access_mode": "desktop",
        "page_title": "Main_Page",
        "normalized_title": "Main Page",
        "normalization_status": "normalized",
        "view_count": 10,
        "response_size": 100,
        "source_file": "pageviews-20260801-000000.gz",
    }
    envelope = event_envelope(event)
    producer = build_ready_producer()
    producer.send(topic, key=envelope["event_id"], value=envelope).get(timeout=30)
    producer.close()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        consumer_timeout_ms=30000,
        value_deserializer=lambda value: json.loads(value.decode()),
    )
    received = next(iter(consumer)).value
    consumer.close()
    assert received["event_id"] == envelope["event_id"]
    assert received["event"]["access_mode"] == "desktop"
