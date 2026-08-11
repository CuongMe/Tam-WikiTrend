from __future__ import annotations

import json
import uuid

import pytest
from kafka import KafkaConsumer, KafkaProducer

from streaming.kafka_replay import event_envelope

pytestmark = [pytest.mark.integration, pytest.mark.kafka_integration]


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
    producer = KafkaProducer(
        bootstrap_servers="localhost:9094",
        key_serializer=str.encode,
        value_serializer=lambda value: json.dumps(value).encode(),
        acks="all",
    )
    producer.send(topic, key=envelope["event_id"], value=envelope).get(timeout=30)
    producer.close()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9094",
        auto_offset_reset="earliest",
        consumer_timeout_ms=30000,
        value_deserializer=lambda value: json.loads(value.decode()),
    )
    received = next(iter(consumer)).value
    consumer.close()
    assert received["event_id"] == envelope["event_id"]
    assert received["event"]["access_mode"] == "desktop"
