from __future__ import annotations

from streaming.kafka_replay import deterministic_event_id, event_envelope


def _event() -> dict[str, object]:
    return {
        "date": "2026-08-01",
        "hour": 3,
        "source_project": "en.m",
        "project": "en",
        "access_mode": "mobile",
        "page_title": "Main_Page",
        "normalized_title": "Main Page",
        "view_count": 12,
    }


def test_event_id_is_stable_across_metric_changes() -> None:
    original = _event()
    corrected = {**original, "view_count": 13}
    assert deterministic_event_id(original) == deterministic_event_id(corrected)


def test_event_id_separates_access_modes_and_hours() -> None:
    original = _event()
    assert deterministic_event_id(original) != deterministic_event_id(
        {**original, "access_mode": "desktop"}
    )
    assert deterministic_event_id(original) != deterministic_event_id({**original, "hour": 4})


def test_envelope_has_version_and_payload() -> None:
    envelope = event_envelope(_event())
    assert envelope["schema_version"] == 1
    assert envelope["event_id"] == deterministic_event_id(_event())
    assert envelope["event"]["source_project"] == "en.m"
