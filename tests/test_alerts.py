"""Unit tests for the alerts stream response parsing."""

from __future__ import annotations

import pytest

from tap_meteotrentino.streams import AlertsStream
from tap_meteotrentino.tap import TapMeteoTrentino

Payload = dict[str, object]


class _FakeResponse:
    """Minimal stand-in for ``requests.Response`` exposing only ``json()``."""

    def __init__(self, payload: Payload) -> None:
        self._payload = payload

    def json(self, **_: object) -> Payload:
        """Return the canned payload, ignoring decoder kwargs."""
        return self._payload


def _alert(identifier: str) -> Payload:
    return {
        "identifier": identifier,
        "sender": "dip.protezionecivile@provincia.tn.it",
        "sent": "09/09/2026 14:35:14",
        "msgType": ["Alert", "Alert"],
        "scope": ["Public", "Public"],
        "headline": "ALLERTA ORDINARIA (gialla) idrogeologica",
        "area": {"areaDesc": "Trentino", "polygon": "0,0 1,1"},
        "resource": {"uri": "https://example.invalid/allerta.pdf"},
    }


@pytest.fixture
def stream() -> AlertsStream:
    """Build an alerts stream on a tap with config validation disabled."""
    tap = TapMeteoTrentino(config={"streams": ["alerts"]}, validate_config=False)
    return AlertsStream(tap)


def _records(stream: AlertsStream, payload: Payload) -> list[dict]:
    rows = stream.parse_response(_FakeResponse(payload))  # type: ignore[arg-type]
    return [r for r in (stream.post_process(row) for row in rows) if r is not None]


def test_info_as_list_yields_one_record_per_alert(stream: AlertsStream) -> None:
    """Several active alerts arrive as a list under ``info``."""
    records = _records(stream, {"info": [_alert("4455"), _alert("4453")]})
    assert [r["identifier"] for r in records] == ["4455", "4453"]


def test_info_as_single_object_yields_one_record(stream: AlertsStream) -> None:
    """A single active alert arrives as a bare object under ``info``."""
    records = _records(stream, {"info": _alert("4455")})
    assert [r["identifier"] for r in records] == ["4455"]


def test_placeholder_without_identifier_yields_nothing(stream: AlertsStream) -> None:
    """No active alert: placeholder object, empty list, or missing key."""
    assert _records(stream, {"info": {"identifier": ""}}) == []
    assert _records(stream, {"info": []}) == []
    assert _records(stream, {}) == []


def test_post_process_flattens_and_normalises(stream: AlertsStream) -> None:
    """Nested area/resource are flattened and dates/arrays normalised."""
    (record,) = _records(stream, {"info": [_alert("4455")]})
    assert record["sent"] == "2026-09-09T14:35:14"
    assert record["area_desc"] == "Trentino"
    assert record["resource_uri"] == "https://example.invalid/allerta.pdf"
    assert record["msg_type"] == '["Alert", "Alert"]'
