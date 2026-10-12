"""Unit tests for the station observations stream XML parsing."""

from __future__ import annotations

from pathlib import Path

import pytest
import requests

from tap_meteotrentino.streams import StationObservationsStream
from tap_meteotrentino.tap import TapMeteoTrentino

FIXTURES = Path(__file__).parent / "fixtures"


def _response(content: bytes) -> requests.Response:
    """Build a real ``requests.Response`` carrying ``content`` as its body."""
    response = requests.Response()
    response.status_code = 200
    response._content = content  # noqa: SLF001
    return response


@pytest.fixture
def stream() -> StationObservationsStream:
    """Build an observations stream on a tap with config validation disabled."""
    tap = TapMeteoTrentino(
        config={"streams": ["meteo_stations", "station_observations"]},
        validate_config=False,
    )
    return StationObservationsStream(tap)


def _records(stream: StationObservationsStream, content: bytes) -> list[dict]:
    stream._current_station_code = "T0129"  # noqa: SLF001
    return list(stream.parse_response(_response(content)))


def test_wind10m_readings_fill_wind_fields(stream: StationObservationsStream) -> None:
    """The live API reports wind as ``wind10m`` with speed, gust and direction."""
    content = (FIXTURES / "last_data_T0129.xml").read_bytes()
    records = _records(stream, content)

    assert [r["timestamp"] for r in records] == [
        "2026-10-07T00:00:00+01:00",
        "2026-10-07T00:15:00+01:00",
    ]
    first = records[0]
    assert first["wind_speed_ms"] == 2.3
    assert first["wind_gust_ms"] == 4.0
    assert first["wind_direction_deg"] == 67.0
    assert records[1]["wind_direction_deg"] == 79.0


def test_other_measurements_still_parsed(stream: StationObservationsStream) -> None:
    """Fixing wind must not disturb the simple measurement lists."""
    content = (FIXTURES / "last_data_T0129.xml").read_bytes()
    first = _records(stream, content)[0]

    assert first["station_code"] == "T0129"
    assert first["air_temperature_c"] == 13.6
    assert first["precipitation_mm"] == 0.0
    assert first["relative_humidity_pct"] == 66.0


def test_station_without_anemometer_has_no_wind(
    stream: StationObservationsStream,
) -> None:
    """Stations without an anemometer send an empty ``wind_list``."""
    content = b"""<?xml version="1.0" encoding="utf-8"?>
<lastData xmlns="http://www.meteotrentino.it/">
  <temperature_list>
    <air_temperature UM="C"><date>2026-10-07T00:00:00+01</date><value>9.1</value></air_temperature>
  </temperature_list>
  <wind_list />
</lastData>"""
    (record,) = _records(stream, content)

    assert record["air_temperature_c"] == 9.1
    assert record.get("wind_speed_ms") is None
    assert record.get("wind_gust_ms") is None
