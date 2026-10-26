"""Unit tests for forecast timestamp flattening."""

from __future__ import annotations

from tap_meteotrentino.streams import _parse_forecast_records


def _timestamps(data: dict, *, daily: bool) -> list[str]:
    records = _parse_forecast_records("L1", data, daily=daily)
    return sorted(r["forecast_timestamp"] for r in records)


def test_daily_steps_stay_on_utc_midnight_across_the_end_of_summer_time() -> None:
    """``start`` is a Europe/Rome wall clock; stepping it naively drifts an hour.

    The 2026-10-25 forecast started at 02:00 CEST (00:00Z). Adding 1440 minutes to
    the naive wall clock gave 02:00 on the 26th, which is 01:00Z in CET, while the
    next day's extraction said 01:00 (00:00Z): two instants for one day.
    """
    data = {
        "start": "2026-10-25T02:00:00",
        "1440": {"144000000": {}, "144001440": {}, "144002880": {}},
    }

    assert _timestamps(data, daily=True) == [
        "2026-10-25T00:00:00+00:00",
        "2026-10-26T00:00:00+00:00",
        "2026-10-27T00:00:00+00:00",
    ]


def test_daily_steps_stay_on_utc_midnight_across_the_start_of_summer_time() -> None:
    """The spring change drifts the other way when the wall clock is stepped."""
    data = {
        "start": "2027-03-27T01:00:00",
        "1440": {"144000000": {}, "144001440": {}},
    }

    assert _timestamps(data, daily=True) == [
        "2027-03-27T00:00:00+00:00",
        "2027-03-28T00:00:00+00:00",
    ]


def test_sub_daily_steps_are_elapsed_time_not_wall_clock() -> None:
    """A 180-minute step is three elapsed hours, emitted with an offset."""
    data = {
        "start": "2026-10-25T02:00:00",
        "180": {"18000000": {}, "18000180": {}},
    }

    assert _timestamps(data, daily=False) == [
        "2026-10-25T00:00:00+00:00",
        "2026-10-25T03:00:00+00:00",
    ]


def test_an_offset_in_start_is_authoritative() -> None:
    """If the API ever sends an offset, it wins over the Europe/Rome assumption."""
    data = {
        "start": "2026-10-26T01:00:00+01:00",
        "1440": {"144000000": {}},
    }

    assert _timestamps(data, daily=True) == ["2026-10-26T00:00:00+00:00"]
