"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_tap_test_class

from tap_meteotrentino.tap import TapMeteoTrentino

SAMPLE_CONFIG = {
    "start_date": (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
    ).strftime("%Y-%m-%dT%H:%M:%SZ"),
    # Limit scope so tests run fast: one well-known station, one location
    "station_codes": ["T0405"],
    "location_ids": ["022409ae-b6f4-430a-90c4-e1a07423f88b"],
    # Only test lightweight streams in the standard SDK test suite
    "streams": ["sky_conditions", "alerts", "meteo_stations"],
}


# Run standard built-in tap tests from the SDK:
TestTapMeteoTrentino = get_tap_test_class(
    tap_class=TapMeteoTrentino,
    config=SAMPLE_CONFIG,
)
