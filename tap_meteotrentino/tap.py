"""MeteoTrentino tap class."""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_meteotrentino import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapMeteoTrentino(Tap):
    """Singer tap for MeteoTrentino open-data APIs."""

    name = "tap-meteotrentino"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description=(
                "Earliest observation timestamp to sync for station_observations. "
                "Defaults to the API's available window (~48 hours)."
            ),
        ),
        th.Property(
            "station_codes",
            th.ArrayType(th.StringType(nullable=False), nullable=True),
            description=(
                "Optional list of station codes to sync (e.g. ['T0405', 'T0153']). "
                "When omitted, all active stations are synced."
            ),
        ),
        th.Property(
            "location_ids",
            th.ArrayType(th.StringType(nullable=False), nullable=True),
            description=(
                "Optional list of forecast location UUIDs to sync. "
                "When omitted, all Trentino locations are synced."
            ),
        ),
        th.Property(
            "streams",
            th.ArrayType(th.StringType(nullable=False), nullable=True),
            description=(
                "Optional list of stream names to enable. "
                "Valid values: sky_conditions, alerts, meteo_stations, "
                "station_observations, forecast_locations, forecasts_hourly, "
                "forecasts_daily. When omitted, all streams are enabled."
            ),
        ),
        th.Property(
            "table_name_prefix",
            th.StringType(nullable=True),
            default="meteotrentino_",
            description=(
                "Prefix prepended to every stream (table) name. "
                "Defaults to 'meteotrentino_'. Set to '' to disable."
            ),
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list:
        """Return a list of discovered streams."""
        enabled: list[str] | None = self.config.get("streams")
        prefix: str = self.config.get("table_name_prefix", "meteotrentino_") or ""

        all_streams = [
            streams.SkyConditionsStream(self),
            streams.AlertsStream(self),
            streams.MeteoStationsStream(self),
            streams.StationObservationsStream(self),
            streams.ForecastLocationsStream(self),
            streams.ForecastsHourlyStream(self),
            streams.ForecastsDailyStream(self),
        ]

        if enabled:
            all_streams = [s for s in all_streams if s.name in enabled]

        if prefix:
            for stream in all_streams:
                stream.name = f"{prefix}{stream.name}"

        return all_streams


if __name__ == "__main__":
    TapMeteoTrentino.cli()
