"""Stream type classes for tap-meteotrentino."""

from __future__ import annotations

import re
import sys
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar
from xml.etree import ElementTree

import requests as http_lib
from singer_sdk import Stream
from singer_sdk import typing as th

from tap_meteotrentino.client import MT_NSMAP, MT_NS, MeteoTrentinoStream, MeteoTrentinoXMLStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context


# ── Helpers ──────────────────────────────────────────────────────────────────


def _normalize_tz(ts: str) -> str:
    """Append ':00' to bare UTC-offset if missing (e.g. '+01' → '+01:00')."""
    return re.sub(r"([+-]\d{2})$", r"\1:00", ts)


def _parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


# ── Reference: Sky Conditions ─────────────────────────────────────────────────


class SkyConditionsStream(MeteoTrentinoStream):
    """Sky condition codes reference table (static)."""

    name = "sky_conditions"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = None

    @property
    @override
    def url_base(self) -> str:
        return "https://manager.meteo.report"

    @property
    @override
    def path(self) -> str:
        return "/api/sky_conditions/"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("icon_day", th.StringType),
        th.Property("icon_night", th.StringType),
        th.Property("name_deu", th.StringType),
        th.Property("name_eng", th.StringType),
        th.Property("name_ita", th.StringType),
        th.Property("name_lld", th.StringType),
    ).to_dict()

    @override
    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        # Drop large embedded SVG content, keep only metadata
        row.pop("svg_day", None)
        row.pop("svg_night", None)
        return row


# ── Alerts ────────────────────────────────────────────────────────────────────


class AlertsStream(MeteoTrentinoStream):
    """Active weather alert (full refresh – single-object endpoint)."""

    name = "alerts"
    primary_keys: ClassVar[list[str]] = ["identifier"]
    replication_key = None
    records_jsonpath = "$.info"  # unwrap the single info object

    @property
    @override
    def url_base(self) -> str:
        return "https://www.meteotrentino.it"

    @property
    @override
    def path(self) -> str:
        return "/wp-content/uploads/jsonfiles/allerte.json"

    schema = th.PropertiesList(
        th.Property("identifier", th.StringType, required=True),
        th.Property("sender", th.StringType),
        th.Property("sent", th.StringType),
        th.Property("msg_type", th.ArrayType(th.StringType)),
        th.Property("scope", th.ArrayType(th.StringType)),
        th.Property("source", th.StringType),
        th.Property("status", th.StringType),
        th.Property("language", th.StringType),
        th.Property("headline", th.StringType),
        th.Property("category", th.StringType),
        th.Property("event", th.StringType),
        th.Property("urgency", th.StringType),
        th.Property("severity", th.StringType),
        th.Property("certainty", th.StringType),
        th.Property("expires", th.StringType),
        th.Property("description", th.StringType),
        th.Property("web", th.StringType),
        th.Property("contact", th.StringType),
        th.Property("area_desc", th.StringType),
        th.Property("area_polygon", th.StringType),
        th.Property("resource_uri", th.StringType),
    ).to_dict()

    @override
    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        # Flatten nested area and resource objects
        area = row.pop("area", {}) or {}
        row["area_desc"] = area.get("areaDesc")
        row["area_polygon"] = area.get("polygon")

        resource = row.pop("resource", {}) or {}
        row["resource_uri"] = resource.get("uri")

        # Rename msgType → msg_type
        row["msg_type"] = row.pop("msgType", row.get("msg_type"))

        # Normalise 'sent' from Italian "DD/MM/YYYY HH:MM:SS" to ISO 8601
        sent = row.get("sent")
        if sent and "/" in sent:
            try:
                dt = datetime.strptime(sent, "%d/%m/%Y %H:%M:%S")
                row["sent"] = dt.strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                pass

        return row


# ── Meteo Stations ────────────────────────────────────────────────────────────


class MeteoStationsStream(MeteoTrentinoXMLStream):
    """Registry of all MeteoTrentino measurement stations (full refresh)."""

    name = "meteo_stations"
    primary_keys: ClassVar[list[str]] = ["code"]
    replication_key = None

    @property
    @override
    def url_base(self) -> str:
        return "https://dati.meteotrentino.it"

    @property
    @override
    def path(self) -> str:
        return "/service.asmx/getListOfMeteoStations"

    schema = th.PropertiesList(
        th.Property("code", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("shortname", th.StringType),
        th.Property("elevation", th.IntegerType),
        th.Property("latitude", th.NumberType),
        th.Property("longitude", th.NumberType),
        th.Property("east", th.NumberType),
        th.Property("north", th.NumberType),
        th.Property("startdate", th.StringType),
        th.Property("enddate", th.StringType),
    ).to_dict()

    @override
    def _parse_xml(self, root: ElementTree.Element) -> Iterable[dict]:
        station_codes: list[str] = self.config.get("station_codes") or []

        for elem in root.findall("mt:pointOfMeasureInfo", MT_NSMAP):

            def txt(tag: str) -> str | None:
                return elem.findtext(f"mt:{tag}", None, MT_NSMAP)

            code = txt("code") or ""
            if station_codes and code not in station_codes:
                continue

            elevation = txt("elevation")
            latitude = txt("latitude")
            longitude = txt("longitude")
            east = txt("east")
            north = txt("north")

            yield {
                "code": code,
                "name": txt("name"),
                "shortname": txt("shortname"),
                "elevation": int(elevation) if elevation else None,
                "latitude": _parse_float(latitude),
                "longitude": _parse_float(longitude),
                "east": _parse_float(east),
                "north": _parse_float(north),
                "startdate": txt("startdate"),
                "enddate": txt("enddate") or None,
            }

    @override
    def get_child_context(
        self,
        record: dict,
        context: Context | None,
    ) -> dict:
        return {"station_code": record["code"]}


# ── Station Observations ──────────────────────────────────────────────────────


class StationObservationsStream(MeteoTrentinoXMLStream):
    """15-minute meteorological observations per station (incremental)."""

    name = "station_observations"
    primary_keys: ClassVar[list[str]] = ["station_code", "timestamp"]
    replication_key = "timestamp"
    is_sorted = True
    parent_stream_type = MeteoStationsStream

    @property
    @override
    def url_base(self) -> str:
        return "https://dati.meteotrentino.it"

    @property
    @override
    def path(self) -> str:
        return "/service.asmx/getLastDataOfMeteoStation"

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        return {"codice": (context or {}).get("station_code", "")}

    schema = th.PropertiesList(
        th.Property("station_code", th.StringType, required=True),
        th.Property("timestamp", th.StringType, required=True),
        th.Property("air_temperature_c", th.NumberType),
        th.Property("precipitation_mm", th.NumberType),
        th.Property("wind_speed_ms", th.NumberType),
        th.Property("wind_direction_deg", th.NumberType),
        th.Property("wind_gust_ms", th.NumberType),
        th.Property("global_radiation_wm2", th.NumberType),
        th.Property("relative_humidity_pct", th.NumberType),
        th.Property("snow_depth_cm", th.NumberType),
    ).to_dict()

    @override
    def _parse_xml(self, root: ElementTree.Element) -> Iterable[dict]:
        # We need context (station_code) but _parse_xml doesn't receive it.
        # Store it temporarily via parse_response override.
        raise NotImplementedError("Use _parse_observations instead.")

    @override
    def parse_response(self, response: http_lib.Response) -> Iterable[dict]:
        # Context is not directly available here; we read it from _current_context
        station_code: str = getattr(self, "_current_station_code", "")
        root = ElementTree.fromstring(response.content)
        yield from self._parse_observations(root, station_code)

    def _parse_observations(
        self,
        root: ElementTree.Element,
        station_code: str,
    ) -> Iterable[dict]:
        """Merge all measurement lists into one record per timestamp."""
        measurements: dict[str, dict[str, float | None]] = {}

        def add(ts: str, field: str, val: float | None) -> None:
            measurements.setdefault(ts, {})[field] = val

        def parse_simple_list(
            list_tag: str,
            item_tag: str,
            field: str,
        ) -> None:
            for elem in root.findall(f"mt:{list_tag}/mt:{item_tag}", MT_NSMAP):
                date = elem.findtext("mt:date", None, MT_NSMAP)
                value = elem.findtext("mt:value", None, MT_NSMAP)
                if date:
                    ts = _normalize_tz(date)
                    add(ts, field, _parse_float(value))

        parse_simple_list("temperature_list", "air_temperature", "air_temperature_c")
        parse_simple_list("precipitation_list", "precipitation", "precipitation_mm")
        parse_simple_list("global_radiation_list", "global_radiation", "global_radiation_wm2")
        parse_simple_list("relative_humidity_list", "relative_humidity", "relative_humidity_pct")
        parse_simple_list("snow_depth_list", "snow_depth", "snow_depth_cm")

        # Wind may carry sub-elements (speed/direction/gust) or a single value
        for elem in root.findall("mt:wind_list/mt:wind", MT_NSMAP):
            date = elem.findtext("mt:date", None, MT_NSMAP)
            if not date:
                continue
            ts = _normalize_tz(date)
            speed = elem.findtext("mt:speed", None, MT_NSMAP)
            direction = elem.findtext("mt:direction", None, MT_NSMAP)
            gust = elem.findtext("mt:gust", None, MT_NSMAP)
            value = elem.findtext("mt:value", None, MT_NSMAP)
            add(ts, "wind_speed_ms", _parse_float(speed or value))
            add(ts, "wind_direction_deg", _parse_float(direction))
            add(ts, "wind_gust_ms", _parse_float(gust))

        starting_value = self.get_starting_replication_key_value(
            {"station_code": station_code}
        )

        for ts in sorted(measurements):
            if starting_value and ts <= starting_value:
                continue
            yield {
                "station_code": station_code,
                "timestamp": ts,
                **measurements[ts],
            }

    @override
    def request_records(self, context: Context | None) -> Iterable[dict]:
        # Stash station code so parse_response can access it
        self._current_station_code: str = (context or {}).get("station_code", "")
        yield from super().request_records(context)


# ── Forecast Locations ────────────────────────────────────────────────────────


class ForecastLocationsStream(MeteoTrentinoStream):
    """Catalogue of forecast locations across Trentino (full refresh)."""

    name = "forecast_locations"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = None

    @property
    @override
    def url_base(self) -> str:
        return "https://meteo.report"

    @property
    @override
    def path(self) -> str:
        return "/open_data/forecasts/trentino.json"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("name_it", th.StringType),
        th.Property("name_de", th.StringType),
        th.Property("name_en", th.StringType),
        th.Property("name_lld", th.StringType),
        th.Property("elevation", th.IntegerType),
        th.Property("latitude", th.NumberType),
        th.Property("longitude", th.NumberType),
        th.Property("venue_type", th.IntegerType),
        th.Property("forecast_url", th.StringType),
    ).to_dict()

    @override
    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        location_ids: list[str] = self.config.get("location_ids") or []
        if location_ids and row.get("id") not in location_ids:
            return None

        # Normalise flat vs nested name format
        name = row.pop("name", None)
        if isinstance(name, dict):
            row["name_it"] = name.get("it") or name.get("ita")
            row["name_de"] = name.get("de") or name.get("deu")
            row["name_en"] = name.get("en") or name.get("eng")
            row["name_lld"] = name.get("ldin") or name.get("lld")
        else:
            row.setdefault("name_it", row.pop("name_ita", None))
            row.setdefault("name_de", row.pop("name_deu", None))
            row.setdefault("name_en", row.pop("name_eng", None))

        # Normalise coordinate field names
        row["latitude"] = row.pop("lat", None) or row.get("latitude")
        row["longitude"] = row.pop("lon", None) or row.get("longitude")

        # Normalise venue_type — API may return it as string
        vt = row.pop("id_venue_type", None) or row.get("venue_type")
        try:
            row["venue_type"] = int(vt) if vt is not None else None
        except (ValueError, TypeError):
            row["venue_type"] = None

        # forecast_url
        row["forecast_url"] = row.pop("url", None) or row.get("forecast_url")

        return row

    @override
    def get_child_context(
        self,
        record: dict,
        context: Context | None,
    ) -> dict:
        return {
            "location_id": record["id"],
            "forecast_url": record["forecast_url"],
        }


# ── Forecast helpers ──────────────────────────────────────────────────────────

# Common forecast schema fields (shared by hourly and daily streams).
# forecast_url is injected automatically by Singer SDK from child context.
_FORECAST_FIELDS = th.PropertiesList(
    th.Property("location_id", th.StringType, required=True),
    th.Property("forecast_timestamp", th.StringType, required=True),
    th.Property("forecast_url", th.StringType),       # injected from parent context
    th.Property("interval_minutes", th.IntegerType),
    th.Property("temperature", th.NumberType),
    th.Property("temperature_maximum", th.NumberType), # present in daily intervals
    th.Property("temperature_minimum", th.NumberType), # present in daily intervals
    th.Property("rain_fall", th.NumberType),
    th.Property("fresh_snow", th.NumberType),
    th.Property("snow_level", th.IntegerType),
    th.Property("wind_speed", th.NumberType),
    th.Property("wind_gust", th.NumberType),
    th.Property("wind_direction", th.IntegerType),
    th.Property("sky_condition", th.StringType),
    th.Property("freezing_level", th.IntegerType),
    th.Property("rain_probability", th.IntegerType),
    th.Property("sunshine_duration", th.NumberType),
)


def _parse_forecast_records(
    location_id: str,
    data: dict,
    *,
    daily: bool,
) -> Iterable[dict]:
    """Yield normalised forecast records from a per-location forecast JSON.

    The JSON structure is::

        {
          "start": "<ISO datetime>",
          "end":   "<ISO datetime>",
          "<interval_minutes>": {          # e.g. "180" (3-hour) or "1440" (daily)
            "<base_offset + n*interval>": { …weather fields… },
            ...
          },
          ...
        }

    The inner key encodes the absolute position; the offset from the minimum
    inner key gives the number of minutes elapsed since *start*.
    """
    start_str = data.get("start", "")
    try:
        start_dt = datetime.fromisoformat(start_str)
    except ValueError:
        return

    for interval_key, interval_data in data.items():
        if not isinstance(interval_data, dict):
            continue
        try:
            interval_min = int(interval_key)
        except ValueError:
            continue

        if daily and interval_min < 1440:
            continue
        if not daily and interval_min >= 1440:
            continue

        inner_keys = sorted(int(k) for k in interval_data)
        if not inner_keys:
            continue
        base_key = inner_keys[0]

        for key_str, forecast in interval_data.items():
            offset_min = int(key_str) - base_key
            forecast_ts = start_dt + timedelta(minutes=offset_min)

            yield {
                "location_id": location_id,
                "forecast_timestamp": forecast_ts.isoformat(),
                "interval_minutes": interval_min,
                **forecast,
            }


# ── Hourly Forecasts ──────────────────────────────────────────────────────────


class ForecastsHourlyStream(Stream):
    """Sub-daily (typically 3-hour) forecast per location (full refresh).

    Inherits from ``Stream`` (not ``RESTStream``) because the URL is dynamic
    per location and provided via parent context.
    """

    name = "forecasts_hourly"
    primary_keys: ClassVar[list[str]] = ["location_id", "forecast_timestamp"]
    replication_key = None
    parent_stream_type = ForecastLocationsStream

    schema = _FORECAST_FIELDS.to_dict()

    @override
    def get_records(self, context: Context | None) -> Iterable[dict]:
        if not context:
            return
        url: str = context["forecast_url"]
        response = http_lib.get(url, timeout=30)
        response.raise_for_status()
        yield from _parse_forecast_records(
            context["location_id"],
            response.json(),
            daily=False,
        )


# ── Daily Forecasts ───────────────────────────────────────────────────────────


class ForecastsDailyStream(Stream):
    """Daily forecast summary per location (full refresh).

    Inherits from ``Stream`` (not ``RESTStream``) because the URL is dynamic
    per location and provided via parent context.
    """

    name = "forecasts_daily"
    primary_keys: ClassVar[list[str]] = ["location_id", "forecast_timestamp"]
    replication_key = None
    parent_stream_type = ForecastLocationsStream

    schema = _FORECAST_FIELDS.to_dict()

    @override
    def get_records(self, context: Context | None) -> Iterable[dict]:
        if not context:
            return
        url: str = context["forecast_url"]
        response = http_lib.get(url, timeout=30)
        response.raise_for_status()
        yield from _parse_forecast_records(
            context["location_id"],
            response.json(),
            daily=True,
        )
