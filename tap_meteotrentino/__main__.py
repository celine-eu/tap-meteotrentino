"""MeteoTrentino entry point."""

from __future__ import annotations

from tap_meteotrentino.tap import TapMeteoTrentino

TapMeteoTrentino.cli()
