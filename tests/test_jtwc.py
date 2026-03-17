"""Tests for crawlers/jtwc.py"""
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
import responses as resp_mock

from crawlers.jtwc import (
    parse_rss,
    parse_warning_text,
    wind_category,
    positions_to_db_rows,
    BASIN_CONFIG,
)


SAMPLE_RSS = """<?xml version="1.0"?>
<rss version="2.0"><channel>
  <item><title>Typhoon 09W (BEBINCA) Warning NR 001</title></item>
  <item><title>Tropical Depression 03W Warning NR 001</title></item>
</channel></rss>"""

SAMPLE_WARNING = """
TROPICAL CYCLONE WARNING
...
POSITION: 18.5N 138.2E
MAX SUSTAINED WINDS: 175 KM/H (95 KT).
MIN SEA LEVEL PRESSURE: 965 MB
...
FORECAST VALID 18/0600Z 20.0N 136.5E
  MAX WIND: 175 KM/H (95 KT).
FORECAST VALID 18/1800Z 21.5N 134.0E
  MAX WIND: 165 KM/H (90 KT).
FORECAST VALID 19/0600Z 23.0N 132.0E
  MAX WIND: 155 KM/H (85 KT).
  DISSIPATED
"""


def test_parse_rss_extracts_storms():
    storms = parse_rss(SAMPLE_RSS)
    assert len(storms) == 2
    assert storms[0]["storm_id"] == "09W"
    assert storms[0]["name"] == "BEBINCA"
    assert storms[0]["basin"] == "W"
    assert "text_url" in storms[0]


def test_parse_rss_empty_returns_empty():
    assert parse_rss("<rss><channel></channel></rss>") == []


def test_parse_rss_malformed_returns_empty():
    assert parse_rss("not xml at all") == []


def test_parse_warning_text_extracts_current_position():
    now = datetime(2024, 9, 18, 0, 0, tzinfo=timezone.utc)
    positions = parse_warning_text(SAMPLE_WARNING, now)
    assert len(positions) >= 1
    p0 = positions[0]
    assert p0["lat"] == pytest.approx(18.5)
    assert p0["lon"] == pytest.approx(138.2)
    assert p0["wind_kt"] == 95
    assert p0["pressure_hpa"] == 965
    assert p0["tau"] == 0
    assert p0["is_forecast"] is False


def test_parse_warning_text_extracts_forecast_positions():
    now = datetime(2024, 9, 18, 0, 0, tzinfo=timezone.utc)
    positions = parse_warning_text(SAMPLE_WARNING, now)
    forecasts = [p for p in positions if p["is_forecast"]]
    assert len(forecasts) >= 2
    assert forecasts[0]["forecast_hour"] == 6
    assert forecasts[0]["wind_kt"] == 95


def test_parse_warning_text_no_position_returns_empty():
    assert parse_warning_text("no position data here", datetime.now(timezone.utc)) == []


def test_wind_category():
    assert wind_category(25) == "TD"
    assert wind_category(40) == "TS"
    assert wind_category(55) == "STS"
    assert wind_category(75) == "TY"
    assert wind_category(140) == "STY"
    assert wind_category(None) == "TD"


def test_positions_to_db_rows_returns_correct_structure():
    now = datetime(2024, 9, 18, 0, 0, tzinfo=timezone.utc)
    positions = [
        {"lat": 18.5, "lon": 138.2, "wind_kt": 95, "pressure_hpa": 965,
         "tau": 0, "is_forecast": False, "forecast_hour": None},
        {"lat": 20.0, "lon": 136.5, "wind_kt": 95, "pressure_hpa": None,
         "tau": 6, "is_forecast": True, "forecast_hour": 6},
    ]
    storm_pk = 42  # storms.id integer PK
    rows = positions_to_db_rows(positions, storm_pk, now)
    assert len(rows) == 2

    # Best-track row
    r0 = rows[0]
    assert r0["storm_id"] == 42
    assert r0["is_forecast"] is False
    assert r0["forecast_hour"] is None
    assert r0["wind_kt"] == 95
    assert r0["category"] == "TY"
    # GeoJSON WKT point representation for PostGIS
    assert "POINT" in r0["location"]

    # Forecast row
    r1 = rows[1]
    assert r1["is_forecast"] is True
    assert r1["forecast_hour"] == 6
