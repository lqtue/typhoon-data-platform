"""Tests for crawlers/jma.py"""
import pytest
from unittest.mock import MagicMock, patch
from crawlers.jma import parse_jma_js, positions_to_db_rows


SAMPLE_JMA_JS = """
var targetTc = [
  {"tcId":"2024W09","enName":"BEBINCA","lat":18.5,"lon":138.2,
   "maxWindSpeedKt":90,"centralPressure":970},
  {"tcId":"2024W10","enName":"PULASAN","lat":12.0,"lon":120.0,
   "maxWindSpeedKt":35,"centralPressure":1000}
];
"""


def test_parse_jma_js_extracts_storms():
    storms = parse_jma_js(SAMPLE_JMA_JS)
    assert len(storms) == 2
    assert storms[0]["storm_id"] == "2024W09"
    assert storms[0]["name"] == "BEBINCA"
    assert storms[0]["lat"] == 18.5
    assert storms[0]["wind_kt"] == 90


def test_parse_jma_js_missing_data_returns_empty():
    assert parse_jma_js("var targetTc = null;") == []


def test_parse_jma_js_malformed_returns_empty():
    assert parse_jma_js("not javascript") == []


def test_positions_to_db_rows_structure():
    from datetime import datetime, timezone
    now = datetime(2024, 9, 18, 0, 0, tzinfo=timezone.utc)
    storm = {"lat": 18.5, "lon": 138.2, "wind_kt": 90, "pressure_hpa": 970}
    rows = positions_to_db_rows(storm, storm_pk=99, now_utc=now)
    assert len(rows) == 1
    r = rows[0]
    assert r["storm_id"] == 99
    assert r["is_forecast"] is False
    assert "source" not in r  # source lives on parent storms row, not storm_positions
    assert "POINT" in r["location"]
