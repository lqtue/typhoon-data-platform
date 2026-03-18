"""Tests for crawlers/vndms.py"""
import pytest
from crawlers.vndms import parse_stations, parse_water_levels, compute_alert_status


SAMPLE_STATIONS_RESPONSE = [
    {
        "stationCode": "HN001",
        "stationName": "Hà Nội",
        "riverName": "Sông Hồng",
        "basinName": "Hồng",
        "provinceName": "Hà Nội",
        "lat": 21.03,
        "lon": 105.84,
        "alertLevel1": 9.5,
        "alertLevel2": 10.5,
        "alertLevel3": 11.5,
    }
]

SAMPLE_LEVELS_RESPONSE = {
    "data": [
        {"time": "2024-09-18T00:00:00+07:00", "value": 8.5},
        {"time": "2024-09-18T01:00:00+07:00", "value": 9.6},
    ]
}


def test_parse_stations_maps_fields():
    rows = parse_stations(SAMPLE_STATIONS_RESPONSE)
    assert len(rows) == 1
    r = rows[0]
    assert r["station_code"] == "HN001"
    assert r["name"] == "Hà Nội"
    assert r["river"] == "Sông Hồng"
    assert r["alert_level_1_m"] == 9.5
    assert "POINT" in r["location"]


def test_parse_stations_empty():
    assert parse_stations([]) == []


def test_parse_water_levels_maps_fields():
    rows = parse_water_levels(SAMPLE_LEVELS_RESPONSE, station_id=1,
                               alert_1=9.5, alert_2=10.5, alert_3=11.5)
    assert len(rows) == 2
    # First row: 8.5 < 9.5 → normal
    assert rows[0]["level_m"] == 8.5
    assert rows[0]["alert_status"] == "normal"
    assert rows[0]["station_id"] == 1
    # Second row: 9.6 > 9.5 → level1
    assert rows[1]["alert_status"] == "level1"


def test_compute_alert_status():
    assert compute_alert_status(8.0, 9.5, 10.5, 11.5) == "normal"
    assert compute_alert_status(9.6, 9.5, 10.5, 11.5) == "level1"
    assert compute_alert_status(10.6, 9.5, 10.5, 11.5) == "level2"
    assert compute_alert_status(11.6, 9.5, 10.5, 11.5) == "level3"
    assert compute_alert_status(None, 9.5, 10.5, 11.5) == "normal"
    assert compute_alert_status(10.0, None, None, None) == "normal"
