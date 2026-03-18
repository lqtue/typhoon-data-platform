"""Tests for crawlers/thuyloivietnam.py"""
import pytest
from crawlers.thuyloivietnam import parse_lakes, parse_lake_levels


SAMPLE_LAKES_RESPONSE = [
    {
        "hoCode": "HOA_BINH",
        "hoName": "Hồ Hoà Bình",
        "provinceName": "Hoà Bình",
        "lat": 20.83,
        "lon": 105.35,
        "dungTich": 9450.0,
    }
]

SAMPLE_LEVELS_RESPONSE = {
    "data": [
        {
            "thoiGian": "2024-09-18T00:00:00",
            "mucNuoc": 115.5,
            "dungTich": 8500.0,
            "luuLuongDen": 2500.0,
            "luuLuongXa": 3000.0,
        }
    ]
}


def test_parse_lakes_maps_fields():
    rows = parse_lakes(SAMPLE_LAKES_RESPONSE)
    assert len(rows) == 1
    r = rows[0]
    assert r["lake_code"] == "HOA_BINH"
    assert r["name"] == "Hồ Hoà Bình"
    assert r["capacity_million_m3"] == 9450.0
    assert "POINT" in r["location"]


def test_parse_lake_levels_maps_fields():
    rows = parse_lake_levels(SAMPLE_LEVELS_RESPONSE, lake_id=5)
    assert len(rows) == 1
    r = rows[0]
    assert r["lake_id"] == 5
    assert r["level_m"] == 115.5
    assert r["storage_million_m3"] == 8500.0
    assert r["inflow_m3s"] == 2500.0
    assert r["outflow_m3s"] == 3000.0
