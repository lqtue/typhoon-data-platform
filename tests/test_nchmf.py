"""Tests for crawlers/nchmf.py"""
import pytest
from crawlers.nchmf import parse_warnings


SAMPLE_WARNINGS_RESPONSE = {
    "features": [
        {
            "properties": {
                "MA_XA": "VN001",
                "TEN_XA": "Xã Hương Sơn",
                "TEN_HUYEN": "Huyện Mỹ Đức",
                "TEN_TINH": "Hà Nội",
                "LOAI_CB": "luquetsatlo",
                "CAP_CB": "rat_cao",
                "TU_NGAY": "2024-09-18T00:00:00+07:00",
                "DEN_NGAY": "2024-09-19T00:00:00+07:00",
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[105.5, 20.5], [106.0, 20.5],
                                  [106.0, 21.0], [105.5, 21.0], [105.5, 20.5]]]
            }
        }
    ]
}

WARNING_TYPE_MAP = {
    "luquetsatlo": "landslide",
    "ngaplut": "waterlogging",
    "luquet": "flash_flood",
}


def test_parse_warnings_maps_fields():
    rows = parse_warnings(SAMPLE_WARNINGS_RESPONSE)
    assert len(rows) == 1
    r = rows[0]
    assert r["ward_code"] == "VN001"
    assert r["ward_name"] == "Xã Hương Sơn"
    assert r["province"] == "Hà Nội"
    assert r["warning_type"] == "landslide"
    assert r["severity"] == "very_high"
    assert "POLYGON" in r["boundary"] or r["boundary"] is not None


def test_parse_warnings_empty():
    assert parse_warnings({"features": []}) == []


def test_parse_warnings_handles_missing_geometry():
    feature = {
        "properties": {
            "MA_XA": "VN002", "TEN_XA": "Test", "TEN_HUYEN": "H",
            "TEN_TINH": "T", "LOAI_CB": "ngaplut", "CAP_CB": "cao",
            "TU_NGAY": "2024-09-18T00:00:00+07:00",
            "DEN_NGAY": "2024-09-19T00:00:00+07:00",
        },
        "geometry": None
    }
    rows = parse_warnings({"features": [feature]})
    assert len(rows) == 1
    assert rows[0]["boundary"] is None
