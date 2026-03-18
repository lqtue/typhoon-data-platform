"""
nchmf.py — NCHMF Landslide & Flood Warning Crawler

Fetches current landslide and flood warning polygons from the National Center
for Hydro-Meteorological Forecasting (NCHMF).

Replacement strategy: TRUNCATE + INSERT per crawl cycle (NCHMF returns a full
current snapshot; no incremental update needed).

Writes to: flood_warnings table (truncate-and-replace)

API endpoint:
  POST https://luquetsatlo.nchmf.gov.vn/LayerMapBox/getDSCanhbaoSLLQ
  Returns: GeoJSON FeatureCollection
"""

import json
import logging
from datetime import datetime, timezone

import requests

from .base import SupabaseWriter, CrawlLogger, CrawlConfig, retry_with_backoff, build_client_from_env

log = logging.getLogger(__name__)

WARNINGS_URL = "https://luquetsatlo.nchmf.gov.vn/LayerMapBox/getDSCanhbaoSLLQ"
TIMEOUT = 30
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; VnExpress-Spotlight-DataBot/1.0)",
    "Content-Type": "application/json",
}

WARNING_TYPE_MAP = {
    "luquetsatlo": "landslide",
    "ngaplut":     "waterlogging",
    "luquet":      "flash_flood",
}

SEVERITY_MAP = {
    "rat_cao":    "very_high",
    "cao":        "high",
    "trung_binh": "medium",
    "thap":       "low",
}


def parse_warnings(api_response: dict) -> list[dict]:
    """
    Map NCHMF GeoJSON FeatureCollection → flood_warnings rows.
    Boundary stored as GeoJSON string; PostGIS ingests via ST_GeomFromGeoJSON.
    Accepts both Polygon and MultiPolygon (boundary column is GEOMETRY(Geometry)).
    """
    features = api_response.get("features", [])
    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for feat in features:
        props = feat.get("properties", {})
        geom = feat.get("geometry")
        boundary_geojson = json.dumps(geom) if geom else None

        loai = props.get("LOAI_CB", "")
        cap  = props.get("CAP_CB", "")

        rows.append({
            "ward_code":    props.get("MA_XA"),
            "ward_name":    props.get("TEN_XA"),
            "district":     props.get("TEN_HUYEN"),
            "province":     props.get("TEN_TINH"),
            "warning_type": WARNING_TYPE_MAP.get(loai, loai),
            "severity":     SEVERITY_MAP.get(cap, cap),
            "valid_from":   props.get("TU_NGAY"),
            "valid_until":  props.get("DEN_NGAY"),
            "boundary":     boundary_geojson,
            "fetched_at":   fetched_at,
        })
    return rows


def _fetch_json_post(url: str, body: dict) -> dict:
    """POST request returning parsed JSON, raises on HTTP error."""
    r = requests.post(url, json=body, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def run():
    """Entry point: truncate flood_warnings and insert fresh snapshot."""
    client = build_client_from_env()
    writer = SupabaseWriter(client)
    logger = CrawlLogger(client, "nchmf")
    config = CrawlConfig(client, "nchmf")

    log_id = logger.start()
    total = 0
    try:
        raw = retry_with_backoff(lambda: _fetch_json_post(WARNINGS_URL, {}))
        rows = parse_warnings(raw)
        total = writer.truncate_and_insert("flood_warnings", rows)
        log.info("NCHMF: replaced flood_warnings with %d rows", total)

        config.update_last_run()
        logger.finish(log_id, total, "success")

    except Exception as exc:
        logger.finish(log_id, total, "error", str(exc))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
