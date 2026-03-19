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
from requests.exceptions import RequestException

SOGIO_DU_BAO = 6  # forecast hours, required by the API

from .base import SupabaseWriter, CrawlLogger, CrawlConfig, retry_with_backoff, build_client_from_env

log = logging.getLogger(__name__)

WARNINGS_URL = "https://luquetsatlo.nchmf.gov.vn/LayerMapBox/getDSCanhbaoSLLQ"
TIMEOUT = 45
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; VnExpress-Spotlight-DataBot/1.0)",
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


def parse_warnings(api_response) -> list[dict]:
    """
    Map NCHMF response → flood_warnings rows.
    Handles two response formats:
      - GeoJSON FeatureCollection (dict with "features") — includes boundary geometry
      - Flat list of commune records — no boundary geometry
    """
    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []

    if isinstance(api_response, dict):
        # GeoJSON FeatureCollection
        for feat in api_response.get("features", []):
            props = feat.get("properties", {})
            geom = feat.get("geometry")
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
                "boundary":     json.dumps(geom) if geom else None,
                "fetched_at":   fetched_at,
            })
    elif isinstance(api_response, list):
        # Flat commune-level list
        for rec in api_response:
            rows.append({
                "ward_code":    rec.get("commune_id_2cap"),
                "ward_name":    rec.get("commune_name_2cap"),
                "district":     None,
                "province":     rec.get("provinceName_2cap"),
                "warning_type": WARNING_TYPE_MAP.get(rec.get("nguycoluquet", ""), None),
                "severity":     rec.get("nguycosatlo") or rec.get("nguycoluquet"),
                "valid_from":   None,
                "valid_until":  None,
                "boundary":     None,
                "fetched_at":   fetched_at,
            })

    return rows


def _fetch(date_str: str) -> dict | list:
    """POST form-encoded request, raises on HTTP error."""
    r = requests.post(
        WARNINGS_URL,
        data={"sogiodubao": SOGIO_DU_BAO, "date": date_str},
        headers=HEADERS,
        timeout=TIMEOUT,
    )
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
        now_utc = datetime.now(timezone.utc)
        date_str = now_utc.strftime("%Y-%m-%d %H:00:00")
        raw = retry_with_backoff(lambda: _fetch(date_str))
        rows = parse_warnings(raw)
        total = writer.truncate_and_insert("flood_warnings", rows)
        log.info("NCHMF: replaced flood_warnings with %d rows", total)

        config.update_last_run()
        logger.finish(log_id, total, "success")

    except RequestException as exc:
        log.warning("NCHMF upstream unavailable (skipping): %s", exc)
        logger.finish(log_id, total, "upstream_unavailable", str(exc))
    except Exception as exc:
        logger.finish(log_id, total, "error", str(exc))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
