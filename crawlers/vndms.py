"""
vndms.py — VNDMS River Water Level Crawler

Fetches river monitoring station metadata and current water level readings
from the Vietnam National Data Management System (VNDMS).

The API returns a single GeoJSON FeatureCollection. Each feature's popupInfo
HTML contains station code, name, river, province, and the current water level.
This avoids 455+ individual per-station requests.

Writes to: water_stations (upsert on station_code), water_levels (upsert on station_id+recorded_at)

API endpoint:
  GET https://vndms.dmptc.gov.vn/water_level — GeoJSON FeatureCollection with all stations
"""

import logging
import re
from datetime import datetime, timezone

import requests

from .base import SupabaseWriter, CrawlLogger, CrawlConfig, retry_with_backoff, build_client_from_env

log = logging.getLogger(__name__)

STATIONS_URL = "https://vndms.dmptc.gov.vn/water_level"
TIMEOUT = 30
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; VnExpress-Spotlight-DataBot/1.0)",
}

# Patterns for parsing the popup HTML
_RE_ID    = re.compile(r"data-id='(\d+)'")
_RE_NAME  = re.compile(r'Tên trạm: <b>([^<]+)</b>')
_RE_RIVER = re.compile(r'Sông: <b>([^<]+)</b>')
_RE_PROV  = re.compile(r'Địa điểm: <b>([^<]+)</b>')
_RE_LEVEL = re.compile(r'Mực nước \(([0-9.]+)\(m\)')


def parse_features(geojson: dict, now_utc: datetime) -> tuple[list[dict], list[dict]]:
    """
    Parse GeoJSON FeatureCollection → (station_rows, pending_level_rows).

    pending_level_rows use 'station_code' instead of 'station_id';
    the caller replaces station_code with the DB primary key after upserting stations.
    """
    recorded_at = now_utc.isoformat()
    station_rows: list[dict] = []
    pending_levels: list[dict] = []

    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        geom  = feature.get("geometry", {})
        popup = props.get("popupInfo", "")

        m_id = _RE_ID.search(popup)
        if not m_id:
            continue
        station_code = m_id.group(1)

        m_name  = _RE_NAME.search(popup)
        m_river = _RE_RIVER.search(popup)
        m_prov  = _RE_PROV.search(popup)
        m_level = _RE_LEVEL.search(popup)

        coords = geom.get("coordinates", [])
        lon = coords[0] if len(coords) > 1 else None
        lat = coords[1] if len(coords) > 1 else None

        station_rows.append({
            "station_code":    station_code,
            "name":            m_name.group(1).strip() if m_name else props.get("label"),
            "river":           m_river.group(1) if m_river else None,
            "basin":           None,
            "province":        m_prov.group(1) if m_prov else None,
            "location":        f"POINT({lon} {lat})" if lat is not None and lon is not None else None,
            "alert_level_1_m": None,
            "alert_level_2_m": None,
            "alert_level_3_m": None,
            "source":          "vndms",
        })

        if m_level:
            pending_levels.append({
                "station_code": station_code,
                "recorded_at":  recorded_at,
                "level_m":      float(m_level.group(1)),
                "alert_status": "normal",
            })

    return station_rows, pending_levels


def compute_alert_status(level: float | None,
                          a1: float | None, a2: float | None, a3: float | None) -> str:
    if level is None:
        return "normal"
    if a3 is not None and level >= a3:
        return "level3"
    if a2 is not None and level >= a2:
        return "level2"
    if a1 is not None and level >= a1:
        return "level1"
    return "normal"


def run():
    """Entry point: parse GeoJSON snapshot → upsert stations + current levels."""
    client = build_client_from_env()
    writer = SupabaseWriter(client)
    logger = CrawlLogger(client, "vndms")
    config = CrawlConfig(client, "vndms")

    log_id = logger.start()
    total = 0
    try:
        now_utc = datetime.now(timezone.utc)

        # Single request: GeoJSON with all 455 stations + current levels in popupInfo
        geojson = retry_with_backoff(lambda: _fetch_json_get(STATIONS_URL))
        station_rows, pending_levels = parse_features(geojson, now_utc)
        log.info("Parsed %d stations, %d with current readings", len(station_rows), len(pending_levels))

        # Upsert station metadata
        writer.upsert("water_stations", station_rows, on_conflict="station_code")

        # Fetch station PKs to build station_code → id map
        db_stations = (
            client.table("water_stations")
            .select("id, station_code")
            .execute()
        ).data
        code_to_id = {s["station_code"]: s["id"] for s in db_stations}

        # Build level rows with proper station_id FK
        level_rows = []
        for pl in pending_levels:
            sid = code_to_id.get(pl["station_code"])
            if sid is not None:
                level_rows.append({
                    "station_id":   sid,
                    "recorded_at":  pl["recorded_at"],
                    "level_m":      pl["level_m"],
                    "alert_status": pl["alert_status"],
                })

        total = writer.upsert("water_levels", level_rows, on_conflict="station_id,recorded_at")

        config.update_last_run()
        logger.finish(log_id, total, "success")
        log.info("VNDMS crawl complete. %d level rows upserted.", total)

    except Exception as exc:
        logger.finish(log_id, total, "error", str(exc))
        raise


def _fetch_json_get(url: str) -> dict:
    """GET request returning parsed JSON, raises on HTTP error."""
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
