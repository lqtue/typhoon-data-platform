"""
vndms.py — VNDMS River Water Level Crawler

Fetches river monitoring station metadata and hourly water level readings
from the Vietnam National Data Management System (VNDMS).

Writes to: water_stations (upsert on station_code), water_levels (upsert on station_id+recorded_at)

API endpoints:
  GET  https://vndms.dmptc.gov.vn/water_level — station list with metadata
  POST https://vndms.dmc.gov.vn/home/detailRain — time-series for a station
       Body: {"stationCode": "HN001", "fromDate": "2024-09-18", "toDate": "2024-09-18"}
"""

import logging
from datetime import datetime, timezone, timedelta

import requests

from .base import SupabaseWriter, CrawlLogger, CrawlConfig, retry_with_backoff, build_client_from_env

log = logging.getLogger(__name__)

STATIONS_URL = "https://vndms.dmptc.gov.vn/water_level"
LEVELS_URL   = "https://vndms.dmc.gov.vn/home/detailRain"
TIMEOUT = 30
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; VnExpress-Spotlight-DataBot/1.0)",
    "Content-Type": "application/json",
}


def parse_stations(api_response: list) -> list[dict]:
    """Map VNDMS station API response → water_stations rows."""
    rows = []
    for s in api_response:
        lat = s.get("lat")
        lon = s.get("lon")
        rows.append({
            "station_code":    s["stationCode"],
            "name":            s.get("stationName"),
            "river":           s.get("riverName"),
            "basin":           s.get("basinName"),
            "province":        s.get("provinceName"),
            "location":        f"POINT({lon} {lat})" if lat and lon else None,
            "alert_level_1_m": s.get("alertLevel1"),
            "alert_level_2_m": s.get("alertLevel2"),
            "alert_level_3_m": s.get("alertLevel3"),
            "source":          "vndms",
        })
    return rows


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


def parse_water_levels(api_response: dict, station_id: int,
                        alert_1: float | None, alert_2: float | None,
                        alert_3: float | None) -> list[dict]:
    """Map VNDMS time-series API response → water_levels rows."""
    data = api_response.get("data", [])
    rows = []
    for entry in data:
        level = entry.get("value")
        rows.append({
            "station_id":   station_id,
            "recorded_at":  entry["time"],
            "level_m":      level,
            "alert_status": compute_alert_status(level, alert_1, alert_2, alert_3),
        })
    return rows


def run():
    """Entry point: sync stations then fetch latest readings for each."""
    client = build_client_from_env()
    writer = SupabaseWriter(client)
    logger = CrawlLogger(client, "vndms")
    config = CrawlConfig(client, "vndms")

    log_id = logger.start()
    total = 0
    try:
        now_utc = datetime.now(timezone.utc)
        today   = now_utc.strftime("%Y-%m-%d")
        yesterday = (now_utc - timedelta(days=1)).strftime("%Y-%m-%d")

        # Fetch + upsert station metadata
        stations_raw = retry_with_backoff(lambda: _fetch_json_get(STATIONS_URL))
        station_rows = parse_stations(stations_raw)
        writer.upsert("water_stations", station_rows, on_conflict="station_code")
        log.info("Synced %d stations", len(station_rows))

        # Fetch stations with their PKs
        db_stations = (client.table("water_stations")
                       .select("id, station_code, alert_level_1_m, alert_level_2_m, alert_level_3_m")
                       .execute()).data

        # Fetch water levels for each station (last 24h)
        for station in db_stations:
            code = station["station_code"]
            try:
                resp = retry_with_backoff(lambda c=code: _fetch_json_post(
                    LEVELS_URL, {"stationCode": c, "fromDate": yesterday, "toDate": today}
                ))
                level_rows = parse_water_levels(
                    resp, station["id"],
                    station["alert_level_1_m"],
                    station["alert_level_2_m"],
                    station["alert_level_3_m"],
                )
                count = writer.upsert("water_levels", level_rows,
                                       on_conflict="station_id,recorded_at")
                total += count
            except Exception as exc:
                log.warning("Failed to fetch levels for %s: %s", code, exc)

        config.update_last_run()
        logger.finish(log_id, total, "success")
        log.info("VNDMS crawl complete. %d level rows upserted.", total)

    except Exception as exc:
        logger.finish(log_id, total, "error", str(exc))
        raise


def _fetch_json_get(url: str) -> list:
    """GET request returning parsed JSON, raises on HTTP error."""
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def _fetch_json_post(url: str, body: dict) -> dict:
    """POST request returning parsed JSON, raises on HTTP error."""
    r = requests.post(url, json=body, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
