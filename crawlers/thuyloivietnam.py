"""
thuyloivietnam.py — Thuy Loi Vietnam Lake Level Crawler

Fetches lake water levels, storage volumes, and inflow/outflow from the
Thuy Loi Vietnam monitoring system.

Writes to: lakes (upsert on lake_code), lake_levels (upsert on lake_id+recorded_at)

API endpoint:
  POST http://e15.thuyloivietnam.vn/CanhBaoSoLieu/ATCBDTHo
  Body: {"fromDate": "2024-09-18", "toDate": "2024-09-18"}
  Returns: list of lake objects, each containing metadata + nested "data" time series
"""

import logging
from datetime import datetime, timezone, timedelta

import requests
from requests.exceptions import RequestException

from .base import SupabaseWriter, CrawlLogger, CrawlConfig, retry_with_backoff, build_client_from_env

log = logging.getLogger(__name__)

LAKE_LEVELS_URL = "http://e15.thuyloivietnam.vn/CanhBaoSoLieu/ATCBDTHo"
TIMEOUT = 30
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; VnExpress-Spotlight-DataBot/1.0)",
    "Content-Type": "application/json",
}


def parse_lakes(api_response: list) -> list[dict]:
    """Map Thuy Loi lake metadata → lakes rows."""
    rows = []
    for lake in api_response:
        lat = lake.get("lat")
        lon = lake.get("lon")
        rows.append({
            "lake_code":           lake["hoCode"],
            "name":                lake.get("hoName"),
            "province":            lake.get("provinceName"),
            "location":            f"POINT({lon} {lat})" if lat is not None and lon is not None else None,
            "capacity_million_m3": lake.get("dungTich"),
        })
    return rows


def parse_lake_levels(api_response: dict, lake_id: int) -> list[dict]:
    """Map Thuy Loi time-series response → lake_levels rows.
    api_response is either the full lake object (which has a 'data' key)
    or a standalone dict with a 'data' key.
    """
    data = api_response.get("data", [])
    rows = []
    for entry in data:
        rows.append({
            "lake_id":            lake_id,
            "recorded_at":        entry.get("thoiGian"),
            "level_m":            entry.get("mucNuoc"),
            "storage_million_m3": entry.get("dungTich"),
            "inflow_m3s":         entry.get("luuLuongDen"),
            "outflow_m3s":        entry.get("luuLuongXa"),
        })
    return rows


def _fetch_json_post(url: str, body: dict) -> list:
    """POST request returning parsed JSON, raises on HTTP error."""
    r = requests.post(url, json=body, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def run():
    """Entry point: fetch all lake data for the last 24 hours."""
    client = build_client_from_env()
    writer = SupabaseWriter(client)
    logger = CrawlLogger(client, "thuyloivietnam")
    config = CrawlConfig(client, "thuyloivietnam")

    log_id = logger.start()
    total = 0
    try:
        now_utc   = datetime.now(timezone.utc)
        today     = now_utc.strftime("%Y-%m-%d")
        yesterday = (now_utc - timedelta(days=1)).strftime("%Y-%m-%d")

        raw = retry_with_backoff(
            lambda: _fetch_json_post(LAKE_LEVELS_URL, {"fromDate": yesterday, "toDate": today})
        )

        # The API returns a list of lake objects; each contains metadata + nested time series
        lake_rows = parse_lakes(raw)
        writer.upsert("lakes", lake_rows, on_conflict="lake_code")
        log.info("Synced %d lakes", len(lake_rows))

        db_lakes = client.table("lakes").select("id, lake_code").execute().data
        lake_pk_map = {lake["lake_code"]: lake["id"] for lake in db_lakes}

        for lake_raw in raw:
            code = lake_raw.get("hoCode")
            lake_pk = lake_pk_map.get(code)
            if not lake_pk:
                continue
            level_rows = parse_lake_levels(lake_raw, lake_pk)
            count = writer.upsert("lake_levels", level_rows,
                                   on_conflict="lake_id,recorded_at")
            total += count

        config.update_last_run()
        logger.finish(log_id, total, "success")
        log.info("Thuy Loi crawl complete. %d rows upserted.", total)

    except RequestException as exc:
        log.warning("Thuy Loi upstream unavailable (skipping): %s", exc)
        logger.finish(log_id, total, "upstream_unavailable", str(exc))
    except Exception as exc:
        logger.finish(log_id, total, "error", str(exc))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
