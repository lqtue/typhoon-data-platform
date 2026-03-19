"""
thuyloivietnam.py — Thuy Loi Vietnam Lake Level Crawler

Fetches lake water levels, storage volumes, and inflow/outflow from the
Thuy Loi Vietnam monitoring system.

Writes to: lakes (upsert on lake_code), lake_levels (upsert on lake_id+recorded_at)

API endpoint:
  POST http://e15.thuyloivietnam.vn/CanhBaoSoLieu/ATCBDTHo
  Body (form-encoded): time=2024-09-18 00:00:00,000&ishothuydien=0
  Returns: list of lake snapshot objects per day
"""

import logging
import re
from datetime import datetime, timezone, timedelta

import requests
from requests.exceptions import RequestException

from .base import SupabaseWriter, CrawlLogger, CrawlConfig, retry_with_backoff, build_client_from_env

log = logging.getLogger(__name__)

LAKE_LEVELS_URL = "http://e15.thuyloivietnam.vn/CanhBaoSoLieu/ATCBDTHo"
TIMEOUT = 60
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; VnExpress-Spotlight-DataBot/1.0)",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Referer": "http://e15.thuyloivietnam.vn/",
}


def _ms_to_iso(ms_val) -> str | None:
    """Convert JS /Date(ms)/ or plain ms integer → ISO UTC string."""
    if not ms_val:
        return None
    try:
        m = re.search(r"\d+", str(ms_val))
        if not m:
            return None
        return datetime.fromtimestamp(int(m.group()) / 1000.0, tz=timezone.utc).isoformat()
    except Exception:
        return None


def parse_lakes(api_response: list) -> list[dict]:
    """Map Thuy Loi API response → lakes rows."""
    rows = []
    for lake in api_response:
        lat = lake.get("Y") or lake.get("lat")
        lon = lake.get("X") or lake.get("lon")
        rows.append({
            "lake_code":           lake.get("LakeCode") or lake.get("hoCode"),
            "name":                lake.get("HoName") or lake.get("hoName") or lake.get("LakeName"),
            "province":            lake.get("ProvinceName") or lake.get("provinceName"),
            "location":            f"POINT({lon} {lat})" if lat is not None and lon is not None else None,
            "capacity_million_m3": lake.get("TkDungTich") or lake.get("dungTich"),
        })
    return [r for r in rows if r["lake_code"]]


def parse_lake_level(rec: dict, lake_id: int) -> dict | None:
    """Map one Thuy Loi snapshot → lake_levels row."""
    recorded_at = _ms_to_iso(rec.get("ThoiGianCapNhat"))
    if not recorded_at:
        return None
    return {
        "lake_id":            lake_id,
        "recorded_at":        recorded_at,
        "level_m":            rec.get("TdMucNuoc"),
        "storage_million_m3": rec.get("TdDungTich"),
        "inflow_m3s":         rec.get("QDen"),
        "outflow_m3s":        rec.get("QXa"),
    }


def _fetch(date_str: str) -> list:
    """POST form-encoded request for one day, raises on HTTP error."""
    r = requests.post(
        LAKE_LEVELS_URL,
        data={"time": f"{date_str} 00:00:00,000", "ishothuydien": "0"},
        headers=HEADERS,
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return r.json()


def run():
    """Entry point: fetch lake snapshots for today and yesterday, upsert to Supabase."""
    client = build_client_from_env()
    writer = SupabaseWriter(client)
    logger = CrawlLogger(client, "thuyloivietnam")
    config = CrawlConfig(client, "thuyloivietnam")

    log_id = logger.start()
    total = 0
    try:
        now_utc = datetime.now(timezone.utc)
        dates = [
            (now_utc - timedelta(days=1)).strftime("%Y-%m-%d"),
            now_utc.strftime("%Y-%m-%d"),
        ]

        all_records: list[dict] = []
        for date_str in dates:
            records = retry_with_backoff(lambda d=date_str: _fetch(d))
            all_records.extend(records)
            log.info("Fetched %d records for %s", len(records), date_str)

        # Deduplicate by lake_code (two days of fetching yields duplicate metadata rows)
        seen = set()
        lake_rows = [r for r in parse_lakes(all_records)
                     if r["lake_code"] not in seen and not seen.add(r["lake_code"])]
        writer.upsert("lakes", lake_rows, on_conflict="lake_code")
        log.info("Synced %d lakes", len(lake_rows))

        db_lakes = client.table("lakes").select("id, lake_code").execute().data
        lake_pk_map = {lake["lake_code"]: lake["id"] for lake in db_lakes}

        level_rows = []
        for rec in all_records:
            code = rec.get("LakeCode") or rec.get("hoCode")
            lake_pk = lake_pk_map.get(code)
            if not lake_pk:
                continue
            row = parse_lake_level(rec, lake_pk)
            if row:
                level_rows.append(row)

        total = writer.upsert("lake_levels", level_rows, on_conflict="lake_id,recorded_at")
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
