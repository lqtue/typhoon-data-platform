"""
jma.py — JMA RSMC Tokyo Fallback Crawler

Fetches active storm data from JMA when JTWC text warnings are unavailable.
JMA's targetTc.js only exists during active typhoon season.
Writes to: storms, storm_positions (current position only, no forecast track).
"""

import json
import logging
import re
from datetime import datetime, timezone

import requests

from .base import SupabaseWriter, CrawlLogger, CrawlConfig, retry_with_backoff, build_client_from_env
from .jtwc import wind_category

log = logging.getLogger(__name__)

JMA_TC_URL = "https://www.jma.go.jp/bosai/typhoon/data/targetTc.js"
TIMEOUT = 20
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; VnExpress-Spotlight-DataBot/1.0)"}


def parse_jma_js(text: str) -> list[dict]:
    """
    Parse JMA targetTc.js → list of storm dicts.
    Format: var targetTc = [{...}, ...];
    """
    m = re.search(r"var\s+targetTc\s*=\s*(\[.*?\]);", text, re.DOTALL)
    if not m:
        return []
    try:
        tc_list = json.loads(m.group(1))
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(tc_list, list):
        return []

    storms = []
    for tc in tc_list:
        if tc.get("lat") is None or tc.get("lon") is None:
            continue
        storms.append({
            "storm_id":     tc.get("tcId", "UNKNOWN"),
            "name":         (tc.get("enName") or tc.get("name", "UNKNOWN")).upper(),
            "lat":          tc["lat"],
            "lon":          tc["lon"],
            "wind_kt":      tc.get("maxWindSpeedKt") or tc.get("windKt"),
            "pressure_hpa": tc.get("centralPressure") or tc.get("pressure"),
        })
    return storms


def positions_to_db_rows(storm: dict, storm_pk: int, now_utc: datetime) -> list[dict]:
    """Convert a JMA storm dict → storm_positions row for Supabase upsert."""
    return [{
        "storm_id":      storm_pk,
        "recorded_at":   now_utc.isoformat(),
        "location":      f"POINT({storm['lon']} {storm['lat']})",
        "wind_kt":       storm["wind_kt"],
        "pressure_hpa":  storm.get("pressure_hpa"),
        "category":      wind_category(storm["wind_kt"]),
        "is_forecast":   False,
        "forecast_hour": None,
        "fetched_at":    now_utc.isoformat(),
        # Note: no 'source' column on storm_positions — source lives on the parent storms row
    }]


def _fetch(url: str) -> str:
    """Fetch URL, raise on HTTP errors so retry_with_backoff can retry."""
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def run():
    """Entry point: fetch JMA data and upsert to Supabase."""
    client = build_client_from_env()
    writer = SupabaseWriter(client)
    logger = CrawlLogger(client, "jma")
    config = CrawlConfig(client, "jma")

    log_id = logger.start()
    total = 0
    try:
        now_utc = datetime.now(timezone.utc)
        text = retry_with_backoff(lambda: _fetch(JMA_TC_URL))
        storms = parse_jma_js(text)
        log.info("JMA storms found: %d", len(storms))

        for storm in storms:
            client.table("storms").upsert({
                "storm_id":     storm["storm_id"],
                "name":         storm["name"],
                "basin":        "WP",
                "source":       "jma",
                "status":       "active",
                "last_seen_at": now_utc.isoformat(),
            }, on_conflict="storm_id").execute()

            pk_row = (client.table("storms")
                      .select("id")
                      .eq("storm_id", storm["storm_id"])
                      .single()
                      .execute())
            storm_pk = pk_row.data["id"]

            rows = positions_to_db_rows(storm, storm_pk, now_utc)
            count = writer.upsert(
                "storm_positions", rows,
                on_conflict="storm_id,recorded_at,is_forecast,forecast_hour"
            )
            total += count

        config.update_last_run()
        logger.finish(log_id, total, "success")

    except Exception as exc:
        logger.finish(log_id, total, "error", str(exc))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
