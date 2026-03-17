"""
jtwc.py — JTWC Active Typhoon Crawler

Fetches active tropical cyclone data from JTWC RSS + per-storm text warnings.
Falls back to JMA RSMC Tokyo XML when JTWC text unavailable.
Writes to: storms, storm_positions tables in Supabase.

Source priority:
  1. JTWC RSS (https://www.metoc.navy.mil/jtwc/rss/jtwc.rss) — discovers active storms
  2. JTWC text warning (per-storm URL) — positions + forecast track + wind radii
  3. JMA XML fallback — current position only (imported via jma.py)
"""

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import requests

from .base import SupabaseWriter, CrawlLogger, CrawlConfig, retry_with_backoff, build_client_from_env

log = logging.getLogger(__name__)

RSS_URL = "https://www.metoc.navy.mil/jtwc/rss/jtwc.rss"
STORM_TXT_URL = "https://www.metoc.navy.mil/jtwc/products/{prefix}{number}{year}.txt"  # year = 2-digit
TIMEOUT = 20
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; VnExpress-Spotlight-DataBot/1.0)",
    "Accept": "text/html,text/plain,application/xml,*/*",
}

BASIN_CONFIG = {
    "W": {"prefix": "wp", "label": "Western Pacific"},
    "A": {"prefix": "io", "label": "North Indian (Arabian Sea)"},
    "B": {"prefix": "io", "label": "North Indian (Bay of Bengal)"},
    "S": {"prefix": "sh", "label": "Southern Hemisphere (Indian)"},
    "P": {"prefix": "sh", "label": "Southern Hemisphere (Pacific)"},
    "C": {"prefix": "cp", "label": "Central Pacific"},
    "E": {"prefix": "ep", "label": "Eastern Pacific"},
}


# ---------------------------------------------------------------------------
# RSS parser — discovers active storm IDs
# ---------------------------------------------------------------------------

def parse_rss(rss_text: str, now_utc: datetime) -> list[dict]:
    """
    Parse JTWC RSS feed → list of active storm dicts.
    Each dict: {storm_id, number, basin, name, label, text_url}
    """
    try:
        root = ET.fromstring(rss_text)
    except ET.ParseError as exc:
        log.error("RSS parse error: %s", exc)
        return []

    year = str(now_utc.year)[-2:]  # JTWC URL uses 2-digit year (e.g. "25" for 2025)
    storms = []
    for item in root.findall(".//item"):
        title = item.findtext("title", "")
        m = re.search(
            r"(?:Super Typhoon|Typhoon|Tropical (?:Storm|Cyclone|Depression))\s+"
            r"(\d{1,2})([A-Z])\s*(?:\(([^)]+)\))?",
            title, re.IGNORECASE,
        )
        if not m:
            continue
        number = m.group(1).zfill(2)
        basin  = m.group(2).upper()
        name   = (m.group(3) or f"INVEST {number}{basin}").strip().upper()
        cfg    = BASIN_CONFIG.get(basin, {"prefix": "wp", "label": "Unknown"})
        storms.append({
            "storm_id": f"{number}{basin}",
            "number":   number,
            "basin":    basin,
            "name":     name,
            "label":    cfg["label"],
            "text_url": STORM_TXT_URL.format(
                prefix=cfg["prefix"], number=number, year=year
            ),
        })
    return storms


# ---------------------------------------------------------------------------
# Text warning parser — positions + forecast track
# ---------------------------------------------------------------------------

def _parse_latlon(lat_str: str, lon_str: str) -> tuple[float, float]:
    lat = float(lat_str[:-1]) * (-1 if lat_str.upper().endswith("S") else 1)
    lon = float(lon_str[:-1]) * (-1 if lon_str.upper().endswith("W") else 1)
    return lat, lon


def parse_warning_text(text: str, now_utc: datetime) -> list[dict]:
    """
    Parse JTWC text warning → list of position dicts ordered by time.
    Each dict: {lat, lon, wind_kt, pressure_hpa, tau, is_forecast, forecast_hour}
    """
    positions = []

    # Current position
    pos_m = re.search(r"POSITION:\s+(\d+\.?\d*[NS])\s+(\d+\.?\d*[EW])", text)
    if not pos_m:
        return []
    lat, lon = _parse_latlon(pos_m.group(1), pos_m.group(2))

    wind_m = re.search(r"MAX (?:SUSTAINED )?WINDS?:.*?\((\d+)\s*KT\)", text, re.IGNORECASE)
    pres_m = re.search(r"MIN(?:IMUM)?\s+SEA\s+LEVEL\s+PRESSURE:\s+(\d+)\s*MB", text, re.IGNORECASE)

    positions.append({
        "lat": lat, "lon": lon,
        "wind_kt": int(wind_m.group(1)) if wind_m else None,
        "pressure_hpa": int(pres_m.group(1)) if pres_m else None,
        "tau": 0,
        "is_forecast": False,
        "forecast_hour": None,
    })

    # Forecast positions
    fc_re = re.compile(
        r"FORECAST VALID\s+(\d{2})/(\d{4})Z\s+(\d+\.?\d*[NS])\s+(\d+\.?\d*[EW])",
        re.IGNORECASE,
    )
    wind_after_re = re.compile(r"MAX WIND:.*?\((\d+)\s*KT\)", re.IGNORECASE)

    year, month = now_utc.year, now_utc.month
    for m in fc_re.finditer(text):
        day_fc = int(m.group(1))
        hhmm   = m.group(2)
        lat_fc, lon_fc = _parse_latlon(m.group(3), m.group(4))
        hour_fc = int(hhmm[:2])
        min_fc  = int(hhmm[2:]) if len(hhmm) == 4 else 0

        fc_month, fc_year = month, year
        if day_fc < now_utc.day and (now_utc.day - day_fc) > 20:
            fc_month = month % 12 + 1
            if fc_month == 1:
                fc_year += 1

        try:
            fc_dt = datetime(fc_year, fc_month, day_fc, hour_fc, min_fc,
                             tzinfo=timezone.utc)
        except ValueError:
            continue

        tau_h = round((fc_dt - now_utc).total_seconds() / 3600)
        if tau_h < 0:
            continue

        segment = text[m.end(): m.end() + 120]
        wind_fc = wind_after_re.search(segment)

        positions.append({
            "lat": lat_fc, "lon": lon_fc,
            "wind_kt": int(wind_fc.group(1)) if wind_fc else None,
            "pressure_hpa": None,
            "tau": tau_h,
            "is_forecast": True,
            "forecast_hour": tau_h,
        })

    return positions


# ---------------------------------------------------------------------------
# Category classifier
# ---------------------------------------------------------------------------

def wind_category(wind_kt: int | None) -> str:
    if wind_kt is None or wind_kt < 34:
        return "TD"
    if wind_kt < 48:
        return "TS"
    if wind_kt < 64:
        return "STS"
    if wind_kt < 130:
        return "TY"
    return "STY"


# ---------------------------------------------------------------------------
# Convert positions → DB rows
# ---------------------------------------------------------------------------

def positions_to_db_rows(positions: list[dict], storm_pk: int,
                          now_utc: datetime) -> list[dict]:
    """
    Convert parsed position dicts to storm_positions row dicts for Supabase upsert.
    location is stored as WKT POINT string; PostGIS accepts this via supabase-py.
    """
    rows = []
    fetched_at = now_utc.isoformat()
    for p in positions:
        rows.append({
            "storm_id":      storm_pk,
            "recorded_at":   now_utc.isoformat() if not p["is_forecast"] else
                             _tau_to_iso(now_utc, p["tau"]),
            "location":      f"POINT({p['lon']} {p['lat']})",
            "wind_kt":       p["wind_kt"],
            "pressure_hpa":  p.get("pressure_hpa"),
            "category":      wind_category(p["wind_kt"]),
            "is_forecast":   p["is_forecast"],
            "forecast_hour": p["forecast_hour"],
            "fetched_at":    fetched_at,
        })
    return rows


def _tau_to_iso(now_utc: datetime, tau_hours: int) -> str:
    from datetime import timedelta
    return (now_utc + timedelta(hours=tau_hours)).isoformat()


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _fetch(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run():
    """Main entry point, called by GitHub Actions."""
    client = build_client_from_env()
    writer = SupabaseWriter(client)
    logger = CrawlLogger(client, "jtwc")
    config = CrawlConfig(client, "jtwc")

    log_id = logger.start()
    total = 0
    try:
        now_utc = datetime.now(timezone.utc)

        rss_text = retry_with_backoff(lambda: _fetch(RSS_URL))
        storms = parse_rss(rss_text, now_utc)
        log.info("Active storms: %s", [s["storm_id"] for s in storms])

        # Archive storms no longer in RSS
        if storms:
            active_ids = [s["storm_id"] for s in storms]
            client.table("storms").update({"status": "archived"}).not_.in_(
                "storm_id", active_ids
            ).eq("source", "jtwc").execute()

        for storm in storms:
            # Upsert storm row and retrieve PK
            client.table("storms").upsert({
                "storm_id":     storm["storm_id"],
                "name":         storm["name"],
                "basin":        storm["basin"],
                "source":       "jtwc",
                "status":       "active",
                "last_seen_at": now_utc.isoformat(),
            }, on_conflict="storm_id").execute()

            pk_row = (client.table("storms")
                      .select("id")
                      .eq("storm_id", storm["storm_id"])
                      .single()
                      .execute())
            storm_pk = pk_row.data["id"]

            text = retry_with_backoff(lambda url=storm["text_url"]: _fetch(url))
            positions = parse_warning_text(text, now_utc)
            if not positions:
                log.warning("No positions parsed for %s", storm["storm_id"])
                continue

            rows = positions_to_db_rows(positions, storm_pk, now_utc)
            count = writer.upsert(
                "storm_positions", rows,
                on_conflict="storm_id,recorded_at,is_forecast,forecast_hour"
            )
            total += count
            log.info("%s: upserted %d position rows", storm["storm_id"], count)

        config.update_last_run()
        logger.finish(log_id, total, "success")
        log.info("JTWC crawl complete. %d rows upserted.", total)

    except Exception as exc:
        logger.finish(log_id, total, "error", str(exc))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
