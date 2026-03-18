"""
import_ibtracs.py — One-time IBTrACS historical storm track importer

Reads the processed historical_tracks.geojson from the 2025Typhoon repo
and loads it into the storms + storm_positions tables in Supabase.

Upserts on storm_id (text) so re-running is safe.
Does NOT overwrite source='jtwc' rows with source='ibtracs' (uses ON CONFLICT DO NOTHING
for the source field: if storm_id already exists with source='jtwc', skip).

Usage:
  SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=... \
  python backfill/import_ibtracs.py --geojson path/to/historical_tracks.geojson
"""

import argparse
import json
import logging
import sys
from datetime import timezone
from pathlib import Path

from crawlers.base import SupabaseWriter, build_client_from_env

log = logging.getLogger(__name__)
BATCH_SIZE = 100


def load_geojson(path: str) -> dict:
    return json.loads(Path(path).read_text())


def feature_to_storm_row(feature: dict) -> dict | None:
    props = feature.get("properties", {})
    sid = props.get("SID")
    if not sid:
        return None
    times = props.get("point_times") or []
    return {
        "storm_id":      sid,
        "name":          (props.get("NAME") or "").upper() or None,
        "basin":         "WP",   # all historical Vietnam-adjacent storms are WP
        "source":        "ibtracs",
        "status":        "archived",
        "first_seen_at": _iso(times[0]) if times else None,
        "last_seen_at":  _iso(times[-1]) if times else None,
    }


def feature_to_position_rows(feature: dict, storm_pk: int) -> list[dict]:
    """Convert a LineString feature to storm_positions rows, one per coordinate."""
    props = feature.get("properties", {})
    coords = feature.get("geometry", {}).get("coordinates", [])
    times = props.get("point_times") or []
    winds = props.get("point_winds") or []
    # Map lowercase IBTrACS category to uppercase DB category
    raw_cat = (props.get("category") or "td").lower()
    cat_map = {"td": "TD", "ts": "TS", "sts": "STS", "ty": "TY", "sty": "STY"}
    category = cat_map.get(raw_cat, "TD")

    rows = []
    for i, (lon, lat) in enumerate(coords):
        recorded_at = _iso(times[i]) if i < len(times) else None
        if not recorded_at:
            continue
        rows.append({
            "storm_id":      storm_pk,
            "recorded_at":   recorded_at,
            "location":      f"POINT({lon} {lat})",
            "wind_kt":       winds[i] if i < len(winds) else None,
            "pressure_hpa":  None,
            "category":      category,
            "is_forecast":   False,
            "forecast_hour": None,
        })
    return rows


def _iso(s: str | None) -> str | None:
    if not s:
        return None
    try:
        from dateutil import parser as dp
        dt = dp.parse(str(s))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return str(s)


def run(geojson_path: str):
    client = build_client_from_env()
    writer = SupabaseWriter(client)

    fc = load_geojson(geojson_path)
    features = fc.get("features", [])
    log.info("Loaded %d features from %s", len(features), geojson_path)

    # Deduplicate storms
    seen_sids = {}
    storm_rows = []
    for feat in features:
        row = feature_to_storm_row(feat)
        if row and row["storm_id"] not in seen_sids:
            seen_sids[row["storm_id"]] = True
            storm_rows.append(row)

    # Upsert storms (skip if already present with source='jtwc')
    for i in range(0, len(storm_rows), BATCH_SIZE):
        batch = storm_rows[i:i + BATCH_SIZE]
        (client.table("storms")
         .upsert(batch, on_conflict="storm_id", ignore_duplicates=True)
         .execute())
    log.info("Upserted %d storm rows", len(storm_rows))

    # Build storm_id → integer PK map
    db_storms = client.table("storms").select("id, storm_id").execute().data
    pk_map = {s["storm_id"]: s["id"] for s in db_storms}

    # Upsert positions
    total = 0
    for feat in features:
        props = feat.get("properties", {})
        sid = props.get("SID")
        if not sid or sid not in pk_map:
            continue
        rows = feature_to_position_rows(feat, pk_map[sid])
        if rows:
            count = writer.upsert(
                "storm_positions", rows,
                on_conflict="storm_id,recorded_at,is_forecast,forecast_hour"
            )
            total += count

    log.info("Upserted %d storm_positions rows", total)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ap = argparse.ArgumentParser()
    ap.add_argument("--geojson", required=True,
                    help="Path to historical_tracks.geojson")
    args = ap.parse_args()
    run(args.geojson)
