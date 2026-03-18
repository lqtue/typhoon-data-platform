"""
import_nasa_power.py — One-time NASA POWER rainfall anomaly importer

Reads the processed RainAnomaly.geojson from the 2025Typhoon repo
and loads it into the rainfall_anomalies table in Supabase.

Backfill is idempotent: TRUNCATEs the table then INSERTs all rows.

Usage:
  SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=... \
  python backfill/import_nasa_power.py --geojson path/to/RainAnomaly.geojson
"""

import argparse
import json
import logging
from pathlib import Path

from crawlers.base import SupabaseWriter, build_client_from_env

log = logging.getLogger(__name__)
BATCH_SIZE = 500


def load_geojson(path: str) -> dict:
    return json.loads(Path(path).read_text())


def feature_to_row(feature: dict) -> dict | None:
    props = feature.get("properties", {})
    geom  = feature.get("geometry", {})
    coords = geom.get("coordinates")
    if not coords:
        return None
    lon, lat = coords[0], coords[1]
    return {
        "location":         f"POINT({lon} {lat})",
        "lat":              lat,
        "lon":              lon,
        "date":             props.get("date"),
        "precipitation_mm": props.get("precipitation_mm"),
        "anomaly_mm":       props.get("anomaly_mm"),
        "anomaly_pct":      props.get("anomaly_pct"),
    }


def run(geojson_path: str):
    client = build_client_from_env()
    writer = SupabaseWriter(client)

    fc = load_geojson(geojson_path)
    features = fc.get("features", [])
    log.info("Loaded %d features", len(features))

    rows = [r for f in features if (r := feature_to_row(f)) is not None]
    log.info("Parsed %d valid rows", len(rows))

    # Truncate then batch-insert
    client.rpc("truncate_table", {"table_name": "rainfall_anomalies"}).execute()
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        result = client.table("rainfall_anomalies").insert(batch).execute()
        total += len(result.data) if result.data else 0
    log.info("Inserted %d rainfall_anomalies rows", total)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ap = argparse.ArgumentParser()
    ap.add_argument("--geojson", required=True,
                    help="Path to RainAnomaly.geojson")
    args = ap.parse_args()
    run(args.geojson)
