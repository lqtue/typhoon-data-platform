-- 001_initial_schema.sql
-- Requires: PostGIS extension (enable in Supabase dashboard under Extensions)

-- ============================================================
-- TYPHOON DOMAIN
-- ============================================================

CREATE TABLE IF NOT EXISTS storms (
  id            BIGSERIAL PRIMARY KEY,
  storm_id      TEXT UNIQUE NOT NULL,
  name          TEXT,
  basin         TEXT,
  source        TEXT NOT NULL,                   -- jtwc | jma | ibtracs
  status        TEXT NOT NULL DEFAULT 'active',  -- active | archived
  first_seen_at TIMESTAMPTZ,
  last_seen_at  TIMESTAMPTZ
);

-- recorded_at semantics:
--   is_forecast=FALSE: observation valid time (UTC)
--   is_forecast=TRUE:  forecast valid time (NOT issue time)
-- storm_id here is storms.id (integer PK), not storms.storm_id (text).
CREATE TABLE IF NOT EXISTS storm_positions (
  id            BIGSERIAL PRIMARY KEY,
  storm_id      BIGINT NOT NULL REFERENCES storms(id) ON DELETE CASCADE,
  recorded_at   TIMESTAMPTZ NOT NULL,
  location      GEOMETRY(Point, 4326) NOT NULL,
  wind_kt       INTEGER,
  pressure_hpa  INTEGER,
  category      TEXT,                            -- TD | TS | STS | TY | STY
  is_forecast   BOOLEAN NOT NULL DEFAULT FALSE,
  forecast_hour INTEGER,                         -- NULL=best track, 12/24/48/72/96/120
  fetched_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (storm_id, recorded_at, is_forecast, forecast_hour)
);

CREATE TABLE IF NOT EXISTS storm_wind_radii (
  id                BIGSERIAL PRIMARY KEY,
  position_id       BIGINT NOT NULL REFERENCES storm_positions(id) ON DELETE CASCADE,
  wind_threshold_kt INTEGER NOT NULL,
  ne_nm             INTEGER,
  se_nm             INTEGER,
  sw_nm             INTEGER,
  nw_nm             INTEGER
);

-- ============================================================
-- WATER DOMAIN
-- ============================================================

CREATE TABLE IF NOT EXISTS water_stations (
  id              BIGSERIAL PRIMARY KEY,
  station_code    TEXT UNIQUE NOT NULL,
  name            TEXT,
  river           TEXT,
  basin           TEXT,
  province        TEXT,
  location        GEOMETRY(Point, 4326),
  alert_level_1_m NUMERIC,
  alert_level_2_m NUMERIC,
  alert_level_3_m NUMERIC,
  source          TEXT NOT NULL DEFAULT 'vndms'
);

-- source inherited from parent water_stations.source row
CREATE TABLE IF NOT EXISTS water_levels (
  id           BIGSERIAL PRIMARY KEY,
  station_id   BIGINT NOT NULL REFERENCES water_stations(id) ON DELETE CASCADE,
  recorded_at  TIMESTAMPTZ NOT NULL,
  level_m      NUMERIC,
  alert_status TEXT,                             -- normal | level1 | level2 | level3
  fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (station_id, recorded_at)
);

CREATE TABLE IF NOT EXISTS lakes (
  id                  BIGSERIAL PRIMARY KEY,
  lake_code           TEXT UNIQUE NOT NULL,
  name                TEXT,
  province            TEXT,
  location            GEOMETRY(Point, 4326),
  capacity_million_m3 NUMERIC
);

-- source inherited from parent lakes row (Thuy Loi Vietnam)
CREATE TABLE IF NOT EXISTS lake_levels (
  id                 BIGSERIAL PRIMARY KEY,
  lake_id            BIGINT NOT NULL REFERENCES lakes(id) ON DELETE CASCADE,
  recorded_at        TIMESTAMPTZ NOT NULL,
  level_m            NUMERIC,
  storage_million_m3 NUMERIC,
  inflow_m3s         NUMERIC,
  outflow_m3s        NUMERIC,
  fetched_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (lake_id, recorded_at)
);

-- ============================================================
-- WARNING DOMAIN
-- ============================================================

-- Replacement strategy: TRUNCATE + INSERT inside a transaction each crawl cycle.
-- boundary uses GEOMETRY(Geometry, 4326) to accept both Polygon and MultiPolygon.
CREATE TABLE IF NOT EXISTS flood_warnings (
  id           BIGSERIAL PRIMARY KEY,
  ward_code    TEXT,
  ward_name    TEXT,
  district     TEXT,
  province     TEXT,
  warning_type TEXT,                             -- landslide | flash_flood | waterlogging
  severity     TEXT,                             -- very_high | high | medium | low
  valid_from   TIMESTAMPTZ,
  valid_until  TIMESTAMPTZ,
  boundary     GEOMETRY(Geometry, 4326),
  fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- HISTORICAL DOMAIN (backfill only)
-- ============================================================

CREATE TABLE IF NOT EXISTS rainfall_anomalies (
  id               BIGSERIAL PRIMARY KEY,
  location         GEOMETRY(Point, 4326) NOT NULL,
  lat              NUMERIC(8,4) NOT NULL,
  lon              NUMERIC(8,4) NOT NULL,
  date             DATE NOT NULL,
  precipitation_mm NUMERIC,
  anomaly_mm       NUMERIC,
  anomaly_pct      NUMERIC,
  UNIQUE (lat, lon, date)
);
