-- 004_rpc_functions.sql
-- Helper RPC for NCHMF and NASA POWER truncate-and-insert pattern.
-- SECURITY DEFINER runs as the migration user (service role) — only callable
-- with the service role key, never via the anon/public role.

CREATE OR REPLACE FUNCTION truncate_table(table_name TEXT)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  EXECUTE 'TRUNCATE TABLE ' || quote_ident(table_name) || ' RESTART IDENTITY CASCADE';
END;
$$;

-- Row-Level Security: read-only public access; write via service role only.
-- Enable RLS on all tables (run these after table creation):
ALTER TABLE storms              ENABLE ROW LEVEL SECURITY;
ALTER TABLE storm_positions     ENABLE ROW LEVEL SECURITY;
ALTER TABLE storm_wind_radii    ENABLE ROW LEVEL SECURITY;
ALTER TABLE water_stations      ENABLE ROW LEVEL SECURITY;
ALTER TABLE water_levels        ENABLE ROW LEVEL SECURITY;
ALTER TABLE lakes               ENABLE ROW LEVEL SECURITY;
ALTER TABLE lake_levels         ENABLE ROW LEVEL SECURITY;
ALTER TABLE flood_warnings      ENABLE ROW LEVEL SECURITY;
ALTER TABLE rainfall_anomalies  ENABLE ROW LEVEL SECURITY;
ALTER TABLE crawl_config        ENABLE ROW LEVEL SECURITY;
ALTER TABLE crawl_log           ENABLE ROW LEVEL SECURITY;

-- Public read access for data tables
CREATE POLICY "public_read_storms"           ON storms              FOR SELECT USING (true);
CREATE POLICY "public_read_storm_positions"  ON storm_positions     FOR SELECT USING (true);
CREATE POLICY "public_read_storm_wind_radii" ON storm_wind_radii    FOR SELECT USING (true);
CREATE POLICY "public_read_water_stations"   ON water_stations      FOR SELECT USING (true);
CREATE POLICY "public_read_water_levels"     ON water_levels        FOR SELECT USING (true);
CREATE POLICY "public_read_lakes"            ON lakes               FOR SELECT USING (true);
CREATE POLICY "public_read_lake_levels"      ON lake_levels         FOR SELECT USING (true);
CREATE POLICY "public_read_flood_warnings"   ON flood_warnings      FOR SELECT USING (true);
CREATE POLICY "public_read_rainfall"         ON rainfall_anomalies  FOR SELECT USING (true);
-- crawl_config and crawl_log: service role only (no public policy)

CREATE OR REPLACE FUNCTION storms_in_watch_zone(
    lon_min FLOAT, lon_max FLOAT,
    lat_min FLOAT, lat_max FLOAT
)
RETURNS TABLE (storm_id TEXT, name TEXT, wind_kt INTEGER)
LANGUAGE sql
STABLE
AS $$
    SELECT DISTINCT s.storm_id, s.name, sp.wind_kt
    FROM storms s
    JOIN storm_positions sp ON sp.storm_id = s.id
    WHERE s.status = 'active'
      AND sp.is_forecast = FALSE
      AND ST_X(sp.location::geometry) BETWEEN lon_min AND lon_max
      AND ST_Y(sp.location::geometry) BETWEEN lat_min AND lat_max
    ORDER BY sp.wind_kt DESC NULLS LAST;
$$;
