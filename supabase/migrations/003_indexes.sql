-- 003_indexes.sql

CREATE INDEX IF NOT EXISTS idx_storm_positions_storm_recorded
  ON storm_positions (storm_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_storm_positions_location
  ON storm_positions USING GIST (location);
CREATE INDEX IF NOT EXISTS idx_storm_positions_active
  ON storm_positions (storm_id, is_forecast, forecast_hour)
  WHERE is_forecast = FALSE;

CREATE INDEX IF NOT EXISTS idx_water_levels_station_time
  ON water_levels (station_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_water_stations_location
  ON water_stations USING GIST (location);

CREATE INDEX IF NOT EXISTS idx_lake_levels_lake_time
  ON lake_levels (lake_id, recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_flood_warnings_boundary
  ON flood_warnings USING GIST (boundary);
CREATE INDEX IF NOT EXISTS idx_flood_warnings_severity
  ON flood_warnings (severity, valid_until);

CREATE INDEX IF NOT EXISTS idx_rainfall_anomalies_date
  ON rainfall_anomalies (date);
CREATE INDEX IF NOT EXISTS idx_rainfall_anomalies_location
  ON rainfall_anomalies USING GIST (location);

CREATE INDEX IF NOT EXISTS idx_crawl_log_source_time
  ON crawl_log (source_name, started_at DESC);
