-- 002_crawl_config_seeds.sql

CREATE TABLE IF NOT EXISTS crawl_config (
  source_name             TEXT PRIMARY KEY,
  normal_interval_min     INTEGER NOT NULL DEFAULT 60,
  alert_interval_min      INTEGER NOT NULL DEFAULT 30,
  is_alert_mode           BOOLEAN NOT NULL DEFAULT FALSE,
  alert_armed_at          TIMESTAMPTZ,
  alert_armed_expires_at  TIMESTAMPTZ,        -- auto-dismiss deadline (armed_at + 48h)
  alert_confirmed_by      TEXT,
  last_run_at             TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS crawl_log (
  id               BIGSERIAL PRIMARY KEY,
  source_name      TEXT NOT NULL REFERENCES crawl_config(source_name),
  started_at       TIMESTAMPTZ NOT NULL,
  completed_at     TIMESTAMPTZ,
  records_upserted INTEGER,
  status           TEXT,   -- running | success | error | partial
  error_message    TEXT
);

-- Idempotent seed data. Re-running this migration is safe.
INSERT INTO crawl_config (source_name, normal_interval_min, alert_interval_min)
VALUES
  ('jtwc',           60, 30),
  ('jma',            60, 30),
  ('vndms',          60, 60),
  ('thuyloivietnam', 60, 60),
  ('nchmf',          60, 60)
ON CONFLICT (source_name) DO NOTHING;
-- Note: intervals are initial defaults; tune after observing real source cadences.
