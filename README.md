# typhoon-data-platform

Backend data platform for VnExpress adverse weather coverage. Crawls live weather APIs into Supabase (PostgreSQL + PostGIS) and exposes a unified REST API consumed by the [2025Typhoon](https://github.com/VnExpress-Spotlight/2025Typhoon) frontend and other newsroom apps.

---

## Sources

| Source | Data | Frequency |
|---|---|---|
| [JTWC](https://www.metoc.navy.mil/jtwc/jtwc.html) | Active typhoon positions + forecast track | Hourly (30min in alert mode) |
| [JMA RSMC Tokyo](https://www.jma.go.jp/bosai/typhoon/) | Active typhoon positions (fallback) | Hourly (30min in alert mode) |
| [VNDMS](https://vndms.dmptc.gov.vn) | River water level stations + hourly readings | Hourly |
| [Thuy Loi Vietnam](http://e15.thuyloivietnam.vn) | Lake levels, storage, inflow/outflow | Hourly |
| [NCHMF](https://luquetsatlo.nchmf.gov.vn) | Landslide + flood warning polygons | Hourly (full snapshot replace) |
| IBTrACS | Historical storm tracks (~140 Vietnam landfalls) | One-time backfill |
| NASA POWER | Historical rainfall anomaly grid | One-time backfill |

---

## Repo Structure

```
typhoon-data-platform/
├── supabase/migrations/
│   ├── 001_initial_schema.sql       # All tables + PostGIS
│   ├── 002_crawl_config_seeds.sql   # crawl_config + crawl_log + seed rows
│   ├── 003_indexes.sql              # Spatial + time-series indexes
│   └── 004_rpc_functions.sql        # truncate_table RPC, storms_in_watch_zone RPC, RLS policies
├── crawlers/
│   ├── base.py                      # SupabaseWriter, CrawlLogger, CrawlConfig, retry
│   ├── jtwc.py
│   ├── jma.py
│   ├── vndms.py
│   ├── thuyloivietnam.py
│   └── nchmf.py
├── backfill/
│   ├── import_ibtracs.py            # One-time: load historical_tracks.geojson
│   └── import_nasa_power.py         # One-time: load RainAnomaly.geojson
├── tests/
├── .github/workflows/
│   ├── crawl-typhoon.yml            # */30 cron — JTWC + JMA
│   ├── crawl-water.yml              # Hourly — VNDMS + Thuy Loi
│   ├── crawl-warnings.yml           # Hourly — NCHMF
│   ├── detect-alert-mode.yml        # After each typhoon crawl — watch zone check
│   └── confirm-alert-mode.yml       # Manual — human confirms alert escalation
├── requirements.txt
└── requirements-dev.txt
```

---

## Alert Mode

Crawl frequency adapts automatically when a storm approaches Vietnam.

```
NORMAL ──► ARMED (auto) ──► ALERT (human confirms) ──► NORMAL (auto-disarm)
```

- **NORMAL → ARMED**: after each JTWC crawl, `detect-alert-mode.yml` checks if any active storm is inside the watch zone (95–120°E, 5–28°N). If yes, a GitHub issue is opened requesting confirmation.
- **ARMED → ALERT**: a team member runs the `confirm-alert-mode` workflow (`workflow_dispatch`). JTWC + JMA switch to 30-minute crawl intervals.
- **ALERT → NORMAL**: automatically disarms when no storms appear in the watch zone for 6 consecutive hours.
- **ARMED → NORMAL (timeout)**: if no one confirms within 48 hours, the armed state clears automatically and the issue closes.

---

## Setup

### Prerequisites

- Supabase project with PostGIS extension enabled (Dashboard → Database → Extensions → `postgis`)
- GitHub repository with Actions enabled

### 1. Run migrations

In the Supabase SQL editor, run in order:

```
supabase/migrations/001_initial_schema.sql
supabase/migrations/002_crawl_config_seeds.sql
supabase/migrations/003_indexes.sql
supabase/migrations/004_rpc_functions.sql
```

### 2. Add GitHub Actions secrets

In the repository Settings → Secrets and variables → Actions:

| Secret | Value |
|---|---|
| `SUPABASE_URL` | `https://<project-ref>.supabase.co` |
| `SUPABASE_SERVICE_ROLE_KEY` | Service role key from Supabase project settings |

### 3. Create GitHub label

Create a label named `alert-mode-request` in the repository. This is used by the alert mode workflows for deduplication.

### 4. Run backfill scripts (one-time)

```bash
pip install -r requirements.txt

# Historical storm tracks (~140 Vietnam landfalls from IBTrACS)
SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=... \
  python backfill/import_ibtracs.py \
  --geojson ../2025Typhoon/data/historical_tracks.geojson

# NASA POWER rainfall anomaly grid
SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=... \
  python backfill/import_nasa_power.py \
  --geojson ../2025Typhoon/rain/RainAnomaly.geojson
```

### 5. Verify

Trigger the `crawl-typhoon` and `crawl-water` workflows manually from the Actions tab to confirm end-to-end connectivity.

---

## Local Development

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest tests/
```

Copy `.env.example` to `.env` and fill in credentials to run crawlers locally:

```bash
source .env
python -m crawlers.jtwc     # or any crawler module
```

---

## API

Supabase auto-generates a REST API for all tables. Key endpoints:

```
GET /rest/v1/storms?status=eq.active
GET /rest/v1/storm_positions?storm_id=eq.{id}&is_forecast=eq.false&order=recorded_at.asc
GET /rest/v1/storm_positions?storm_id=eq.{id}&is_forecast=eq.true&order=forecast_hour.asc
GET /rest/v1/water_levels?station_id=eq.{id}&order=recorded_at.desc&limit=48
GET /rest/v1/flood_warnings?severity=in.(high,very_high)
GET /rest/v1/rainfall_anomalies?date=eq.{YYYY-MM-DD}
```

All tables have public read access (RLS). Writes require the service role key (crawlers only).

---

## Notes

- The Thuy Loi Vietnam endpoint (`e15.thuyloivietnam.vn`) uses HTTP, not HTTPS. Treat as accepted risk until the operator upgrades the endpoint.
- Crawl intervals in `crawl_config` are initial defaults. Tune after observing real source cadences.
- The `truncate_table` RPC used by the NCHMF crawler can truncate any table — restrict to allowed tables if the service role key is ever shared with less-trusted services.
