# NTSB Aviation Database ETL

A local, self-hosted SQLite database of NTSB aviation accident data, built with a Python ETL pipeline. Supports a one-time full seed and idempotent weekly incremental updates.

## Prerequisites

**System (required):**
```
# Ubuntu / Debian
sudo apt install mdbtools

# macOS
brew install mdbtools
```

**Python 3.10+:**
```
pip install -r requirements.txt
```

## Quick Start

### 1. Initial Seed (~400 MB, run once)

Downloads `avall.zip` from the NTSB and builds the full database from 1982 to present.

```bash
python main.py --seed
```

Force overwrite an existing database:

```bash
python main.py --seed --force
```

### 2. Weekly Update

Checks the NTSB server for new `upXXMMM.zip` files not yet applied and upserts them. Safe to run repeatedly — already-applied files are skipped.

```bash
python main.py --update
```

Recommended schedule: **every Wednesday** (NTSB releases vary).

### Custom database path

```bash
python main.py --seed   --db /path/to/custom.db
python main.py --update --db /path/to/custom.db
```

## Directory Structure

```
ntsb_etl/
├── data/
│   └── ntsb_aviation.db      # Output database (gitignored)
├── temp/                     # Transient downloads (gitignored)
├── src/
│   ├── config.py             # URLs, table names, primary keys
│   ├── db.py                 # SQLite helpers, UPSERT logic, views
│   ├── downloader.py         # NTSB scraper & file fetcher
│   ├── mdb_adapter.py        # mdbtools wrapper → pandas
│   └── orchestrator.py       # seed() and update() workflows
├── requirements.txt
└── main.py                   # CLI entrypoint
```

## Database Schema

### Primary Tables

| Table | Primary Key | Description |
|-------|------------|-------------|
| `events` | `ev_id` | Central fact table — location, date, weather, injury totals |
| `aircraft` | `ev_id, aircraft_key` | Aircraft involved (make, model, registration) |
| `engines` | `ev_id, aircraft_key, eng_no` | Powerplant details |
| `narratives` | `ev_id` | Factual narrative and probable cause text |
| `seq_of_events` | `ev_id, aircraft_key, occurrence_no` | Chain-of-events codes |
| `findings` | `ev_id, aircraft_key, finding_no` | Finding codes |
| `injury` | `ev_id, aircraft_key, injury_desc` | Injury records |

### Lookup Tables (`ct_*`)

Decode integer codes used in primary tables: `ct_acft_make`, `ct_acft_model`, `ct_inj_level`, `ct_weather_cond`, and more.

### Views

`v_full_report` — joins events, aircraft, and narratives into a single queryable view:

```sql
SELECT * FROM v_full_report WHERE ev_date >= '2020-01-01';
```

## Sync Tracking

The `_meta_sync` table records every zip file applied, preventing re-processing on subsequent `--update` runs.

```sql
SELECT * FROM _meta_sync ORDER BY processed_at DESC;
```
