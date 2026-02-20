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

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate   # macOS / Linux
# .venv\Scripts\activate    # Windows
```

Install dependencies:

```bash
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
│   ├── labels.py             # User tag & label CRUD operations
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

## Tags & Labels

Annotate event records with your own metadata for richer querying.

**Tags** are simple strings (e.g. `reviewed`, `weather-related`).
**Labels** are key-value pairs (e.g. `severity=high`, `root_cause=engine`).

Both are stored in separate tables (`user_tags`, `user_labels`) that are never overwritten by ETL updates.

### Managing Tags

```bash
# Add one or more tags to an event
python main.py tag add ERA20LA123 reviewed weather-related

# Remove a tag
python main.py tag rm ERA20LA123 weather-related

# List all tags and how many events use each
python main.py tag ls

# Find all events with a specific tag
python main.py tag find reviewed
```

### Managing Labels

```bash
# Set a label (key=value) on an event — overwrites previous value for that key
python main.py label set ERA20LA123 severity high
python main.py label set ERA20LA123 root_cause engine_failure

# Remove a label
python main.py label rm ERA20LA123 severity

# List all label name/value combinations and their counts
python main.py label ls

# Find events by label (optionally filter by value)
python main.py label find severity
python main.py label find severity high
```

### Browsing & Viewing Events

```bash
# Show full detail for a single event (including tags and labels)
python main.py show ERA20LA123

# Browse events (most recent first, 20 per page)
python main.py browse
python main.py browse --limit 50 --offset 20

# Filter by date range
python main.py browse --date-from 2020-01-01 --date-to 2020-12-31

# Filter by tag or label
python main.py browse --tag reviewed
python main.py browse --label-name severity --label-value high

# Show only events you haven't tagged yet
python main.py browse --untagged
```

### Querying with SQL

The `v_labeled_report` view joins events with their tags and labels:

```sql
SELECT * FROM v_labeled_report WHERE tags LIKE '%reviewed%';
SELECT * FROM v_labeled_report WHERE labels LIKE '%severity=high%';
```

## Sync Tracking

The `_meta_sync` table records every zip file applied, preventing re-processing on subsequent `--update` runs.

```sql
SELECT * FROM _meta_sync ORDER BY processed_at DESC;
```
