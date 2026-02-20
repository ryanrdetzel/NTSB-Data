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
│   ├── labels.py             # User label CRUD & query operations
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

## Labels

Annotate events with structured `category:value` labels for fast, queryable classification. An event can carry multiple values per category (e.g. both `weather:wind` and `weather:icing`). Labels are stored in `user_labels` and never overwritten by ETL updates.

### Label Taxonomy

Labels are validated against a predefined taxonomy. Run `python main.py categories` to see all categories and allowed values. The full set:

| Category | Values |
|----------|--------|
| `weather` | clear, wind, gusts, crosswind, windshear, turbulence, icing, thunderstorm, rain, snow, fog, haze, low_ceiling, low_visibility, density_altitude, mountain_wave |
| `lighting` | day, night, dawn, dusk |
| `flight_rules` | vfr, ifr, vmc, imc, svfr, nvfr |
| `phase_of_flight` | preflight, taxi, takeoff, initial_climb, climb, cruise, descent, approach, landing, go_around, maneuvering, hover, emergency_descent, other |
| `operation_type` | part91, part121, part135, part137, part125, part129, public_use, military, other |
| `aircraft_category` | sep, mep, set, met, jet, helicopter, gyroplane, glider, balloon, ultralight, lsa, experimental, other |
| `engine_type` | reciprocating, turboprop, turbojet, turbofan, turboshaft, electric, none |
| `num_engines` | single, twin, multi |
| `cause_category` | mechanical, weather, human_factors, environmental, maintenance, design, unknown, other |
| `failure_system` | engine, propeller, landing_gear, electrical, hydraulic, flight_controls, fuel, structural, avionics, instruments, vacuum, pitot_static, autopilot, other |
| `human_factors` | pilot_error, spatial_disorientation, fuel_management, inadequate_preflight, loss_of_control, controlled_flight_into_terrain, vfr_into_imc, improper_decision, fatigue, impairment, distraction, crew_coordination, atc_error, maintenance_error, other |
| `pilot_certificate` | student, sport, recreational, private, commercial, atp |
| `pilot_experience` | student, low_time, moderate, experienced, high_time |
| `injury_severity` | fatal, serious, minor, none |
| `damage_level` | destroyed, substantial, minor, none |
| `location_type` | airport, off_airport, water, mountain, urban, rural, remote |
| `altitude` | ground, low, mid, high |
| `reviewed` | yes, partial, no |

### Managing Labels

```bash
# Add labels — multiple values per category in one command
python main.py label add ERA20LA123 weather wind icing
python main.py label add ERA20LA123 flight_rules imc
python main.py label add ERA20LA123 phase_of_flight approach
python main.py label add ERA20LA123 failure_system engine
python main.py label add ERA20LA123 aircraft_category sep
python main.py label add ERA20LA123 num_engines single

# Remove a specific value
python main.py label rm ERA20LA123 weather icing

# Remove ALL values in a category
python main.py label rm ERA20LA123 weather

# List all applied labels and their counts
python main.py label ls

# Find events by label
python main.py label find flight_rules imc
python main.py label find failure_system engine
```

### Browsing & Viewing Events

```bash
# Show full event detail including all labels
python main.py show ERA20LA123

# Browse events (most recent first, 20 per page)
python main.py browse
python main.py browse --limit 50 --offset 20

# Filter by date range
python main.py browse --date-from 2020-01-01 --date-to 2020-12-31

# Filter by label
python main.py browse --category flight_rules --value imc

# Show only events not yet labeled
python main.py browse --unlabeled
```

### Counting & Querying

```bash
# How many accidents happened in IMC with engine failure?
python main.py count flight_rules:imc failure_system:engine

# How many takeoff accidents in single-engine piston planes?
python main.py count phase_of_flight:takeoff num_engines:single aircraft_category:sep

# How many fatal icing accidents?
python main.py count weather:icing injury_severity:fatal
```

### SQL Queries

The `v_labeled_report` view joins events with their labels:

```sql
-- All labeled events with their labels as a comma-separated string
SELECT * FROM v_labeled_report WHERE labels IS NOT NULL;

-- Direct queries against user_labels for precise filtering
SELECT e.ev_id, e.ev_date, e.ev_city, e.ev_state
FROM events e
WHERE EXISTS (SELECT 1 FROM user_labels ul WHERE ul.ev_id = e.ev_id
              AND ul.category = 'flight_rules' AND ul.value = 'imc')
  AND EXISTS (SELECT 1 FROM user_labels ul WHERE ul.ev_id = e.ev_id
              AND ul.category = 'failure_system' AND ul.value = 'engine');
```

## Sync Tracking

The `_meta_sync` table records every zip file applied, preventing re-processing on subsequent `--update` runs.

```sql
SELECT * FROM _meta_sync ORDER BY processed_at DESC;
```
