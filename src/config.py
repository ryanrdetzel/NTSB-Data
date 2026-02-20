"""
Central configuration: URLs, table targets, and primary key mappings.
"""

BASE_URL = "https://data.ntsb.gov/avdata"

# Output paths
DB_PATH = "data/ntsb_aviation.db"
TEMP_DIR = "temp"

# Primary tables to extract from the MDB.
# Names must match the MDB table names (case-insensitive lookup is used at
# runtime, but the exact MDB name is needed when two tables differ only in case).
TARGET_TABLES = [
    "events",
    "aircraft",
    "engines",
    "narratives",
    "Events_Sequence",  # MDB name; the empty "seq_of_events" table is a stub
    "Findings",         # MDB name is capitalised
    "injury",
]

# Lookup tables present in the MDB
LOOKUP_TABLES = [
    "ct_seqevt",   # sequence-of-events code lookup (code, meaning)
    "ct_iaids",    # IAIDS cross-reference codes
]

# Primary keys per table — used to build idempotent UPSERT logic.
# Keys are the lowercase/normalised SQLite column names (mdb_adapter lowercases all columns).
TABLE_PRIMARY_KEYS = {
    "events":          ["ev_id"],
    "aircraft":        ["ev_id", "aircraft_key"],
    "engines":         ["ev_id", "aircraft_key", "eng_no"],
    "narratives":      ["ev_id"],
    "Events_Sequence": ["ev_id", "aircraft_key", "occurrence_no"],
    "Findings":        ["ev_id", "aircraft_key", "finding_no"],
    "injury":          ["ev_id", "aircraft_key", "injury_desc"],
}

LOOKUP_PRIMARY_KEYS = {
    "ct_seqevt": ["code"],
    "ct_iaids":  ["ct_name", "code_iaids"],
}

# Explicit column type overrides applied after mdb-export / read_csv.
# Pandas infers most types from CSV, but numeric-looking integer codes and
# counts come out as float64 (because of NaN).  Mapping them to pandas
# nullable "Int64" produces SQLite INTEGER columns instead of REAL.
# "TEXT" entries force a string cast (e.g. user-id fields stored as numbers).
# ---------------------------------------------------------------------------
# Label taxonomy — predefined categories and their allowed values.
# An event can have multiple values per category (e.g. weather: wind + icing).
# Values are validated on insert; use "other" for edge cases.
# ---------------------------------------------------------------------------
LABEL_TAXONOMY: dict[str, list[str]] = {
    # ── Weather & environment ────────────────────────────────────────────────
    "weather": [
        "clear", "wind", "gusts", "crosswind", "windshear", "turbulence",
        "icing", "thunderstorm", "rain", "snow", "fog", "haze",
        "low_ceiling", "low_visibility", "density_altitude", "mountain_wave",
    ],
    "lighting": [
        "day", "night", "dawn", "dusk",
    ],
    "flight_rules": [
        "vfr", "ifr", "vmc", "imc", "svfr", "nvfr",
    ],

    # ── Flight & operation ───────────────────────────────────────────────────
    "phase_of_flight": [
        "preflight", "taxi", "takeoff", "initial_climb", "climb",
        "cruise", "descent", "approach", "landing", "go_around",
        "maneuvering", "hover", "emergency_descent", "other",
    ],
    "operation_type": [
        "part91", "part121", "part135", "part137", "part125",
        "part129", "public_use", "military", "other",
    ],

    # ── Aircraft ─────────────────────────────────────────────────────────────
    "aircraft_category": [
        "sep", "mep", "set", "met",          # single/multi engine piston/turboprop
        "jet", "helicopter", "gyroplane",
        "glider", "balloon", "ultralight", "lsa", "experimental", "other",
    ],
    "engine_type": [
        "reciprocating", "turboprop", "turbojet", "turbofan",
        "turboshaft", "electric", "none",
    ],
    "num_engines": [
        "single", "twin", "multi",
    ],

    # ── Failure & cause ──────────────────────────────────────────────────────
    "cause_category": [
        "mechanical", "weather", "human_factors", "environmental",
        "maintenance", "design", "unknown", "other",
    ],
    "failure_system": [
        "engine", "propeller", "landing_gear", "electrical", "hydraulic",
        "flight_controls", "fuel", "structural", "avionics", "instruments",
        "vacuum", "pitot_static", "autopilot", "other",
    ],
    "human_factors": [
        "pilot_error", "spatial_disorientation", "fuel_management",
        "inadequate_preflight", "loss_of_control",
        "controlled_flight_into_terrain", "vfr_into_imc",
        "improper_decision", "fatigue", "impairment", "distraction",
        "crew_coordination", "atc_error", "maintenance_error", "other",
    ],

    # ── People ───────────────────────────────────────────────────────────────
    "pilot_certificate": [
        "student", "sport", "recreational", "private", "commercial", "atp",
    ],
    "pilot_experience": [
        "student", "low_time", "moderate", "experienced", "high_time",
    ],

    # ── Outcome ──────────────────────────────────────────────────────────────
    "injury_severity": [
        "fatal", "serious", "minor", "none",
    ],
    "damage_level": [
        "destroyed", "substantial", "minor", "none",
    ],

    # ── Location ─────────────────────────────────────────────────────────────
    "location_type": [
        "airport", "off_airport", "water", "mountain", "urban", "rural", "remote",
    ],
    "altitude": [
        "ground", "low", "mid", "high",
    ],

    # ── Review workflow ──────────────────────────────────────────────────────
    "reviewed": [
        "yes", "partial", "no",
    ],
}

# Explicit column type overrides applied after mdb-export / read_csv.
# Pandas infers most types from CSV, but numeric-looking integer codes and
# counts come out as float64 (because of NaN).  Mapping them to pandas
# nullable "Int64" produces SQLite INTEGER columns instead of REAL.
# "TEXT" entries force a string cast (e.g. user-id fields stored as numbers).
COLUMN_TYPES: dict[str, str] = {
    # ── events ──────────────────────────────────────────────────────────────
    "ev_time":        "Int64",   # HHMM military time  (e.g. 1907.0 → 1907)
    "inj_tot_t":      "Int64",   # total injuries
    "inj_f_grnd":     "Int64",   # fatal ground injuries
    "inj_m_grnd":     "Int64",   # minor ground injuries
    "inj_s_grnd":     "Int64",   # serious ground injuries
    "wx_obs_time":    "Int64",   # observation time HHMM
    "wx_obs_dir":     "Int64",   # wind direction in degrees
    "wx_brief_comp":  "Int64",   # weather briefing completion code

    # ── aircraft ─────────────────────────────────────────────────────────────
    "fc_seats":         "Int64",
    "cc_seats":         "Int64",
    "pax_seats":        "Int64",
    "total_seats":      "Int64",
    "num_eng":          "Int64",
    "acft_year":        "Int64",
    "acft_reg_cls":     "Int64",
    "dprt_time":        "Int64",  # departure time HHMM
    "dprt_timezn":      "Int64",  # departure timezone code
    "dest_same_local":  "Int64",  # boolean flag
    "phase_flt_spec":   "Int64",  # flight phase code
    "report_to_icao":   "Int64",  # boolean flag
    "oper_same":        "Int64",  # boolean flag
    "oper_addr_same":   "Int64",  # boolean flag
    "oprtng_cert":      "Int64",  # operating certificate code
    "oper_cert":        "Int64",  # operator certificate number
    "evacuation":       "Int64",  # boolean flag

    # ── Events_Sequence ───────────────────────────────────────────────────────
    # lchg_userid is a text user-id that mdb-export sometimes exports as a number
    "lchg_userid":    "TEXT",

    # ── injury ───────────────────────────────────────────────────────────────
    "inj_person_count": "Int64",
}
