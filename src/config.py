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
    "ct_seqevt",        # sequence-of-events code lookup (code, meaning)
    "ct_iaids",         # IAIDS cross-reference codes
    "ct_acft_make",     # aircraft make codes
    "ct_acft_model",    # aircraft model codes
    "ct_inj_level",     # injury severity level codes
    "ct_weather_cond",  # weather condition codes
    "ct_light_cond",    # light condition codes
    "ct_sky_cond",      # sky condition codes
    "ct_far_part",      # FAR part / operation type codes
    "ct_phase_of_flt",  # phase of flight codes
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
    "ct_seqevt":       ["code"],
    "ct_iaids":        ["ct_name", "code_iaids"],
    "ct_acft_make":    ["code"],
    "ct_acft_model":   ["code"],
    "ct_inj_level":    ["code"],
    "ct_weather_cond": ["code"],
    "ct_light_cond":   ["code"],
    "ct_sky_cond":     ["code"],
    "ct_far_part":     ["code"],
    "ct_phase_of_flt": ["code"],
}
