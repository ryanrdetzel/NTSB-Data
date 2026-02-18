"""
Central configuration: URLs, table targets, and primary key mappings.
"""

BASE_URL = "https://data.ntsb.gov/avdata"

# Output paths
DB_PATH = "data/ntsb_aviation.db"
TEMP_DIR = "temp"

# Primary tables to extract from the MDB
TARGET_TABLES = [
    "events",
    "aircraft",
    "engines",
    "narratives",
    "seq_of_events",
    "findings",
    "injury",
]

# Lookup (ct_*) tables to extract
LOOKUP_TABLES = [
    "ct_acft_make",
    "ct_acft_model",
    "ct_inj_level",
    "ct_weather_cond",
    "ct_occurrences",
    "ct_phase_of_flt",
    "ct_seq_of_events",
    "ct_accident_cause",
]

# Primary keys per table — used to build idempotent UPSERT logic
TABLE_PRIMARY_KEYS = {
    "events":         ["ev_id"],
    "aircraft":       ["ev_id", "aircraft_key"],
    "engines":        ["ev_id", "aircraft_key", "eng_no"],
    "narratives":     ["ev_id"],
    "seq_of_events":  ["ev_id", "aircraft_key", "occurrence_no"],
    "findings":       ["ev_id", "aircraft_key", "finding_no"],
    "injury":         ["ev_id", "aircraft_key", "injury_desc"],
}

LOOKUP_PRIMARY_KEYS = {
    "ct_acft_make":      ["make_name"],
    "ct_acft_model":     ["make_name", "model_name"],
    "ct_inj_level":      ["inj_level"],
    "ct_weather_cond":   ["weather_cond_code"],
    "ct_occurrences":    ["occurrence_code"],
    "ct_phase_of_flt":   ["phase_flt_code"],
    "ct_seq_of_events":  ["seq_event_code"],
    "ct_accident_cause": ["cause_factor"],
}
