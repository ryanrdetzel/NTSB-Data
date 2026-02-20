"""
SQLite connection management, schema helpers, and idempotent UPSERT logic.
"""

import sqlite3
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_connection(db_path: str) -> sqlite3.Connection:
    """Open (and create if needed) a SQLite database at db_path."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    # WAL mode for better concurrent read performance
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# ---------------------------------------------------------------------------
# Metadata / sync tracking
# ---------------------------------------------------------------------------

def init_meta_table(conn: sqlite3.Connection) -> None:
    """Create the sync-tracking table if it does not already exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _meta_sync (
            filename     TEXT PRIMARY KEY,
            processed_at TEXT DEFAULT CURRENT_TIMESTAMP,
            record_count INTEGER DEFAULT 0
        )
    """)
    conn.commit()


def get_processed_files(conn: sqlite3.Connection) -> set[str]:
    """Return the set of zip filenames already applied to this database."""
    cur = conn.execute("SELECT filename FROM _meta_sync")
    return {row[0] for row in cur.fetchall()}


def log_processed_file(conn: sqlite3.Connection, filename: str, record_count: int = 0) -> None:
    """Record a successfully processed zip file."""
    conn.execute(
        "INSERT OR REPLACE INTO _meta_sync (filename, record_count) VALUES (?, ?)",
        (filename, record_count),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# User tags & labels
# ---------------------------------------------------------------------------

def init_user_tables(conn: sqlite3.Connection) -> None:
    """Create the user_tags and user_labels tables if they do not exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_tags (
            ev_id      TEXT NOT NULL,
            tag        TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ev_id, tag)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_labels (
            ev_id       TEXT NOT NULL,
            label_name  TEXT NOT NULL,
            label_value TEXT NOT NULL,
            created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ev_id, label_name)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_user_tags_tag ON user_tags(tag)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_user_labels_name ON user_labels(label_name)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_user_labels_name_value ON user_labels(label_name, label_value)"
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Load helpers
# ---------------------------------------------------------------------------

def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
    )
    return cur.fetchone() is not None


def replace_dataframe(conn: sqlite3.Connection, df: pd.DataFrame, table_name: str) -> None:
    """Full table replace — used for the initial seed and lookup tables."""
    if df.empty:
        return
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.commit()


def upsert_dataframe(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
    table_name: str,
    primary_keys: list[str],
) -> None:
    """
    Idempotent row-level UPSERT for a target table.

    Strategy:
      1. Write incoming data to a temporary table.
      2. DELETE rows from the target that share primary-key values with the
         incoming data (avoids duplicate / stale rows).
      3. INSERT all rows from the temp table into the target.

    This approach works with tables that have no explicit PRIMARY KEY
    constraint (as created by pandas to_sql).
    """
    if df.empty:
        return

    temp_table = f"_temp_{table_name}"
    df.to_sql(temp_table, conn, if_exists="replace", index=False)

    if not _table_exists(conn, table_name):
        # Target does not exist yet — just rename the temp table
        conn.execute(f'ALTER TABLE "{temp_table}" RENAME TO "{table_name}"')
        conn.commit()
        return

    # Build a correlated DELETE that removes matching rows from the target
    pk_conditions = " AND ".join(
        f'"{table_name}"."{k}" = "{temp_table}"."{k}"' for k in primary_keys
    )
    conn.execute(f"""
        DELETE FROM "{table_name}"
        WHERE EXISTS (
            SELECT 1 FROM "{temp_table}"
            WHERE {pk_conditions}
        )
    """)

    # Re-insert with the fresh data
    col_list = ", ".join(f'"{c}"' for c in df.columns)
    conn.execute(f"""
        INSERT INTO "{table_name}" ({col_list})
        SELECT {col_list} FROM "{temp_table}"
    """)

    conn.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
    conn.commit()


def replace_child_for_events(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
    table_name: str,
    ev_ids: list[str],
) -> None:
    """
    Replace all child-table rows for the given ev_ids.

    Simpler than row-level diffing for tables like aircraft/engines where
    the full set of records per event is always present in an update file.
    """
    if df.empty or not ev_ids:
        return

    placeholders = ",".join("?" * len(ev_ids))

    if _table_exists(conn, table_name):
        conn.execute(
            f'DELETE FROM "{table_name}" WHERE ev_id IN ({placeholders})',
            ev_ids,
        )
        df.to_sql(table_name, conn, if_exists="append", index=False)
    else:
        df.to_sql(table_name, conn, if_exists="replace", index=False)

    conn.commit()


# ---------------------------------------------------------------------------
# Indices & views
# ---------------------------------------------------------------------------

def create_indices(conn: sqlite3.Connection) -> None:
    """Create performance indices on frequently queried columns."""
    index_defs = [
        ("idx_events_ev_id",    "events",        "ev_id"),
        ("idx_events_ev_date",  "events",        "ev_date"),
        ("idx_aircraft_ev_id",  "aircraft",      "ev_id"),
        ("idx_engines_ev_id",   "engines",       "ev_id"),
        ("idx_narratives_ev_id","narratives",    "ev_id"),
        ("idx_seq_ev_id",       "seq_of_events", "ev_id"),
        ("idx_findings_ev_id",  "findings",      "ev_id"),
        ("idx_injury_ev_id",    "injury",        "ev_id"),
    ]
    for idx_name, table, column in index_defs:
        if _table_exists(conn, table):
            conn.execute(
                f'CREATE INDEX IF NOT EXISTS {idx_name} ON "{table}"("{column}")'
            )
    conn.commit()


def create_views(conn: sqlite3.Connection) -> None:
    """Create standard SQL views that join primary and lookup tables."""
    conn.execute("DROP VIEW IF EXISTS v_full_report")
    conn.execute("""
        CREATE VIEW v_full_report AS
        SELECT
            e.ev_id,
            e.ev_date,
            e.ev_city || ', ' || e.ev_state  AS location,
            a.regis_no,
            a.acft_make,
            a.acft_model,
            e.inj_tot_t                       AS injury_total,
            n.narr_cause
        FROM events e
        LEFT JOIN aircraft   a ON e.ev_id = a.ev_id
        LEFT JOIN narratives n ON e.ev_id = n.ev_id
    """)

    conn.execute("DROP VIEW IF EXISTS v_labeled_report")
    conn.execute("""
        CREATE VIEW v_labeled_report AS
        SELECT
            e.ev_id,
            e.ev_date,
            e.ev_city || ', ' || e.ev_state  AS location,
            a.regis_no,
            a.acft_make,
            a.acft_model,
            e.inj_tot_t                       AS injury_total,
            n.narr_cause,
            GROUP_CONCAT(DISTINCT ut.tag)     AS tags,
            GROUP_CONCAT(DISTINCT ul.label_name || '=' || ul.label_value) AS labels
        FROM events e
        LEFT JOIN aircraft    a  ON e.ev_id = a.ev_id
        LEFT JOIN narratives  n  ON e.ev_id = n.ev_id
        LEFT JOIN user_tags   ut ON e.ev_id = ut.ev_id
        LEFT JOIN user_labels ul ON e.ev_id = ul.ev_id
        GROUP BY e.ev_id, e.ev_date, location,
                 a.regis_no, a.acft_make, a.acft_model,
                 e.inj_tot_t, n.narr_cause
    """)
    conn.commit()
