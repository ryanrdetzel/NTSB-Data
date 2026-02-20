"""
User-defined labels for NTSB event records.

Labels use a category:value model (e.g. weather:icing, phase_of_flight:takeoff).
An event can carry multiple values per category.  Categories and values are
validated against the taxonomy defined in config.LABEL_TAXONOMY.

Stored in the user_labels table — never overwritten by ETL updates.
"""

import sqlite3
from typing import Optional

from src import config


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate(category: str, value: str) -> tuple[str, str]:
    """Normalise and validate a category:value pair against the taxonomy.

    Returns the normalised (category, value) or raises ValueError.
    """
    cat = category.strip().lower()
    val = value.strip().lower()

    if cat not in config.LABEL_TAXONOMY:
        allowed = ", ".join(sorted(config.LABEL_TAXONOMY))
        raise ValueError(
            f"Unknown category '{cat}'. Valid categories:\n  {allowed}"
        )

    allowed_values = config.LABEL_TAXONOMY[cat]
    if val not in allowed_values:
        raise ValueError(
            f"Unknown value '{val}' for category '{cat}'. "
            f"Allowed values:\n  {', '.join(allowed_values)}"
        )

    return cat, val


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def add_label(conn: sqlite3.Connection, ev_id: str, category: str, value: str) -> bool:
    """Add a label to an event. Returns True if newly added."""
    cat, val = validate(category, value)
    try:
        conn.execute(
            "INSERT INTO user_labels (ev_id, category, value) VALUES (?, ?, ?)",
            (ev_id, cat, val),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def remove_label(
    conn: sqlite3.Connection,
    ev_id: str,
    category: str,
    value: Optional[str] = None,
) -> int:
    """Remove label(s) from an event.

    If value is given, removes that specific category:value pair.
    If value is None, removes ALL values for that category on the event.
    Returns the number of rows deleted.
    """
    cat = category.strip().lower()
    if value is not None:
        cur = conn.execute(
            "DELETE FROM user_labels WHERE ev_id = ? AND category = ? AND value = ?",
            (ev_id, cat, value.strip().lower()),
        )
    else:
        cur = conn.execute(
            "DELETE FROM user_labels WHERE ev_id = ? AND category = ?",
            (ev_id, cat),
        )
    conn.commit()
    return cur.rowcount


def get_labels(conn: sqlite3.Connection, ev_id: str) -> dict[str, list[str]]:
    """Return all labels for an event as {category: [values]}."""
    cur = conn.execute(
        "SELECT category, value FROM user_labels "
        "WHERE ev_id = ? ORDER BY category, value",
        (ev_id,),
    )
    result: dict[str, list[str]] = {}
    for cat, val in cur.fetchall():
        result.setdefault(cat, []).append(val)
    return result


def find_events(
    conn: sqlite3.Connection,
    category: str,
    value: Optional[str] = None,
) -> list[str]:
    """Find ev_ids matching a category (and optionally a specific value)."""
    cat = category.strip().lower()
    if value is not None:
        cur = conn.execute(
            "SELECT ev_id FROM user_labels "
            "WHERE category = ? AND value = ? ORDER BY ev_id",
            (cat, value.strip().lower()),
        )
    else:
        cur = conn.execute(
            "SELECT DISTINCT ev_id FROM user_labels "
            "WHERE category = ? ORDER BY ev_id",
            (cat,),
        )
    return [row[0] for row in cur.fetchall()]


def list_labels(conn: sqlite3.Connection) -> list[tuple[str, str, int]]:
    """Return all distinct category:value pairs with usage counts."""
    cur = conn.execute(
        "SELECT category, value, COUNT(*) AS cnt "
        "FROM user_labels GROUP BY category, value "
        "ORDER BY category, cnt DESC, value"
    )
    return cur.fetchall()


def label_coverage(conn: sqlite3.Connection) -> dict[str, int]:
    """Return count of labeled events per category."""
    cur = conn.execute(
        "SELECT category, COUNT(DISTINCT ev_id) AS cnt "
        "FROM user_labels GROUP BY category ORDER BY category"
    )
    return dict(cur.fetchall())


# ---------------------------------------------------------------------------
# Event display helpers
# ---------------------------------------------------------------------------

def show_event(conn: sqlite3.Connection, ev_id: str) -> Optional[dict]:
    """Return a summary dict for a single event including all labels."""
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT e.ev_id, e.ev_date, e.ev_city, e.ev_state, "
        "       a.acft_make, a.acft_model, a.regis_no, "
        "       e.inj_tot_t, n.narr_cause "
        "FROM events e "
        "LEFT JOIN aircraft   a ON e.ev_id = a.ev_id "
        "LEFT JOIN narratives n ON e.ev_id = n.ev_id "
        "WHERE e.ev_id = ?",
        (ev_id,),
    ).fetchone()
    conn.row_factory = None

    if row is None:
        return None

    result = dict(row)
    result["labels"] = get_labels(conn, ev_id)
    return result


def browse_events(
    conn: sqlite3.Connection,
    *,
    limit: int = 20,
    offset: int = 0,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    category: Optional[str] = None,
    value: Optional[str] = None,
    unlabeled: bool = False,
) -> list[dict]:
    """Browse events with optional filters.

    Filters:
      date_from / date_to   — ev_date range
      category / value      — only events with this label
      unlabeled             — only events with no labels at all
    """
    clauses: list[str] = []
    params: list = []

    if date_from:
        clauses.append("e.ev_date >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("e.ev_date <= ?")
        params.append(date_to)
    if category:
        if value:
            clauses.append(
                "EXISTS (SELECT 1 FROM user_labels ul WHERE ul.ev_id = e.ev_id "
                "AND ul.category = ? AND ul.value = ?)"
            )
            params.extend([category.strip().lower(), value.strip().lower()])
        else:
            clauses.append(
                "EXISTS (SELECT 1 FROM user_labels ul WHERE ul.ev_id = e.ev_id "
                "AND ul.category = ?)"
            )
            params.append(category.strip().lower())
    if unlabeled:
        clauses.append(
            "NOT EXISTS (SELECT 1 FROM user_labels ul WHERE ul.ev_id = e.ev_id)"
        )

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    sql = (
        "SELECT e.ev_id, e.ev_date, e.ev_city, e.ev_state, "
        "       a.acft_make, a.acft_model, e.inj_tot_t "
        "FROM events e "
        "LEFT JOIN aircraft a ON e.ev_id = a.ev_id "
        f"{where} "
        "ORDER BY e.ev_date DESC "
        "LIMIT ? OFFSET ?"
    )
    params.extend([limit, offset])

    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql, params).fetchall()
    conn.row_factory = None
    return [dict(r) for r in rows]


def count_events(
    conn: sqlite3.Connection,
    filters: list[tuple[str, Optional[str]]],
) -> int:
    """Count events matching ALL of the given (category, value?) filters.

    Example: count_events(conn, [("flight_rules", "imc"), ("failure_system", "engine")])
    answers "How many accidents in IMC with engine failure?"
    """
    clauses: list[str] = []
    params: list = []

    for i, (cat, val) in enumerate(filters):
        alias = f"ul{i}"
        if val is not None:
            clauses.append(
                f"EXISTS (SELECT 1 FROM user_labels {alias} "
                f"WHERE {alias}.ev_id = e.ev_id AND {alias}.category = ? AND {alias}.value = ?)"
            )
            params.extend([cat.strip().lower(), val.strip().lower()])
        else:
            clauses.append(
                f"EXISTS (SELECT 1 FROM user_labels {alias} "
                f"WHERE {alias}.ev_id = e.ev_id AND {alias}.category = ?)"
            )
            params.append(cat.strip().lower())

    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    cur = conn.execute(f"SELECT COUNT(DISTINCT e.ev_id) FROM events e{where}", params)
    return cur.fetchone()[0]
