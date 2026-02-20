"""
User-defined tags and labels for NTSB event records.

Tags are simple strings attached to an event (e.g. "reviewed", "weather-related").
Labels are key-value pairs attached to an event (e.g. severity=high, root_cause=engine).

Both are stored in dedicated tables and never overwritten by ETL updates.
"""

import sqlite3
from typing import Optional

from src import db as database


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def add_tag(conn: sqlite3.Connection, ev_id: str, tag: str) -> bool:
    """Attach a tag to an event. Returns True if the tag was newly added."""
    try:
        conn.execute(
            "INSERT INTO user_tags (ev_id, tag) VALUES (?, ?)",
            (ev_id, tag.strip().lower()),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def remove_tag(conn: sqlite3.Connection, ev_id: str, tag: str) -> bool:
    """Remove a tag from an event. Returns True if a row was deleted."""
    cur = conn.execute(
        "DELETE FROM user_tags WHERE ev_id = ? AND tag = ?",
        (ev_id, tag.strip().lower()),
    )
    conn.commit()
    return cur.rowcount > 0


def get_tags(conn: sqlite3.Connection, ev_id: str) -> list[str]:
    """Return all tags for a given event."""
    cur = conn.execute(
        "SELECT tag FROM user_tags WHERE ev_id = ? ORDER BY tag", (ev_id,)
    )
    return [row[0] for row in cur.fetchall()]


def find_by_tag(conn: sqlite3.Connection, tag: str) -> list[str]:
    """Return all ev_ids that carry a given tag."""
    cur = conn.execute(
        "SELECT ev_id FROM user_tags WHERE tag = ? ORDER BY ev_id",
        (tag.strip().lower(),),
    )
    return [row[0] for row in cur.fetchall()]


def list_all_tags(conn: sqlite3.Connection) -> list[tuple[str, int]]:
    """Return all distinct tags and their usage counts."""
    cur = conn.execute(
        "SELECT tag, COUNT(*) AS cnt FROM user_tags GROUP BY tag ORDER BY cnt DESC, tag"
    )
    return cur.fetchall()


# ---------------------------------------------------------------------------
# Labels (key-value pairs)
# ---------------------------------------------------------------------------

def set_label(conn: sqlite3.Connection, ev_id: str, name: str, value: str) -> None:
    """Set a label on an event. Overwrites any previous value for that name."""
    conn.execute(
        "INSERT OR REPLACE INTO user_labels (ev_id, label_name, label_value) VALUES (?, ?, ?)",
        (ev_id, name.strip().lower(), value.strip()),
    )
    conn.commit()


def remove_label(conn: sqlite3.Connection, ev_id: str, name: str) -> bool:
    """Remove a label from an event. Returns True if a row was deleted."""
    cur = conn.execute(
        "DELETE FROM user_labels WHERE ev_id = ? AND label_name = ?",
        (ev_id, name.strip().lower()),
    )
    conn.commit()
    return cur.rowcount > 0


def get_labels(conn: sqlite3.Connection, ev_id: str) -> dict[str, str]:
    """Return all labels for a given event as {name: value}."""
    cur = conn.execute(
        "SELECT label_name, label_value FROM user_labels WHERE ev_id = ? ORDER BY label_name",
        (ev_id,),
    )
    return dict(cur.fetchall())


def find_by_label(
    conn: sqlite3.Connection, name: str, value: Optional[str] = None
) -> list[tuple[str, str]]:
    """
    Find events by label name (and optionally value).

    Returns list of (ev_id, label_value) tuples.
    """
    name = name.strip().lower()
    if value is not None:
        cur = conn.execute(
            "SELECT ev_id, label_value FROM user_labels "
            "WHERE label_name = ? AND label_value = ? ORDER BY ev_id",
            (name, value.strip()),
        )
    else:
        cur = conn.execute(
            "SELECT ev_id, label_value FROM user_labels "
            "WHERE label_name = ? ORDER BY ev_id",
            (name,),
        )
    return cur.fetchall()


def list_all_labels(conn: sqlite3.Connection) -> list[tuple[str, str, int]]:
    """Return all distinct label name/value combinations and their counts."""
    cur = conn.execute(
        "SELECT label_name, label_value, COUNT(*) AS cnt "
        "FROM user_labels GROUP BY label_name, label_value "
        "ORDER BY label_name, cnt DESC"
    )
    return cur.fetchall()


# ---------------------------------------------------------------------------
# Event summary (for reviewing records)
# ---------------------------------------------------------------------------

def show_event(conn: sqlite3.Connection, ev_id: str) -> Optional[dict]:
    """Return a summary dict for a single event including tags and labels."""
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
    result["tags"] = get_tags(conn, ev_id)
    result["labels"] = get_labels(conn, ev_id)
    return result


def browse_events(
    conn: sqlite3.Connection,
    *,
    limit: int = 20,
    offset: int = 0,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    tag: Optional[str] = None,
    label_name: Optional[str] = None,
    label_value: Optional[str] = None,
    untagged: bool = False,
) -> list[dict]:
    """
    Browse events with optional filters. Returns a list of summary dicts.

    Filters:
      date_from / date_to  — restrict by ev_date range
      tag                  — only events carrying this tag
      label_name/value     — only events with this label
      untagged             — only events with no tags at all
    """
    clauses = []
    params: list = []

    if date_from:
        clauses.append("e.ev_date >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("e.ev_date <= ?")
        params.append(date_to)
    if tag:
        clauses.append(
            "EXISTS (SELECT 1 FROM user_tags ut WHERE ut.ev_id = e.ev_id AND ut.tag = ?)"
        )
        params.append(tag.strip().lower())
    if label_name:
        if label_value:
            clauses.append(
                "EXISTS (SELECT 1 FROM user_labels ul WHERE ul.ev_id = e.ev_id "
                "AND ul.label_name = ? AND ul.label_value = ?)"
            )
            params.extend([label_name.strip().lower(), label_value.strip()])
        else:
            clauses.append(
                "EXISTS (SELECT 1 FROM user_labels ul WHERE ul.ev_id = e.ev_id "
                "AND ul.label_name = ?)"
            )
            params.append(label_name.strip().lower())
    if untagged:
        clauses.append(
            "NOT EXISTS (SELECT 1 FROM user_tags ut WHERE ut.ev_id = e.ev_id)"
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
