"""
Wraps the mdbtools CLI to export MDB tables directly into Pandas DataFrames.
Requires mdbtools to be installed: apt install mdbtools / brew install mdbtools
"""

import subprocess
import shutil
import pandas as pd

from src.config import COLUMN_TYPES


def _check_mdbtools():
    """Verify mdbtools is available on PATH."""
    if shutil.which("mdb-export") is None:
        raise EnvironmentError(
            "mdbtools not found. Install with:\n"
            "  Ubuntu/Debian: sudo apt install mdbtools\n"
            "  macOS:         brew install mdbtools"
        )


def list_tables(mdb_path: str) -> list[str]:
    """Return all table names present in an MDB file."""
    _check_mdbtools()
    result = subprocess.run(
        ["mdb-tables", "-1", mdb_path],
        capture_output=True,
        text=True,
        check=True,
    )
    return [t.strip() for t in result.stdout.strip().splitlines() if t.strip()]


def export_table(mdb_path: str, table_name: str) -> pd.DataFrame:
    """
    Stream mdb-export stdout directly into a DataFrame.

    Column names are normalised to snake_case with leading/trailing
    whitespace removed.
    """
    _check_mdbtools()
    cmd = ["mdb-export", mdb_path, table_name]
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as proc:
        try:
            df = pd.read_csv(proc.stdout, low_memory=False)
        except Exception as exc:
            stderr_output = proc.stderr.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Failed to export table '{table_name}' from {mdb_path}: {exc}\n"
                f"mdb-export stderr: {stderr_output}"
            ) from exc

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    _coerce_types(df)
    return df


def _coerce_types(df: pd.DataFrame) -> None:
    """Apply explicit type overrides defined in COLUMN_TYPES (in-place).

    Converts:
      - "Int64"  → pandas nullable integer (SQLite INTEGER, NaN-safe)
      - "TEXT"   → string (guards against numeric-looking user-id columns)

    Also normalises ev_date from the MDB format "MM/DD/YY HH:MM:SS" to ISO
    "YYYY-MM-DD" so date ordering and range queries work correctly in SQLite.
    """
    # Date normalisation — ev_date arrives as "01/10/08 00:00:00"
    if "ev_date" in df.columns:
        df["ev_date"] = (
            pd.to_datetime(df["ev_date"], format="%m/%d/%y %H:%M:%S", errors="coerce")
            .dt.strftime("%Y-%m-%d")
        )

    for col, dtype in COLUMN_TYPES.items():
        if col not in df.columns:
            continue
        if dtype == "Int64":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif dtype == "TEXT":
            # Preserve NaN/None as None; everything else becomes a string
            df[col] = df[col].where(df[col].isna(), df[col].astype(str))
