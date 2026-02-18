"""
Wraps the mdbtools CLI to export MDB tables directly into Pandas DataFrames.
Requires mdbtools to be installed: apt install mdbtools / brew install mdbtools
"""

import subprocess
import shutil
import pandas as pd


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
    return df
