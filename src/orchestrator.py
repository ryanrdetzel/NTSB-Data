"""
High-level ETL orchestration.

  seed()   — one-time full load from avall.zip
  update() — idempotent weekly refresh from upXXMMM.zip files
"""

import sys
from pathlib import Path

from src import config
from src import db as database
from src import downloader
from src import mdb_adapter


# ---------------------------------------------------------------------------
# Seed
# ---------------------------------------------------------------------------

def seed(db_path: str = config.DB_PATH, force: bool = False) -> None:
    """
    Perform the initial full seed from avall.zip.

    Downloads avall.zip, exports every TARGET_TABLE and LOOKUP_TABLE from
    the embedded MDB, and loads them into a fresh SQLite database.
    """
    db_file = Path(db_path)

    if db_file.exists():
        if not force:
            print(f"Database already exists at {db_path}.")
            print("Use --force to overwrite. Aborting.")
            sys.exit(1)
        print(f"  Removing existing database: {db_path}")
        db_file.unlink()

    print("=== NTSB Aviation ETL: SEED ===")

    # 1. Download the full archive
    zip_path = downloader.download_file("avall.zip")

    # 2. Extract the MDB
    print("  Extracting MDB ...")
    mdb_path = downloader.extract_mdb(zip_path)

    # 3. Inventory tables available in the MDB (case-insensitive map)
    mdb_tables = mdb_adapter.list_tables(str(mdb_path))
    available_ci = {t.lower(): t for t in mdb_tables}  # lowercase -> actual name
    print(f"  MDB contains {len(mdb_tables)} table(s)")

    conn = database.get_connection(db_path)
    database.init_meta_table(conn)

    total_rows = 0

    # 4. Load primary tables
    print("\n  Loading primary tables ...")
    for table in config.TARGET_TABLES:
        actual = available_ci.get(table.lower())
        if actual is None:
            print(f"    [SKIP] '{table}' not found in MDB")
            continue
        df = mdb_adapter.export_table(str(mdb_path), actual)
        database.replace_dataframe(conn, df, table)
        print(f"    {table}: {len(df):>10,} rows")
        total_rows += len(df)

    # 5. Load lookup tables
    print("\n  Loading lookup tables ...")
    for table in config.LOOKUP_TABLES:
        actual = available_ci.get(table.lower())
        if actual is None:
            print(f"    [SKIP] '{table}' not found in MDB")
            continue
        df = mdb_adapter.export_table(str(mdb_path), actual)
        database.replace_dataframe(conn, df, table)
        print(f"    {table}: {len(df):>10,} rows")
        total_rows += len(df)

    # 6. Post-load optimisations
    print("\n  Creating indices ...")
    database.create_indices(conn)

    print("  Creating views ...")
    database.create_views(conn)

    print("  Initialising user label tables ...")
    database.init_user_tables(conn)

    # 7. Record that avall.zip has been applied
    database.log_processed_file(conn, "avall.zip", total_rows)
    conn.close()

    size_mb = db_file.stat().st_size / 1_048_576
    print(f"\n  Seed complete. {total_rows:,} rows total.")
    print(f"  Database: {db_path}  ({size_mb:.1f} MB)")


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------

def update(db_path: str = config.DB_PATH) -> None:
    """
    Apply any new upXXMMM.zip incremental update files not yet in the DB.

    For the events table, individual rows are upserted by ev_id.
    For all child tables (aircraft, engines, …), the full set of rows for
    the affected ev_ids is replaced — simpler and safer than row-level diff.
    """
    db_file = Path(db_path)
    if not db_file.exists():
        print(f"Database not found at {db_path}. Run --seed first.")
        sys.exit(1)

    print("=== NTSB Aviation ETL: UPDATE ===")

    conn = database.get_connection(db_path)
    database.init_meta_table(conn)

    # 1. Discover what the server has
    print("  Fetching file list from NTSB ...")
    available_files = downloader.fetch_available_files()
    update_files = downloader.get_update_files(available_files)
    print(f"  Server has {len(update_files)} update archive(s)")

    # 2. Calculate delta
    processed = database.get_processed_files(conn)
    new_files = sorted(f for f in update_files if f not in processed)

    if not new_files:
        print("  Database is up to date. Nothing to apply.")
        conn.close()
        return

    print(f"  {len(new_files)} new file(s): {new_files}\n")

    # 3. Apply each new file
    for filename in new_files:
        print(f"  --- {filename} ---")
        try:
            _apply_update_file(conn, filename)
        except Exception as exc:
            print(f"  ERROR applying {filename}: {exc}")
            conn.close()
            raise

    conn.close()
    print("\n  Update complete.")


def _apply_update_file(conn, filename: str) -> None:
    """Download, extract, and apply a single update zip."""
    zip_path = downloader.download_file(filename)
    mdb_path = downloader.extract_mdb(zip_path)

    mdb_tables = mdb_adapter.list_tables(str(mdb_path))
    available_ci = {t.lower(): t for t in mdb_tables}
    total_rows = 0

    # --- events: row-level UPSERT ---
    if "events" in available_ci:
        df = mdb_adapter.export_table(str(mdb_path), available_ci["events"])
        if not df.empty:
            ev_ids = df["ev_id"].dropna().unique().tolist()
            database.upsert_dataframe(conn, df, "events", config.TABLE_PRIMARY_KEYS["events"])
            print(f"    events: {len(df):,} rows upserted")
            total_rows += len(df)
        else:
            ev_ids = []
    else:
        ev_ids = []

    # --- child tables: replace-for-ev_ids strategy ---
    child_tables = [t for t in config.TARGET_TABLES if t != "events"]
    for table in child_tables:
        actual = available_ci.get(table.lower())
        if actual is None:
            continue
        df = mdb_adapter.export_table(str(mdb_path), actual)
        if df.empty:
            continue

        if ev_ids:
            database.replace_child_for_events(conn, df, table, ev_ids)
        else:
            # No events context — fall back to primary-key upsert
            pk = config.TABLE_PRIMARY_KEYS.get(table, ["ev_id"])
            database.upsert_dataframe(conn, df, table, pk)

        print(f"    {table}: {len(df):,} rows replaced")
        total_rows += len(df)

    # --- lookup tables: key-level upsert ---
    for table in config.LOOKUP_TABLES:
        actual = available_ci.get(table.lower())
        if actual is None:
            continue
        df = mdb_adapter.export_table(str(mdb_path), actual)
        if df.empty:
            continue
        pk = config.LOOKUP_PRIMARY_KEYS.get(table, [])
        if pk:
            database.upsert_dataframe(conn, df, table, pk)
        else:
            database.replace_dataframe(conn, df, table)
        total_rows += len(df)

    database.log_processed_file(conn, filename, total_rows)
    print(f"    Logged {filename} ({total_rows:,} rows total)")
