#!/usr/bin/env python3
"""
NTSB Aviation Database — Daily Update Runner

Designed for scheduling via cron or a task scheduler:

  0 8 * * 3  cd /path/to/ntsb-data && python daily_update.py

What it does:
  1. Applies any new incremental update files from the NTSB server.
  2. Prints a structured statistics report for the current database.
  3. Cleans up downloaded temp files.

Exit codes
----------
  0  — success (whether or not new data was available)
  1  — unexpected error (see stderr)

Usage
-----
  python daily_update.py                      # default database path
  python daily_update.py --db /custom/path.db # custom database path
  python daily_update.py --stats-only         # print stats without updating
  python daily_update.py --no-cleanup         # skip temp file removal
"""

import argparse
import sys
import textwrap
from datetime import datetime

from src.config import DB_PATH, TEMP_DIR
from src import db as database
from src.orchestrator import update


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------

def _section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print("=" * 60)


def _row(label: str, value) -> None:
    if value is None:
        value = "N/A"
    print(f"  {label:<35} {value}")


def print_stats_report(db_path: str) -> None:
    """Open the database, gather stats, and print a formatted report."""
    from pathlib import Path

    db_file = Path(db_path)
    if not db_file.exists():
        print(f"  [WARN] Database not found at {db_path} — stats unavailable.")
        return

    conn = database.get_connection(db_path)
    stats = database.get_summary_stats(conn)
    conn.close()

    db_size_mb = db_file.stat().st_size / 1_048_576

    _section("DATABASE STATISTICS")
    _row("Database path", db_path)
    _row("Database size (MB)", f"{db_size_mb:.1f}")
    _row("Total events", f"{stats.get('total_events', 'N/A'):,}" if stats.get('total_events') else "N/A")
    _row("Total aircraft records", f"{stats.get('total_aircraft', 'N/A'):,}" if stats.get('total_aircraft') else "N/A")
    _row("Most recent event date", stats.get("most_recent_event_date"))

    _section("RECENT ACTIVITY")
    e30 = stats.get("events_last_30_days")
    e365 = stats.get("events_last_365_days")
    f365 = stats.get("fatal_events_last_365_days")
    _row("Events (last 30 days)", f"{e30:,}" if e30 is not None else "N/A")
    _row("Events (last 365 days)", f"{e365:,}" if e365 is not None else "N/A")
    _row("Fatal events (last 365 days)", f"{f365:,}" if f365 is not None else "N/A")

    _section("SYNC STATUS")
    _row("Update files applied", stats.get("sync_files_applied"))
    _row("Last file applied", stats.get("last_sync_file"))
    _row("Last sync timestamp", stats.get("last_sync_at"))

    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        prog="daily-update",
        description="NTSB Aviation Database — Daily Update Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Designed for daily scheduling (e.g. via cron). Runs the incremental
            update, prints a statistics report, and cleans up temp files.
        """),
    )
    parser.add_argument(
        "--db",
        default=DB_PATH,
        metavar="PATH",
        help=f"Path to the SQLite database file (default: {DB_PATH})",
    )
    parser.add_argument(
        "--stats-only",
        action="store_true",
        help="Print the statistics report without running an update",
    )
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Skip cleanup of downloaded temp files after the update",
    )
    args = parser.parse_args()

    run_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\nNTSB Daily Update  [{run_at}]")
    print("-" * 60)

    # 1. Update (unless --stats-only)
    if not args.stats_only:
        try:
            update(db_path=args.db)
        except SystemExit as exc:
            # orchestrator calls sys.exit(1) on expected failures
            print(f"\n[ERROR] Update aborted: {exc}", file=sys.stderr)
            return int(str(exc)) if str(exc).isdigit() else 1
        except Exception as exc:
            print(f"\n[ERROR] Unexpected error during update: {exc}", file=sys.stderr)
            return 1

    # 2. Stats report
    print_stats_report(args.db)

    # 3. Cleanup temp files
    if not args.no_cleanup:
        deleted = database.cleanup_temp(TEMP_DIR)
        if deleted:
            print(f"  Cleaned up {deleted} temp file(s) from '{TEMP_DIR}/'.")

    print(f"  Done.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
