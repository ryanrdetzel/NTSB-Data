#!/usr/bin/env python3
"""
NTSB Aviation Database ETL — CLI entrypoint.

Usage
-----
  python main.py --seed                    # Initial full seed from avall.zip
  python main.py --seed --force            # Force re-seed (overwrites existing DB)
  python main.py --update                  # Apply new incremental update files
  python main.py --seed --db custom.db     # Use a custom database path
"""

import argparse
import sys

from src.config import DB_PATH
from src.orchestrator import seed, update


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="ntsb-etl",
        description="NTSB Aviation Database ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--seed",
        action="store_true",
        help="Perform the initial full seed from avall.zip",
    )
    mode.add_argument(
        "--update",
        action="store_true",
        help="Apply any new incremental upXXMMM.zip files",
    )

    parser.add_argument(
        "--db",
        default=DB_PATH,
        metavar="PATH",
        help=f"Path to the SQLite database file (default: {DB_PATH})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing database when seeding (seed only)",
    )

    args = parser.parse_args()

    if args.seed:
        seed(db_path=args.db, force=args.force)
    elif args.update:
        if args.force:
            print("Note: --force has no effect with --update")
        update(db_path=args.db)


if __name__ == "__main__":
    sys.exit(main())
