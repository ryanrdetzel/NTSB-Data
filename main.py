#!/usr/bin/env python3
"""
NTSB Aviation Database ETL — CLI entrypoint.

Usage
-----
  python main.py --seed                    # Initial full seed from avall.zip
  python main.py --seed --force            # Force re-seed (overwrites existing DB)
  python main.py --update                  # Apply new incremental update files

  # Labels  (category:value pairs on events)
  python main.py label add EV_ID category value [value2 ...]
  python main.py label rm  EV_ID category [value]
  python main.py label ls
  python main.py label find category [value]

  python main.py show EV_ID                # Full event detail + labels
  python main.py browse                    # Browse events with filters
  python main.py categories                # Show the full label taxonomy
  python main.py count cat1:val1 cat2:val2 # Count events matching filters
"""

import argparse
import sys

from src.config import DB_PATH, LABEL_TAXONOMY
from src.orchestrator import seed, update
from src import db as database
from src import labels


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def _print_event_summary(evt: dict) -> None:
    print(f"  {evt['ev_id']}  {evt.get('ev_date', '?'):10}  "
          f"{evt.get('ev_city', '') or ''}, {evt.get('ev_state', '') or ''}  "
          f"{evt.get('acft_make', '') or ''} {evt.get('acft_model', '') or ''}  "
          f"injuries={evt.get('inj_tot_t', '?')}")


def _print_event_detail(evt: dict) -> None:
    print(f"\n  Event:     {evt['ev_id']}")
    print(f"  Date:      {evt.get('ev_date', '?')}")
    print(f"  Location:  {evt.get('ev_city', '') or ''}, {evt.get('ev_state', '') or ''}")
    print(f"  Aircraft:  {evt.get('acft_make', '') or ''} {evt.get('acft_model', '') or ''}")
    print(f"  Reg No:    {evt.get('regis_no', '') or ''}")
    print(f"  Injuries:  {evt.get('inj_tot_t', '?')}")

    lbl = evt.get("labels", {})
    if lbl:
        print("  Labels:")
        for cat, vals in lbl.items():
            print(f"    {cat}: {', '.join(vals)}")
    else:
        print("  Labels:    (none)")

    cause = evt.get("narr_cause")
    if cause:
        print(f"\n  Probable cause:\n    {cause[:500]}")
    print()


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------

def _handle_label(args, conn) -> None:
    action = args.label_action

    if action == "add":
        for val in args.values:
            try:
                added = labels.add_label(conn, args.ev_id, args.category, val)
                status = "added" if added else "already exists"
                print(f"  {args.ev_id}  {args.category}:{val.lower()}  ({status})")
            except ValueError as e:
                print(f"  ERROR: {e}")

    elif action == "rm":
        value = getattr(args, "value", None)
        count = labels.remove_label(conn, args.ev_id, args.category, value)
        if value:
            print(f"  {args.ev_id}  -{args.category}:{value}  ({'removed' if count else 'not found'})")
        else:
            print(f"  {args.ev_id}  -{args.category}:*  ({count} removed)")

    elif action == "ls":
        all_labels = labels.list_labels(conn)
        if not all_labels:
            print("  No labels applied yet.")
            return
        print(f"  {'Category':<22} {'Value':<28} Count")
        print(f"  {'--------':<22} {'-----':<28} -----")
        for cat, val, count in all_labels:
            print(f"  {cat:<22} {val:<28} {count}")

    elif action == "find":
        value = getattr(args, "value", None)
        ev_ids = labels.find_events(conn, args.category, value)
        label_str = args.category + (f":{value}" if value else "")
        if not ev_ids:
            print(f"  No events found with {label_str}")
            return
        print(f"  {len(ev_ids)} event(s) with {label_str}:\n")
        for eid in ev_ids[:50]:
            evt = labels.show_event(conn, eid)
            if evt:
                _print_event_summary(evt)
        if len(ev_ids) > 50:
            print(f"\n  ... and {len(ev_ids) - 50} more")


def _handle_show(args, conn) -> None:
    evt = labels.show_event(conn, args.ev_id)
    if evt is None:
        print(f"  Event '{args.ev_id}' not found.")
        return
    _print_event_detail(evt)


def _handle_browse(args, conn) -> None:
    events = labels.browse_events(
        conn,
        limit=args.limit,
        offset=args.offset,
        date_from=args.date_from,
        date_to=args.date_to,
        category=args.category,
        value=args.value,
        unlabeled=args.unlabeled,
    )
    if not events:
        print("  No events match the given filters.")
        return
    print(f"  Showing {len(events)} event(s):\n")
    for evt in events:
        _print_event_summary(evt)


def _handle_categories(args, conn) -> None:
    coverage = labels.label_coverage(conn) if conn else {}

    for cat, allowed_values in sorted(LABEL_TAXONOMY.items()):
        used = coverage.get(cat, 0)
        print(f"  {cat}  ({used} events labeled)")
        print(f"    {', '.join(allowed_values)}")
        print()


def _handle_count(args, conn) -> None:
    filters = []
    for spec in args.filters:
        if ":" in spec:
            cat, val = spec.split(":", 1)
            filters.append((cat, val))
        else:
            filters.append((spec, None))

    count = labels.count_events(conn, filters)
    desc = " AND ".join(
        f"{cat}:{val}" if val else cat for cat, val in filters
    )
    print(f"  {count} event(s) matching: {desc}")


# ---------------------------------------------------------------------------
# CLI definition
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="ntsb-etl",
        description="NTSB Aviation Database ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--db", default=DB_PATH, metavar="PATH",
        help=f"Path to the SQLite database file (default: {DB_PATH})",
    )

    subparsers = parser.add_subparsers(dest="command")

    # --- seed / update flags (backward compat) ---
    parser.add_argument("--seed",   action="store_true", help="Perform the initial full seed")
    parser.add_argument("--update", action="store_true", help="Apply incremental updates")
    parser.add_argument("--force",  action="store_true", help="Overwrite existing DB (seed only)")

    # --- label ---
    label_parser = subparsers.add_parser("label", help="Manage labels on events")
    label_sub = label_parser.add_subparsers(dest="label_action", required=True)

    label_add = label_sub.add_parser("add", help="Add labels to an event")
    label_add.add_argument("ev_id", help="Event ID")
    label_add.add_argument("category", help="Label category (e.g. weather, phase_of_flight)")
    label_add.add_argument("values", nargs="+", help="One or more values to add")

    label_rm = label_sub.add_parser("rm", help="Remove label(s) from an event")
    label_rm.add_argument("ev_id", help="Event ID")
    label_rm.add_argument("category", help="Label category")
    label_rm.add_argument("value", nargs="?", default=None, help="Specific value (omit to remove all in category)")

    label_sub.add_parser("ls", help="List all labels and their counts")

    label_find = label_sub.add_parser("find", help="Find events by label")
    label_find.add_argument("category", help="Label category to search")
    label_find.add_argument("value", nargs="?", default=None, help="Optional value filter")

    # --- show ---
    show_parser = subparsers.add_parser("show", help="Show full detail for an event")
    show_parser.add_argument("ev_id", help="Event ID")

    # --- browse ---
    browse_parser = subparsers.add_parser("browse", help="Browse events with filters")
    browse_parser.add_argument("--limit",     type=int, default=20, help="Max results (default: 20)")
    browse_parser.add_argument("--offset",    type=int, default=0,  help="Skip first N results")
    browse_parser.add_argument("--date-from", dest="date_from", help="Start date (YYYY-MM-DD)")
    browse_parser.add_argument("--date-to",   dest="date_to",   help="End date (YYYY-MM-DD)")
    browse_parser.add_argument("--category",  help="Filter by label category")
    browse_parser.add_argument("--value",     help="Filter by label value (requires --category)")
    browse_parser.add_argument("--unlabeled", action="store_true", help="Show only unlabeled events")

    # --- categories ---
    subparsers.add_parser("categories", help="Show the full label taxonomy")

    # --- count ---
    count_parser = subparsers.add_parser("count", help="Count events matching label filters")
    count_parser.add_argument(
        "filters", nargs="+", metavar="cat:val",
        help="Label filters as category:value pairs (e.g. flight_rules:imc failure_system:engine)",
    )

    args = parser.parse_args()

    # Handle --seed / --update flags
    if args.seed:
        seed(db_path=args.db, force=args.force)
        return
    if args.update:
        update(db_path=args.db)
        return

    # Handle subcommands
    if args.command in ("label", "show", "browse", "categories", "count"):
        from pathlib import Path
        if not Path(args.db).exists():
            print(f"Database not found at {args.db}. Run --seed first.")
            sys.exit(1)

        conn = database.get_connection(args.db)
        database.init_user_tables(conn)

        handlers = {
            "label": _handle_label,
            "show": _handle_show,
            "browse": _handle_browse,
            "categories": _handle_categories,
            "count": _handle_count,
        }
        handlers[args.command](args, conn)
        conn.close()
        return

    parser.print_help()


if __name__ == "__main__":
    sys.exit(main())
