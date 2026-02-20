#!/usr/bin/env python3
"""
NTSB Aviation Database ETL — CLI entrypoint.

Usage
-----
  python main.py --seed                    # Initial full seed from avall.zip
  python main.py --seed --force            # Force re-seed (overwrites existing DB)
  python main.py --update                  # Apply new incremental update files
  python main.py --seed --db custom.db     # Use a custom database path

  # Tags & labels
  python main.py tag add EVENT_ID tag1 tag2       # Add tags to an event
  python main.py tag rm  EVENT_ID tag1             # Remove a tag
  python main.py tag ls                            # List all tags and counts
  python main.py tag find TAG                      # Find events with a tag

  python main.py label set EVENT_ID key value      # Set a label on an event
  python main.py label rm  EVENT_ID key            # Remove a label
  python main.py label ls                          # List all label values and counts
  python main.py label find KEY [VALUE]            # Find events by label

  python main.py show EVENT_ID                     # Show full event detail + tags/labels
  python main.py browse                            # Browse events (with filters)
"""

import argparse
import sys

from src.config import DB_PATH
from src.orchestrator import seed, update
from src import db as database
from src import labels


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def _print_event_summary(evt: dict) -> None:
    """Pretty-print a single event summary."""
    print(f"  {evt['ev_id']}  {evt.get('ev_date', '?'):10}  "
          f"{evt.get('ev_city', '') or ''}, {evt.get('ev_state', '') or ''}  "
          f"{evt.get('acft_make', '') or ''} {evt.get('acft_model', '') or ''}  "
          f"injuries={evt.get('inj_tot_t', '?')}")


def _print_event_detail(evt: dict) -> None:
    """Pretty-print full event detail including tags and labels."""
    print(f"\n  Event:     {evt['ev_id']}")
    print(f"  Date:      {evt.get('ev_date', '?')}")
    print(f"  Location:  {evt.get('ev_city', '') or ''}, {evt.get('ev_state', '') or ''}")
    print(f"  Aircraft:  {evt.get('acft_make', '') or ''} {evt.get('acft_model', '') or ''}")
    print(f"  Reg No:    {evt.get('regis_no', '') or ''}")
    print(f"  Injuries:  {evt.get('inj_tot_t', '?')}")

    tag_list = evt.get("tags", [])
    if tag_list:
        print(f"  Tags:      {', '.join(tag_list)}")
    else:
        print("  Tags:      (none)")

    lbl = evt.get("labels", {})
    if lbl:
        print("  Labels:")
        for k, v in lbl.items():
            print(f"    {k}: {v}")
    else:
        print("  Labels:    (none)")

    cause = evt.get("narr_cause")
    if cause:
        print(f"\n  Probable cause:\n    {cause[:500]}")
    print()


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------

def _handle_tag(args, conn: database.sqlite3.Connection) -> None:
    action = args.tag_action

    if action == "add":
        for t in args.tags:
            added = labels.add_tag(conn, args.ev_id, t)
            status = "added" if added else "already exists"
            print(f"  {args.ev_id}  +{t.lower()}  ({status})")

    elif action == "rm":
        for t in args.tags:
            removed = labels.remove_tag(conn, args.ev_id, t)
            status = "removed" if removed else "not found"
            print(f"  {args.ev_id}  -{t.lower()}  ({status})")

    elif action == "ls":
        all_tags = labels.list_all_tags(conn)
        if not all_tags:
            print("  No tags defined yet.")
            return
        print(f"  {'Tag':<30} Count")
        print(f"  {'---':<30} -----")
        for tag, count in all_tags:
            print(f"  {tag:<30} {count}")

    elif action == "find":
        ev_ids = labels.find_by_tag(conn, args.find_tag)
        if not ev_ids:
            print(f"  No events found with tag '{args.find_tag}'")
            return
        print(f"  {len(ev_ids)} event(s) tagged '{args.find_tag}':")
        for eid in ev_ids:
            evt = labels.show_event(conn, eid)
            if evt:
                _print_event_summary(evt)


def _handle_label(args, conn: database.sqlite3.Connection) -> None:
    action = args.label_action

    if action == "set":
        labels.set_label(conn, args.ev_id, args.label_name, args.label_value)
        print(f"  {args.ev_id}  {args.label_name.lower()}={args.label_value}")

    elif action == "rm":
        removed = labels.remove_label(conn, args.ev_id, args.label_name)
        status = "removed" if removed else "not found"
        print(f"  {args.ev_id}  -{args.label_name.lower()}  ({status})")

    elif action == "ls":
        all_labels = labels.list_all_labels(conn)
        if not all_labels:
            print("  No labels defined yet.")
            return
        print(f"  {'Label':<20} {'Value':<30} Count")
        print(f"  {'-----':<20} {'-----':<30} -----")
        for name, value, count in all_labels:
            print(f"  {name:<20} {value:<30} {count}")

    elif action == "find":
        value = getattr(args, "label_value", None)
        results = labels.find_by_label(conn, args.label_name, value)
        if not results:
            msg = f"  No events found with label '{args.label_name}'"
            if value:
                msg += f"={value}"
            print(msg)
            return
        print(f"  {len(results)} event(s):")
        for ev_id, val in results:
            print(f"    {ev_id}  {args.label_name.lower()}={val}")


def _handle_show(args, conn: database.sqlite3.Connection) -> None:
    evt = labels.show_event(conn, args.ev_id)
    if evt is None:
        print(f"  Event '{args.ev_id}' not found.")
        return
    _print_event_detail(evt)


def _handle_browse(args, conn: database.sqlite3.Connection) -> None:
    events = labels.browse_events(
        conn,
        limit=args.limit,
        offset=args.offset,
        date_from=args.date_from,
        date_to=args.date_to,
        tag=args.tag,
        label_name=args.label_name,
        label_value=args.label_value,
        untagged=args.untagged,
    )
    if not events:
        print("  No events match the given filters.")
        return
    print(f"  Showing {len(events)} event(s):\n")
    for evt in events:
        _print_event_summary(evt)


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
        "--db",
        default=DB_PATH,
        metavar="PATH",
        help=f"Path to the SQLite database file (default: {DB_PATH})",
    )

    subparsers = parser.add_subparsers(dest="command")

    # --- seed / update (kept as subcommands AND flags for backward compat) ---
    parser.add_argument("--seed",   action="store_true", help="Perform the initial full seed")
    parser.add_argument("--update", action="store_true", help="Apply incremental updates")
    parser.add_argument("--force",  action="store_true", help="Overwrite existing DB (seed only)")

    # --- tag ---
    tag_parser = subparsers.add_parser("tag", help="Manage tags on events")
    tag_sub = tag_parser.add_subparsers(dest="tag_action", required=True)

    tag_add = tag_sub.add_parser("add", help="Add tags to an event")
    tag_add.add_argument("ev_id", help="Event ID")
    tag_add.add_argument("tags", nargs="+", help="Tags to add")

    tag_rm = tag_sub.add_parser("rm", help="Remove tags from an event")
    tag_rm.add_argument("ev_id", help="Event ID")
    tag_rm.add_argument("tags", nargs="+", help="Tags to remove")

    tag_sub.add_parser("ls", help="List all tags and their counts")

    tag_find = tag_sub.add_parser("find", help="Find events by tag")
    tag_find.add_argument("find_tag", help="Tag to search for")

    # --- label ---
    label_parser = subparsers.add_parser("label", help="Manage labels on events")
    label_sub = label_parser.add_subparsers(dest="label_action", required=True)

    label_set = label_sub.add_parser("set", help="Set a label on an event")
    label_set.add_argument("ev_id", help="Event ID")
    label_set.add_argument("label_name", help="Label name (key)")
    label_set.add_argument("label_value", help="Label value")

    label_rm = label_sub.add_parser("rm", help="Remove a label from an event")
    label_rm.add_argument("ev_id", help="Event ID")
    label_rm.add_argument("label_name", help="Label name to remove")

    label_sub.add_parser("ls", help="List all labels and their counts")

    label_find = label_sub.add_parser("find", help="Find events by label")
    label_find.add_argument("label_name", help="Label name to search")
    label_find.add_argument("label_value", nargs="?", default=None, help="Optional value filter")

    # --- show ---
    show_parser = subparsers.add_parser("show", help="Show full detail for an event")
    show_parser.add_argument("ev_id", help="Event ID")

    # --- browse ---
    browse_parser = subparsers.add_parser("browse", help="Browse events with filters")
    browse_parser.add_argument("--limit",       type=int, default=20, help="Max results (default: 20)")
    browse_parser.add_argument("--offset",      type=int, default=0,  help="Skip first N results")
    browse_parser.add_argument("--date-from",   dest="date_from", help="Start date (YYYY-MM-DD)")
    browse_parser.add_argument("--date-to",     dest="date_to",   help="End date (YYYY-MM-DD)")
    browse_parser.add_argument("--tag",         help="Filter by tag")
    browse_parser.add_argument("--label-name",  dest="label_name",  help="Filter by label name")
    browse_parser.add_argument("--label-value", dest="label_value", help="Filter by label value (requires --label-name)")
    browse_parser.add_argument("--untagged",    action="store_true", help="Show only events with no tags")

    args = parser.parse_args()

    # Handle legacy --seed / --update flags
    if args.seed:
        seed(db_path=args.db, force=args.force)
        return
    if args.update:
        if args.force:
            print("Note: --force has no effect with --update")
        update(db_path=args.db)
        return

    # Handle subcommands that need the DB
    if args.command in ("tag", "label", "show", "browse"):
        from pathlib import Path
        if not Path(args.db).exists():
            print(f"Database not found at {args.db}. Run --seed first.")
            sys.exit(1)

        conn = database.get_connection(args.db)
        database.init_user_tables(conn)

        if args.command == "tag":
            _handle_tag(args, conn)
        elif args.command == "label":
            _handle_label(args, conn)
        elif args.command == "show":
            _handle_show(args, conn)
        elif args.command == "browse":
            _handle_browse(args, conn)

        conn.close()
        return

    # No valid command given
    parser.print_help()


if __name__ == "__main__":
    sys.exit(main())
