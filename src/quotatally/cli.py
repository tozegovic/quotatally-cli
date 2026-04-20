"""CLI entry point."""
from __future__ import annotations

import argparse

from . import __version__
from .aggregator import aggregate
from .db import DEFAULT_DB_PATH, Database
from .parser import iter_events
from .report import render


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="quotatally",
        description="Cross-client AI cost attribution from local session logs.",
    )
    ap.add_argument("--version", action="version", version=f"quotatally {__version__}")
    sub = ap.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("report", help="Print a cost report to stdout.")
    r.add_argument("--days", type=int, default=14)
    r.add_argument("--top", type=int, default=15)
    r.add_argument("--sources", nargs="+", default=["claude", "codex"],
                   help="Which log sources to include (when reading live).")
    r.add_argument("--from-db", action="store_true",
                   help="Read from the local SQLite DB instead of live logs.")
    r.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite DB path.")
    r.add_argument("--user-id", default="local")
    r.add_argument("--team-id", default="local")

    i = sub.add_parser("ingest", help="Parse logs and insert into the local SQLite DB.")
    i.add_argument("--days", type=int, default=30)
    i.add_argument("--sources", nargs="+", default=["claude", "codex"])
    i.add_argument("--db", default=DEFAULT_DB_PATH)
    i.add_argument("--user-id", default="local")
    i.add_argument("--team-id", default="local")

    s = sub.add_parser("stats", help="Show DB stats.")
    s.add_argument("--db", default=DEFAULT_DB_PATH)
    s.add_argument("--user-id", default="local")
    s.add_argument("--team-id", default="local")

    w = sub.add_parser("watch", help="Tail JSONL log dirs and ingest new events as they appear.")
    w.add_argument("--interval", type=float, default=5.0,
                   help="Seconds between polls (default 5).")
    w.add_argument("--once", action="store_true",
                   help="Run a single poll cycle and exit (for cron/test use).")
    w.add_argument("--sources", nargs="+", default=["claude", "codex"])
    w.add_argument("--db", default=DEFAULT_DB_PATH)
    w.add_argument("--user-id", default="local")
    w.add_argument("--team-id", default="local")

    args = ap.parse_args(argv)

    if args.cmd == "report":
        if args.from_db:
            db = Database(args.db)
            summary = db.summary(days=args.days, user_id=args.user_id, team_id=args.team_id)
        else:
            events = iter_events(days_back=args.days, sources=args.sources)
            summary = aggregate(events, top_k_messages=args.top)
        print(render(summary, days=args.days, top=args.top))
        return 0

    if args.cmd == "ingest":
        db = Database(args.db)
        events = iter_events(days_back=args.days, sources=args.sources)
        inserted, skipped = db.ingest(events, user_id=args.user_id, team_id=args.team_id)
        print(f"ingested {inserted:,} new events; skipped {skipped:,} duplicates "
              f"(db={args.db}, user={args.user_id}, team={args.team_id})")
        return 0

    if args.cmd == "stats":
        db = Database(args.db)
        n = db.count_events(user_id=args.user_id, team_id=args.team_id)
        print(f"{n:,} events stored for user={args.user_id} team={args.team_id} (db={args.db})")
        return 0

    if args.cmd == "watch":
        from .watcher import Watcher
        db = Database(args.db)
        watcher = Watcher(
            db,
            sources=tuple(args.sources),
            user_id=args.user_id,
            team_id=args.team_id,
        )
        if args.once:
            stats = watcher.run_once()
            print(f"watch: scanned {stats.files_scanned} files, "
                  f"+{stats.events_inserted} new, {stats.events_skipped} dup")
            return 0
        print(f"watch: polling every {args.interval}s (Ctrl-C to stop)")
        try:
            while True:
                stats = watcher.run_once()
                if stats.events_inserted or stats.events_skipped:
                    print(f"+{stats.events_inserted} new, {stats.events_skipped} dup "
                          f"({stats.files_scanned} files)")
                import time
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nwatch: stopped")
            return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
