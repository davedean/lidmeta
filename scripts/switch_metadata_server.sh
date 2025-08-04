#!/usr/bin/env python3
"""
Safely view, set or reset Lidarr’s metadata source.

If --url is omitted AND --reset is omitted → show current value.
If --reset is supplied            → use DEFAULT_URL.
If --url is supplied              → use that URL.

The script is idempotent: it updates the existing row or inserts a new one.
"""

import argparse, shutil, sqlite3, sys
from pathlib import Path
from urllib.parse import urlparse

DEFAULT_URL = "https://api.lidarr.audio/api/v0.4/"  # upstream server :contentReference[oaicite:0]{index=0}
CONFIG_KEY  = "metadatasource"

def valid_url(text: str) -> str:
    parsed = urlparse(text)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise argparse.ArgumentTypeError(f"Invalid URL: {text}")
    return text.rstrip("/") + "/"

def parse_args():
    p = argparse.ArgumentParser(description="Change Lidarr metadata source")
    p.add_argument("--db", required=True, type=Path, help="Path to Lidarr.sqlite")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--url", type=valid_url, help="New metadata server URL")
    g.add_argument("--reset", action="store_true", help="Revert to default server")
    return p.parse_args()

def backup(db_path: Path):
    bak = db_path.with_suffix(".bak")
    shutil.copy2(db_path, bak)
    print(f"Backup saved → {bak}")

def upsert(conn: sqlite3.Connection, url: str):
    conn.execute(
        """
        INSERT INTO Config (Key, Value)
        VALUES (?, ?)
        ON CONFLICT(Key) DO UPDATE SET Value = excluded.Value
        """,
        (CONFIG_KEY, url),
    )
    conn.commit()

def main():
    args = parse_args()

    if not args.db.exists():
        sys.exit(f"DB not found: {args.db}")

    # Resolve desired action
    if args.reset:
        new_url = DEFAULT_URL
    elif args.url:
        new_url = args.url
    else:
        # just show current setting
        with sqlite3.connect(args.db) as conn:
            cur = conn.execute("SELECT Value FROM Config WHERE Key = ?", (CONFIG_KEY,))
            row = cur.fetchone()
            print(f"Current metadata source: {row[0] if row else '<unset>'}")
        return

    with sqlite3.connect(args.db) as conn:
        cur = conn.execute("SELECT Value FROM Config WHERE Key = ?", (CONFIG_KEY,))
        row = cur.fetchone()
        if row and row[0] == new_url:
            print("No change needed; already set.")
            return

        backup(args.db)
        upsert(conn, new_url)
        print(f"Metadata source updated → {new_url}")

if __name__ == "__main__":
    main()

