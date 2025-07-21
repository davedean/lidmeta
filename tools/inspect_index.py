#!/usr/bin/env python3
"""Inspect human-readable index files."""

import argparse
import sys
from pathlib import Path
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def inspect_index(index_path: Path, limit: int = 10):
    """Inspect a human-readable index file and show entries."""
    logger.info(f"Inspecting index file: {index_path}")
    logger.info(f"File size: {index_path.stat().st_size:,} bytes")

    entry_count = 0
    with open(index_path, 'r') as f:
        for line in f:
            entry_count += 1

    logger.info(f"Total entries: {entry_count:,}")
    logger.info("")

    with open(index_path, 'r') as f:
        for i, line in enumerate(f):
            if i >= limit:
                break
            line = line.strip()
            if not line:
                continue
            parts = line.split('|')
            if len(parts) == 2:
                mbid, offset = parts
                logger.info(f"{i+1:3d}. MBID: {mbid} | Offset: {int(offset):,}")
            else:
                logger.info(f"{i+1:3d}. Invalid line: {line}")

    if entry_count > limit:
        logger.info(f"... and {entry_count - limit:,} more entries")

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Inspect a binary index file")
    parser.add_argument("index_file", help="Path to the index file")
    parser.add_argument("--limit", type=int, default=10, help="Number of entries to show (default: 10)")
    args = parser.parse_args()

    index_path = Path(args.index_file)
    if not index_path.exists():
        logger.error(f"Error: Index file {index_path} not found")
        sys.exit(1)

    try:
        inspect_index(index_path, args.limit)
    except Exception as e:
        logger.error(f"Error inspecting index: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
