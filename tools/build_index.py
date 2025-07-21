#!/usr/bin/env python3
"""Build line-offset indexes for MusicBrainz JSON dumps."""

import argparse
import json
import logging
import struct
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def build_index(dump_path: Path, index_path: Path):
    """Build line-offset index for NDJSON dump."""
    logger.info(f"Building index for {dump_path} -> {index_path}")

    with open(dump_path, 'rb') as dump_file, open(index_path, 'wb') as index_file:
        pos = dump_file.tell()
        count = 0

        for line in dump_file:
            try:
                # Parse the JSON to get the MBID
                data = json.loads(line.strip())
                mbid = data.get('id', '')

                if not mbid:
                    logger.warning(f"  Warning: Skipping line at position {pos} - no MBID found")
                    pos = dump_file.tell()
                    continue

                                # Write human-readable format: MBID|offset
                index_file.write(f"{mbid}|{pos}\n".encode())

                pos = dump_file.tell()
                count += 1

                if count % 100000 == 0:
                    logger.info(f"  Processed {count:,} entries...")

            except json.JSONDecodeError as e:
                logger.warning(f"  Warning: Skipping invalid line at position {pos}: {e}")
                pos = dump_file.tell()

    logger.info(f"Index built: {index_path} ({count:,} entries)")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Build an index for a MusicBrainz JSON dump")
    parser.add_argument("dump_file", help="Path to the dump file")
    parser.add_argument("index_file", help="Path to the output index file")
    args = parser.parse_args()

    dump_path = Path(args.dump_file)
    index_path = Path(args.index_file)

    if not dump_path.exists():
        logger.error(f"Error: Dump file {dump_path} not found")
        sys.exit(1)

    # Create output directory if needed
    index_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        build_index(dump_path, index_path)
    except Exception as e:
        logger.error(f"Error building index: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
