#!/usr/bin/env python3
"""
Build reverse indexes for efficient release processing.
This script creates indexes mapping artists and release-groups to line numbers in the release file.
"""

import json
import sys
import logging
import pickle
from pathlib import Path
from typing import Dict, List, Any, Set
from collections import defaultdict
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ReleaseIndexer:
    def __init__(self, release_file_path: Path, output_dir: Path):
        self.release_file_path = release_file_path
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Indexes
        self.artist_index = defaultdict(list)      # artist_id -> [line_numbers]
        self.release_group_index = defaultdict(list)  # rg_id -> [line_numbers]
        self.line_positions = []  # [byte_position_for_line_0, byte_position_for_line_1, ...]

        # Statistics
        self.total_releases = 0
        self.artists_found = set()
        self.release_groups_found = set()

    def build_line_positions(self):
        """Pre-compute byte positions for each line in the release file."""
        logger.info("Building line position index...")

        start_time = time.time()
        with open(self.release_file_path, 'rb') as f:
            pos = 0
            for line in f:
                self.line_positions.append(pos)
                pos = f.tell()

        elapsed = time.time() - start_time
        logger.info(f"Line positions built: {len(self.line_positions):,} lines in {elapsed:.1f}s")

    def build_indexes(self, target_artists: Set[str] = None):
        """Build artist and release-group indexes from the release file."""
        logger.info("Building release indexes...")

        start_time = time.time()
        processed = 0

        with open(self.release_file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue

                processed += 1
                if processed % 100000 == 0:
                    elapsed = time.time() - start_time
                    rate = processed / elapsed
                    eta = (len(self.line_positions) - processed) / rate if rate > 0 else 0
                    logger.info(f"Processed {processed:,} releases... "
                              f"({rate:.0f} releases/sec, ETA: {eta/3600:.1f}h)")

                try:
                    release = json.loads(line)

                    # Index by release-group
                    release_group = release.get('release-group', {})
                    if isinstance(release_group, dict):
                        rg_id = release_group.get('id')
                        if rg_id:
                            self.release_group_index[rg_id].append(line_num)
                            self.release_groups_found.add(rg_id)

                    # Index by artist
                    artist_credits = release.get('artist-credit', [])
                    for credit in artist_credits:
                        artist = credit.get('artist', {})
                        if isinstance(artist, dict):
                            artist_id = artist.get('id')
                            if artist_id:
                                # If target_artists is specified, only index those
                                if target_artists is None or artist_id in target_artists:
                                    self.artist_index[artist_id].append(line_num)
                                    self.artists_found.add(artist_id)

                except json.JSONDecodeError as e:
                    logger.warning(f"JSON decode error at line {line_num}: {e}")
                    continue
                except Exception as e:
                    logger.warning(f"Error processing line {line_num}: {e}")
                    continue

        self.total_releases = processed
        elapsed = time.time() - start_time

        logger.info(f"Indexing complete!")
        logger.info(f"Total releases processed: {self.total_releases:,}")
        logger.info(f"Artists found: {len(self.artists_found):,}")
        logger.info(f"Release groups found: {len(self.release_groups_found):,}")
        logger.info(f"Processing time: {elapsed:.1f}s ({self.total_releases/elapsed:.0f} releases/sec)")

    def save_indexes(self):
        """Save indexes to disk."""
        logger.info("Saving indexes...")

        # Save artist index
        artist_index_file = self.output_dir / "artist_index.pkl"
        with open(artist_index_file, 'wb') as f:
            pickle.dump(dict(self.artist_index), f)
        logger.info(f"Artist index saved: {artist_index_file}")

        # Save release-group index
        rg_index_file = self.output_dir / "release_group_index.pkl"
        with open(rg_index_file, 'wb') as f:
            pickle.dump(dict(self.release_group_index), f)
        logger.info(f"Release-group index saved: {rg_index_file}")

        # Save line positions
        positions_file = self.output_dir / "line_positions.pkl"
        with open(positions_file, 'wb') as f:
            pickle.dump(self.line_positions, f)
        logger.info(f"Line positions saved: {positions_file}")

        # Save statistics
        stats = {
            "total_releases": self.total_releases,
            "artists_found": len(self.artists_found),
            "release_groups_found": len(self.release_groups_found),
            "index_sizes": {
                "artist_index": len(self.artist_index),
                "release_group_index": len(self.release_group_index)
            }
        }

        stats_file = self.output_dir / "index_stats.json"
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
        logger.info(f"Statistics saved: {stats_file}")

    def get_artist_releases(self, artist_id: str) -> List[int]:
        """Get line numbers for releases by a specific artist."""
        return self.artist_index.get(artist_id, [])

    def get_release_group_releases(self, rg_id: str) -> List[int]:
        """Get line numbers for releases in a specific release group."""
        return self.release_group_index.get(rg_id, [])

def main():
    """Main function."""
    import argparse

    parser = argparse.ArgumentParser(description="Build release indexes for efficient processing")
    parser.add_argument("--release-file", default="/media/oldnas3/data/musicbrainz/mbdump/release",
                       help="Path to the release dump file")
    parser.add_argument("--output-dir", default="/tmp/release_indexes",
                       help="Output directory for indexes")
    parser.add_argument("--target-artists", nargs="*",
                       help="Only index specific artist MBIDs")
    parser.add_argument("--skip-line-positions", action="store_true",
                       help="Skip building line positions (faster but less efficient)")

    args = parser.parse_args()

    release_file = Path(args.release_file)
    output_dir = Path(args.output_dir)

    if not release_file.exists():
        logger.error(f"Release file not found: {release_file}")
        sys.exit(1)

    # Convert target artists to set
    target_artists = set(args.target_artists) if args.target_artists else None

    logger.info("Starting release indexing...")
    logger.info(f"Release file: {release_file}")
    logger.info(f"Output directory: {output_dir}")
    if target_artists:
        logger.info(f"Target artists: {len(target_artists)}")

    # Create indexer
    indexer = ReleaseIndexer(release_file, output_dir)

    # Build line positions (unless skipped)
    if not args.skip_line_positions:
        indexer.build_line_positions()

    # Build indexes
    indexer.build_indexes(target_artists)

    # Save indexes
    indexer.save_indexes()

    logger.info("Indexing complete!")
    logger.info(f"Indexes saved to: {output_dir}")

if __name__ == "__main__":
    main()
