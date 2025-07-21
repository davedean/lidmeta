#!/usr/bin/env python3
"""
Streaming normalization from compressed release file.
This approach processes the release.tar.xz file directly without extraction.
"""

import json
import sys
import logging
import tarfile
import tempfile
from pathlib import Path
from typing import Dict, List, Any, Generator
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StreamingNormalizer:
    def __init__(self, compressed_file: Path, output_dir: Path):
        self.compressed_file = compressed_file
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Track processing stats
        self.total_releases = 0
        self.matched_artists = 0
        self.matched_release_groups = 0

    def stream_releases_from_tar(self) -> Generator[Dict[str, Any], None, None]:
        """Stream releases directly from compressed tar file."""
        logger.info(f"Streaming releases from {self.compressed_file}")

        try:
            with tarfile.open(self.compressed_file, 'r:xz') as tar:
                # Find the release file in the archive
                release_member = None
                for member in tar.getmembers():
                    if member.name.endswith('/release'):
                        release_member = member
                        break

                if not release_member:
                    raise ValueError("No release file found in archive")

                logger.info(f"Found release file: {release_member.name}")

                # Extract and stream the release file
                with tar.extractfile(release_member) as release_file:
                    for line_num, line in enumerate(release_file):
                        line = line.decode('utf-8').strip()
                        if not line:
                            continue

                        self.total_releases += 1
                        if self.total_releases % 100000 == 0:
                            logger.info(f"Streamed {self.total_releases:,} releases...")

                        try:
                            release = json.loads(line)
                            yield release
                        except json.JSONDecodeError:
                            logger.warning(f"JSON decode error at line {line_num}")
                            continue

        except Exception as e:
            logger.error(f"Error streaming from tar file: {e}")
            raise

    def load_normalized_base_data(self, artist_file: Path, release_groups_file: Path) -> Dict[str, Any]:
        """Load pre-normalized artist and release-group data."""
        logger.info("Loading normalized base data...")

        # Load normalized artist data
        with open(artist_file, 'r') as f:
            artist_data = json.load(f)

        # Load normalized release-group data
        with open(release_groups_file, 'r') as f:
            release_groups_data = json.load(f)

        return {
            "artist": artist_data,
            "release_groups": release_groups_data
        }

    def enrich_with_releases(self, base_data: Dict[str, Any], target_artists: List[str] = None):
        """Enrich base data with release information from compressed file."""
        logger.info("Enriching base data with releases...")

        # Create lookup structures
        artist_lookup = {base_data["artist"]["id"]: base_data["artist"]}
        rg_lookup = {rg["id"]: rg for rg in base_data["release_groups"]}

        # Track which artists/release-groups we're looking for
        target_artist_ids = set(target_artists) if target_artists else {base_data["artist"]["id"]}

        start_time = time.time()

        # Stream releases and enrich
        for release in self.stream_releases_from_tar():
            # Check if this release is relevant
            relevant = False

            # Check artist credits
            artist_credits = release.get("artist-credit", [])
            for credit in artist_credits:
                artist = credit.get("artist", {})
                artist_id = artist.get("id")
                if artist_id in target_artist_ids:
                    relevant = True
                    self.matched_artists += 1
                    break

            # Check release group
            release_group = release.get("release-group", {})
            if isinstance(release_group, dict):
                rg_id = release_group.get("id")
                if rg_id in rg_lookup:
                    relevant = True
                    self.matched_release_groups += 1

            if relevant:
                # Enrich the data (this is where we'd add release details)
                self._enrich_release_data(release, base_data)

        elapsed = time.time() - start_time
        logger.info(f"Enrichment complete!")
        logger.info(f"Total releases processed: {self.total_releases:,}")
        logger.info(f"Artist matches: {self.matched_artists}")
        logger.info(f"Release-group matches: {self.matched_release_groups}")
        logger.info(f"Processing time: {elapsed:.1f}s")

        return base_data

    def _enrich_release_data(self, release: Dict[str, Any], base_data: Dict[str, Any]):
        """Enrich base data with release information."""
        # This is where we'd add release details to the normalized data
        # For now, just track that we found relevant releases
        pass

    def save_enriched_data(self, enriched_data: Dict[str, Any]):
        """Save the enriched normalized data."""
        logger.info("Saving enriched data...")

        # Save enriched artist data
        artist_file = self.output_dir / "enriched_artist.json"
        with open(artist_file, 'w') as f:
            json.dump(enriched_data["artist"], f, indent=2)

        # Save enriched release-group data
        rg_file = self.output_dir / "enriched_release_groups.json"
        with open(rg_file, 'w') as f:
            json.dump(enriched_data["release_groups"], f, indent=2)

        logger.info(f"Enriched data saved to {self.output_dir}")

def main():
    """Main function."""
    import argparse

    parser = argparse.ArgumentParser(description="Streaming normalization from compressed release file")
    parser.add_argument("--compressed-file", required=True,
                       help="Path to release.tar.xz file")
    parser.add_argument("--artist-file", required=True,
                       help="Path to normalized artist file")
    parser.add_argument("--release-groups-file", required=True,
                       help="Path to normalized release-groups file")
    parser.add_argument("--output-dir", default="local/streaming_output",
                       help="Output directory for enriched data")
    parser.add_argument("--target-artists", nargs="*",
                       help="Only process specific artist MBIDs")

    args = parser.parse_args()

    compressed_file = Path(args.compressed_file)
    artist_file = Path(args.artist_file)
    release_groups_file = Path(args.release_groups_file)
    output_dir = Path(args.output_dir)

    if not compressed_file.exists():
        logger.error(f"Compressed file not found: {compressed_file}")
        sys.exit(1)

    if not artist_file.exists():
        logger.error(f"Artist file not found: {artist_file}")
        sys.exit(1)

    if not release_groups_file.exists():
        logger.error(f"Release groups file not found: {release_groups_file}")
        sys.exit(1)

    logger.info("Starting streaming normalization...")
    logger.info(f"Compressed file: {compressed_file}")
    logger.info(f"Artist file: {artist_file}")
    logger.info(f"Release groups file: {release_groups_file}")
    logger.info(f"Output directory: {output_dir}")

    # Create normalizer
    normalizer = StreamingNormalizer(compressed_file, output_dir)

    # Load base data
    base_data = normalizer.load_normalized_base_data(artist_file, release_groups_file)

    # Enrich with releases
    enriched_data = normalizer.enrich_with_releases(base_data, args.target_artists)

    # Save enriched data
    normalizer.save_enriched_data(enriched_data)

    logger.info("Streaming normalization complete!")

if __name__ == "__main__":
    main()
