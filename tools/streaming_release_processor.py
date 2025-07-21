#!/usr/bin/env python3
"""
Streaming release processor for MusicBrainz dump.
Decompresses release.tar.xz and extracts releases for specific artists.
"""

import json
import logging
import tarfile
import time
from pathlib import Path
from typing import Dict, List, Set, Optional
import lzma

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

class StreamingReleaseProcessor:
    def __init__(self, release_tar_xz_path: str, output_dir: str):
        self.release_tar_xz_path = Path(release_tar_xz_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def extract_releases_for_artists(self, artist_mbids: Set[str]) -> Dict[str, List[dict]]:
        """
        Stream decompress release.tar.xz and extract releases for specific artists.

        Args:
            artist_mbids: Set of artist MBIDs to extract releases for

        Returns:
            Dict mapping artist MBID to list of their releases
        """
        logger.info(f"Starting streaming release extraction for {len(artist_mbids)} artists")
        logger.info(f"Release file: {self.release_tar_xz_path}")

        start_time = time.time()
        releases_by_artist = {mbid: [] for mbid in artist_mbids}
        total_releases_processed = 0
        total_releases_matched = 0

        # Open the tar.xz file for streaming
        with lzma.open(self.release_tar_xz_path, 'rb') as xz_file:
            with tarfile.open(fileobj=xz_file, mode='r') as tar:
                # Find the release file in the tar archive
                release_member = None
                for member in tar.getmembers():
                    if member.name.endswith('/release'):
                        release_member = member
                        break

                if not release_member:
                    raise FileNotFoundError("No 'release' file found in tar archive")

                logger.info(f"Found release file: {release_member.name} ({release_member.size:,} bytes)")

                # Extract and process the release file
                release_file = tar.extractfile(release_member)
                if not release_file:
                    raise ValueError("Could not extract release file from tar archive")

                # Process releases line by line
                for line_num, line in enumerate(release_file, 1):
                    try:
                        release = json.loads(line.decode('utf-8').strip())
                        total_releases_processed += 1

                        # Check if this release belongs to one of our target artists
                        if 'artist-credit' in release and release['artist-credit']:
                            artist_id = release['artist-credit'][0]['artist']['id']
                            if artist_id in artist_mbids:
                                releases_by_artist[artist_id].append(release)
                                total_releases_matched += 1

                        # Progress logging
                        if line_num % 100000 == 0:
                            elapsed = time.time() - start_time
                            rate = line_num / elapsed if elapsed > 0 else 0
                            logger.info(f"Processed {line_num:,} releases ({rate:.0f}/sec), "
                                       f"matched {total_releases_matched} for target artists")

                    except json.JSONDecodeError as e:
                        logger.warning(f"Invalid JSON at line {line_num}: {e}")
                        continue
                    except Exception as e:
                        logger.error(f"Error processing line {line_num}: {e}")
                        continue

        elapsed_time = time.time() - start_time
        logger.info(f"✅ Completed release extraction in {elapsed_time:.1f}s")
        logger.info(f"   Total releases processed: {total_releases_processed:,}")
        logger.info(f"   Total releases matched: {total_releases_matched:,}")
        logger.info(f"   Processing rate: {total_releases_processed/elapsed_time:.0f} releases/sec")

        return releases_by_artist

    def save_releases(self, releases_by_artist: Dict[str, List[dict]]) -> None:
        """Save releases to individual JSON files by artist."""
        for artist_mbid, releases in releases_by_artist.items():
            if releases:
                output_file = self.output_dir / f"{artist_mbid}_releases.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(releases, f, indent=2, ensure_ascii=False)
                logger.info(f"Saved {len(releases)} releases for {artist_mbid}: {output_file}")
            else:
                logger.warning(f"No releases found for artist {artist_mbid}")

def main():
    """Main function to run the streaming release processor."""
    # Artist MBIDs from our extraction (using the correct ones from fresh dump)
    ARTISTS = {
        "Bob Dylan": "72c536dc-7137-4477-a521-567eeb840fa8",
        "Billy Joel": "64b94289-9474-4d43-8c93-918ccc1920d1",
        "Radiohead": "a74b1b7f-71a5-4011-9441-d0b5e4122711",
        "Pink Floyd": "83d91898-7763-47d7-b03b-b92132375c47",
        "The Rolling Stones": "b071f9fa-14b0-4217-8e97-eb41da73f598",
        "David Bowie": "5441c29d-3602-4898-b1a1-b77fa23b8e50",
    }

    # Configuration
    release_tar_xz_path = "deploy/data/mbjson/dump-20250716-001001/release.tar.xz"
    output_dir = "deploy/data/mbjson/dump-20250716-001001/streaming_releases"

    # Create processor and extract releases
    processor = StreamingReleaseProcessor(release_tar_xz_path, output_dir)

    artist_mbids = set(ARTISTS.values())
    releases_by_artist = processor.extract_releases_for_artists(artist_mbids)

    # Save releases to files
    processor.save_releases(releases_by_artist)

    # Summary
    logger.info("🎵 Release extraction summary:")
    for artist_name, mbid in ARTISTS.items():
        count = len(releases_by_artist[mbid])
        logger.info(f"   {artist_name}: {count} releases")

if __name__ == "__main__":
    main()
