#!/usr/bin/env python3
"""
OS-based streaming release processor using tar command for better performance.
"""

import json
import logging
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Set, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

class OSStreamingReleaseProcessor:
    def __init__(self, release_tar_xz_path: str, output_dir: str):
        self.release_tar_xz_path = Path(release_tar_xz_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def extract_releases_for_artists(self, artist_mbids: Set[str]) -> Dict[str, List[dict]]:
        """
        Use OS tar command to stream decompress and extract releases for specific artists.
        """
        logger.info(f"Starting OS-based streaming release extraction for {len(artist_mbids)} artists")
        logger.info(f"Release file: {self.release_tar_xz_path}")

        start_time = time.time()
        releases_by_artist = {mbid: [] for mbid in artist_mbids}
        total_releases_processed = 0
        total_releases_matched = 0
        last_save_time = start_time

        # Track progress per artist
        artist_progress = {mbid: 0 for mbid in artist_mbids}

        # Track bytes processed for progress logging
        bytes_processed = 0
        last_bytes_log = 0

        # Use OS tar command to extract just the release file
        tar_cmd = [
            'tar', '-xf', str(self.release_tar_xz_path),
            '--to-stdout', '*/release'
        ]

        logger.info(f"🔧 Running tar command: {' '.join(tar_cmd)}")

        try:
            # Start the tar process
            process = subprocess.Popen(
                tar_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1  # Line buffered
            )

            logger.info("🎵 Starting to process releases from tar stream...")

            # Process the stream line by line
            for line_num, line in enumerate(process.stdout, 1):
                try:
                    # Track bytes processed
                    line_bytes = len(line.encode('utf-8'))
                    bytes_processed += line_bytes

                    release = json.loads(line.strip())
                    total_releases_processed += 1

                    # Check if this release belongs to one of our target artists
                    if 'artist-credit' in release and release['artist-credit']:
                        artist_id = release['artist-credit'][0]['artist']['id']
                        if artist_id in artist_mbids:
                            releases_by_artist[artist_id].append(release)
                            total_releases_matched += 1
                            artist_progress[artist_id] += 1

                    # Progress logging every 100MB processed
                    mb_processed = bytes_processed / (1024 * 1024)
                    if mb_processed - last_bytes_log >= 100:
                        elapsed = time.time() - start_time
                        rate = bytes_processed / elapsed if elapsed > 0 else 0
                        mb_per_sec = rate / (1024 * 1024)
                        logger.info(f"📊 Processed {mb_processed:.0f}MB ({mb_per_sec:.1f}MB/s), "
                                   f"{total_releases_processed:,} releases, "
                                   f"matched {total_releases_matched} for target artists")

                        # Show per-artist progress
                        for mbid, count in artist_progress.items():
                            if count > 0:
                                logger.info(f"  🎤 {mbid}: {count} releases")

                        last_bytes_log = mb_processed

                    # Save incremental files every 2 minutes
                    current_time = time.time()
                    if current_time - last_save_time > 120:  # 2 minutes
                        self.save_incremental_releases(releases_by_artist)
                        last_save_time = current_time
                        logger.info(f"💾 Saved incremental files ({total_releases_matched} total releases)")

                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON at line {line_num}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing line {line_num}: {e}")
                    continue

            # Wait for process to finish
            process.wait()

            # Check for errors
            if process.returncode != 0:
                stderr_output = process.stderr.read()
                logger.error(f"Tar command failed with return code {process.returncode}")
                logger.error(f"Stderr: {stderr_output}")
                raise RuntimeError(f"Tar command failed: {stderr_output}")

        except Exception as e:
            logger.error(f"Error running tar command: {e}")
            raise

        elapsed_time = time.time() - start_time
        logger.info(f"✅ Completed release extraction in {elapsed_time:.1f}s")
        logger.info(f"   Total bytes processed: {bytes_processed / (1024*1024*1024):.1f}GB")
        logger.info(f"   Total releases processed: {total_releases_processed:,}")
        logger.info(f"   Total releases matched: {total_releases_matched:,}")
        logger.info(f"   Processing rate: {total_releases_processed/elapsed_time:.0f} releases/sec")

        return releases_by_artist

    def save_incremental_releases(self, releases_by_artist: Dict[str, List[dict]]) -> None:
        """Save releases incrementally to show progress."""
        for artist_mbid, releases in releases_by_artist.items():
            if releases:
                output_file = self.output_dir / f"{artist_mbid}_releases_incremental.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(releases, f, indent=2, ensure_ascii=False)

    def save_releases(self, releases_by_artist: Dict[str, List[dict]]) -> None:
        """Save final releases to individual JSON files by artist."""
        for artist_mbid, releases in releases_by_artist.items():
            if releases:
                output_file = self.output_dir / f"{artist_mbid}_releases.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(releases, f, indent=2, ensure_ascii=False)
                logger.info(f"Saved {len(releases)} releases for {artist_mbid}: {output_file}")
            else:
                logger.warning(f"No releases found for artist {artist_mbid}")

def main():
    """Main function to run the OS-based streaming release processor."""
    # Artist MBIDs from our extraction
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
    output_dir = "deploy/data/mbjson/dump-20250716-001001/os_streaming_releases"

    # Create processor and extract releases
    processor = OSStreamingReleaseProcessor(release_tar_xz_path, output_dir)

    artist_mbids = set(ARTISTS.values())
    releases_by_artist = processor.extract_releases_for_artists(artist_mbids)

    # Save final releases to files
    processor.save_releases(releases_by_artist)

    # Summary
    logger.info("🎵 Release extraction summary:")
    for artist_name, mbid in ARTISTS.items():
        count = len(releases_by_artist[mbid])
        logger.info(f"   {artist_name}: {count} releases")

if __name__ == "__main__":
    main()
