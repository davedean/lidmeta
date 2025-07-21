#!/usr/bin/env python3
"""
Benchmark script to compare search performance between full and filtered release-group files.
"""

import json
import time
import sys
from pathlib import Path

# Radiohead's MBID
RADIOHEAD_MBID = "a74b1b7f-71a5-4011-9441-d0b5e4122711"

def search_artist_albums(file_path: str, artist_mbid: str) -> tuple[int, float]:
    """
    Search for albums by artist MBID in the given file.

    Returns: (count, time_taken)
    """
    start_time = time.time()
    count = 0

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                release_group = json.loads(line)

                # Check if this release-group has the artist in its artist-credit
                artist_credits = release_group.get("artist-credit", [])
                for credit in artist_credits:
                    artist = credit.get("artist", {})
                    if artist.get("id") == artist_mbid:
                        count += 1
                        break

            except json.JSONDecodeError:
                continue

    end_time = time.time()
    return count, end_time - start_time

def get_file_size_mb(file_path: str) -> float:
    """Get file size in MB."""
    return Path(file_path).stat().st_size / (1024 * 1024)

def main():
    """Main function to run the benchmark."""
    # File paths
    full_file = "/Users/david/Projects/lidarr_musicbrainz_cache/lidarr-metadata-server/deploy/data/mbjson/dump-20250716-001001/release-group"
    albums_only_file = "/Users/david/Projects/lidarr_musicbrainz_cache/lidarr-metadata-server/deploy/data/mbjson/dump-20250716-001001/release-group.albums_only"

    print("=" * 80)
    print("SEARCH PERFORMANCE BENCHMARK")
    print("=" * 80)
    print(f"Searching for Radiohead (MBID: {RADIOHEAD_MBID})")
    print()

    # Check if files exist
    if not Path(full_file).exists():
        print(f"❌ Full file not found: {full_file}")
        return

    if not Path(albums_only_file).exists():
        print(f"❌ Albums-only file not found: {albums_only_file}")
        return

    # Get file sizes
    full_size = get_file_size_mb(full_file)
    albums_only_size = get_file_size_mb(albums_only_file)

    print(f"Full file size: {full_size:.2f} MB")
    print(f"Albums-only file size: {albums_only_size:.2f} MB")
    print(f"Size reduction: {((full_size - albums_only_size) / full_size * 100):.1f}%")
    print()

    # Test full file
    print("🔍 Searching in FULL file...")
    full_count, full_time = search_artist_albums(full_file, RADIOHEAD_MBID)
    print(f"   Found {full_count} releases in {full_time:.2f} seconds")
    print(f"   Speed: {full_count/full_time:.1f} releases/second")
    print()

    # Test albums-only file
    print("🔍 Searching in ALBUMS-ONLY file...")
    albums_count, albums_time = search_artist_albums(albums_only_file, RADIOHEAD_MBID)
    print(f"   Found {albums_count} albums in {albums_time:.2f} seconds")
    print(f"   Speed: {albums_count/albums_time:.1f} albums/second")
    print()

    # Calculate improvements
    time_improvement = ((full_time - albums_time) / full_time * 100) if full_time > 0 else 0
    speed_improvement = ((albums_count/albums_time) / (full_count/full_time) - 1) * 100 if full_time > 0 and albums_time > 0 else 0

    print("=" * 80)
    print("RESULTS SUMMARY")
    print("=" * 80)
    print(f"Full file:     {full_count} releases in {full_time:.2f}s ({full_count/full_time:.1f}/s)")
    print(f"Albums-only:   {albums_count} albums in {albums_time:.2f}s ({albums_count/albums_time:.1f}/s)")
    print()
    print(f"Time improvement:   {time_improvement:.1f}% faster")
    print(f"Speed improvement:  {speed_improvement:.1f}% faster")
    print(f"Relevance:          {albums_count}/{full_count} releases are albums ({albums_count/full_count*100:.1f}%)")
    print("=" * 80)

if __name__ == "__main__":
    main()
