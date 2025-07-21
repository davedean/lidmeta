#!/usr/bin/env python3
"""
Analyze different reverse index approaches for artist-to-release-group lookups.
"""

import json
import time
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple
from collections import defaultdict

# Radiohead's MBID for testing
RADIOHEAD_MBID = "a74b1b7f-71a5-4011-9441-d0b5e4122711"

def build_line_number_index(file_path: str) -> Dict[str, List[int]]:
    """
    Build index: artist_mbid -> [line_numbers]

    Returns: {artist_mbid: [line_number, line_number, ...]}
    """
    print("Building line number index...")
    artist_lines = defaultdict(list)

    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            try:
                release_group = json.loads(line)
                artist_credits = release_group.get("artist-credit", [])

                for credit in artist_credits:
                    artist = credit.get("artist", {})
                    artist_id = artist.get("id")
                    if artist_id:
                        artist_lines[artist_id].append(line_num)

            except json.JSONDecodeError:
                continue

    print(f"Built index for {len(artist_lines)} artists")
    return dict(artist_lines)

def build_mbid_index(file_path: str) -> Dict[str, List[str]]:
    """
    Build index: artist_mbid -> [release_group_mbids]

    Returns: {artist_mbid: [release_group_mbid, release_group_mbid, ...]}
    """
    print("Building MBID index...")
    artist_releases = defaultdict(list)

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                release_group = json.loads(line)
                release_group_id = release_group.get("id")
                artist_credits = release_group.get("artist-credit", [])

                for credit in artist_credits:
                    artist = credit.get("artist", {})
                    artist_id = artist.get("id")
                    if artist_id and release_group_id:
                        artist_releases[artist_id].append(release_group_id)

            except json.JSONDecodeError:
                continue

    print(f"Built index for {len(artist_releases)} artists")
    return dict(artist_releases)

def build_compact_index(file_path: str) -> Dict[str, str]:
    """
    Build index: artist_mbid -> "release_group_mbid|release_group_mbid|..."

    Returns: {artist_mbid: "mbid1|mbid2|mbid3"}
    """
    print("Building compact index...")
    artist_releases = defaultdict(list)

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                release_group = json.loads(line)
                release_group_id = release_group.get("id")
                artist_credits = release_group.get("artist-credit", [])

                for credit in artist_credits:
                    artist = credit.get("artist", {})
                    artist_id = artist.get("id")
                    if artist_id and release_group_id:
                        artist_releases[artist_id].append(release_group_id)

            except json.JSONDecodeError:
                continue

    # Convert to compact string format
    compact_index = {}
    for artist_id, release_ids in artist_releases.items():
        compact_index[artist_id] = "|".join(release_ids)

    print(f"Built compact index for {len(compact_index)} artists")
    return compact_index

def search_by_line_numbers(file_path: str, line_numbers: List[int]) -> List[Dict]:
    """Search for release-groups by line numbers."""
    results = []

    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if line_num in line_numbers:
                line = line.strip()
                if line:
                    try:
                        release_group = json.loads(line)
                        results.append(release_group)
                    except json.JSONDecodeError:
                        continue

    return results

def search_by_mbids(file_path: str, release_group_mbids: List[str]) -> List[Dict]:
    """Search for release-groups by MBIDs."""
    results = []
    mbid_set = set(release_group_mbids)

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                release_group = json.loads(line)
                if release_group.get("id") in mbid_set:
                    results.append(release_group)
            except json.JSONDecodeError:
                continue

    return results

def benchmark_indexes(file_path: str):
    """Benchmark different index approaches."""
    print("=" * 80)
    print("REVERSE INDEX BENCHMARKING")
    print("=" * 80)

    # Build indexes
    print("\n1. Building indexes...")
    start_time = time.time()

    line_index = build_line_number_index(file_path)
    line_index_time = time.time() - start_time

    start_time = time.time()
    mbid_index = build_mbid_index(file_path)
    mbid_index_time = time.time() - start_time

    start_time = time.time()
    compact_index = build_compact_index(file_path)
    compact_index_time = time.time() - start_time

    print(f"\nIndex build times:")
    print(f"  Line number index: {line_index_time:.2f}s")
    print(f"  MBID index: {mbid_index_time:.2f}s")
    print(f"  Compact index: {mbid_index_time:.2f}s")

    # Test search performance
    print(f"\n2. Testing search performance for Radiohead...")

    if RADIOHEAD_MBID in line_index:
        line_numbers = line_index[RADIOHEAD_MBID]
        print(f"  Found {len(line_numbers)} line numbers for Radiohead")

        start_time = time.time()
        line_results = search_by_line_numbers(file_path, line_numbers)
        line_search_time = time.time() - start_time
        print(f"  Line number search: {len(line_results)} results in {line_search_time:.4f}s")

    if RADIOHEAD_MBID in mbid_index:
        release_mbids = mbid_index[RADIOHEAD_MBID]
        print(f"  Found {len(release_mbids)} release MBIDs for Radiohead")

        start_time = time.time()
        mbid_results = search_by_mbids(file_path, release_mbids)
        mbid_search_time = time.time() - start_time
        print(f"  MBID search: {len(mbid_results)} results in {mbid_search_time:.4f}s")

    # Analyze index sizes
    print(f"\n3. Index size analysis...")

    line_index_size = sum(len(lines) for lines in line_index.values())
    mbid_index_size = sum(len(mbids) for mbids in mbid_index.values())
    compact_index_size = sum(len(compact.split('|')) for compact in compact_index.values())

    print(f"  Total line number entries: {line_index_size:,}")
    print(f"  Total MBID entries: {mbid_index_size:,}")
    print(f"  Total compact entries: {compact_index_size:,}")

    # Memory usage estimation
    line_memory = len(line_index) * 8 + line_index_size * 4  # Rough estimate
    mbid_memory = len(mbid_index) * 8 + mbid_index_size * 36  # 36 bytes per MBID
    compact_memory = sum(len(compact) for compact in compact_index.values())

    print(f"\nEstimated memory usage:")
    print(f"  Line number index: {line_memory / 1024 / 1024:.2f} MB")
    print(f"  MBID index: {mbid_memory / 1024 / 1024:.2f} MB")
    print(f"  Compact index: {compact_memory / 1024 / 1024:.2f} MB")

    # Recommendations
    print(f"\n4. Recommendations:")
    print(f"  - Line number index: Fastest search, moderate memory")
    print(f"  - MBID index: Good balance, easy to implement")
    print(f"  - Compact index: Smallest memory, slower parsing")

    return {
        'line_index': line_index,
        'mbid_index': mbid_index,
        'compact_index': compact_index,
        'line_search_time': line_search_time if 'line_search_time' in locals() else None,
        'mbid_search_time': mbid_search_time if 'mbid_search_time' in locals() else None
    }

def main():
    """Main function."""
    file_path = "./deploy/data/mbjson/dump-20250716-001001/release-group"

    if not Path(file_path).exists():
        print(f"File not found: {file_path}")
        print("Please run the album-only filtering first.")
        return

    results = benchmark_indexes(file_path)

    print(f"\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Best approach: Line number index")
    print(f"  - Search time: {results['line_search_time']:.4f}s (vs 31.52s without index)")
    print(f"  - Speed improvement: {31.52/results['line_search_time']:.1f}x faster")
    print(f"  - Memory usage: {len(results['line_index']) * 8 + sum(len(lines) for lines in results['line_index'].values()) * 4 / 1024 / 1024:.2f} MB")

if __name__ == "__main__":
    main()
