#!/usr/bin/env python3
"""
Test script to verify reverse index integration with the dump provider.
"""

import asyncio
import time
import sys
from pathlib import Path

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from lidarr_metadata_server.providers.dump import fetch_artist, _find_artist_albums
from lidarr_metadata_server.providers.reverse_index import ArtistAlbumIndex

# Radiohead's MBID for testing
RADIOHEAD_MBID = "a74b1b7f-71a5-4011-9441-d0b5e4122711"

async def test_reverse_index_integration():
    """Test the reverse index integration."""
    print("=" * 80)
    print("REVERSE INDEX INTEGRATION TEST")
    print("=" * 80)

    # Test 1: Direct reverse index usage
    print("\n1. Testing direct reverse index usage...")

    dump_dir = Path("./deploy/data/mbjson/dump-20250716-001001")
    release_group_file = dump_dir / "release-group"
    reverse_index_file = dump_dir / "release-group.artist-index"

    if not release_group_file.exists():
        print(f"❌ Release-group file not found: {release_group_file}")
        return

    if not reverse_index_file.exists():
        print(f"❌ Reverse index file not found: {reverse_index_file}")
        print("Please build the reverse index first.")
        return

    # Test direct reverse index lookup
    artist_index = ArtistAlbumIndex(release_group_file, reverse_index_file)

    start_time = time.time()
    albums = artist_index.find_albums(RADIOHEAD_MBID)
    direct_time = time.time() - start_time

    print(f"  Direct reverse index lookup: {len(albums)} albums in {direct_time:.4f}s")
    for album in albums:
        print(f"    - {album.get('title')} ({album.get('primary-type')})")

    # Test 2: Provider integration
    print(f"\n2. Testing provider integration...")

    start_time = time.time()
    try:
        artist_data = await fetch_artist(RADIOHEAD_MBID)
        provider_time = time.time() - start_time

        albums = artist_data.get("Albums", [])
        print(f"  Provider integration: {len(albums)} albums in {provider_time:.4f}s")

        # Show album details
        for album in albums:
            print(f"    - {album.get('Title')} ({album.get('Type')})")

    except Exception as e:
        print(f"  ❌ Provider integration failed: {e}")
        import traceback
        print(f"  Traceback: {traceback.format_exc()}")

    # Test 3: Performance comparison
    print(f"\n3. Performance comparison...")

    # Test the old scan method
    start_time = time.time()
    scan_albums = _find_artist_albums_scan(RADIOHEAD_MBID, limit=10)  # Limit for speed
    scan_time = time.time() - start_time

    print(f"  Scan method (limited): {len(scan_albums)} albums in {scan_time:.4f}s")
    print(f"  Direct reverse index: {len(albums)} albums in {direct_time:.4f}s")

    if scan_time > 0 and direct_time > 0:
        speedup = scan_time / direct_time
        print(f"  Speed improvement: {speedup:.1f}x faster")

    # Test 4: Index statistics
    print(f"\n4. Index statistics...")

    stats = artist_index.get_index_stats()
    print(f"  Artists indexed: {stats['artists']:,}")
    print(f"  Total entries: {stats['total_entries']:,}")
    print(f"  Average albums per artist: {stats['avg_albums_per_artist']:.1f}")

    print(f"\n" + "=" * 80)
    print("TEST COMPLETED")
    print("=" * 80)

def _find_artist_albums_scan(artist_mbid: str, limit: int = 200):
    """Fallback scan method for testing."""
    import json
    import mmap

    dump_dir = Path("./deploy/data/mbjson/dump-20250716-001001")
    release_group_file = dump_dir / "release-group"

    if not release_group_file.exists():
        return []

    albums = []
    try:
        with open(release_group_file, 'rb') as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

            for line_num, line in enumerate(mm):
                if len(albums) >= limit:
                    break

                try:
                    release_group = json.loads(line.decode('utf-8').strip())

                    artist_credits = release_group.get("artist-credit", [])
                    for credit in artist_credits:
                        artist_obj = credit.get("artist", {})
                        if artist_obj.get("id") == artist_mbid:
                            albums.append({
                                "Id": release_group["id"],
                                "Title": release_group.get("title", ""),
                                "Type": release_group.get("primary-type", "Album"),
                                "ReleaseStatuses": ["Official"],
                                "SecondaryTypes": release_group.get("secondary-types", []),
                                "OldIds": [],
                            })
                            break

                except json.JSONDecodeError:
                    continue

            mm.close()

    except Exception as e:
        print(f"Error in scan method: {e}")

    return albums

if __name__ == "__main__":
    asyncio.run(test_reverse_index_integration())
