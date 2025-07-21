#!/usr/bin/env python3
"""
Test script to verify the release index functionality.
"""

import sys
from pathlib import Path

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from lidarr_metadata_server.providers.reverse_index import ReleaseGroupReleaseIndex

# The Bends album MBID for testing
THE_BENDS_MBID = "b8048f24-c026-3398-b23a-b5e50716cbc7"

def test_release_index():
    """Test the release index functionality."""
    print("=" * 80)
    print("RELEASE INDEX TEST")
    print("=" * 80)

    dump_dir = Path("/Users/david/Projects/lidarr_musicbrainz_cache/lidarr-metadata-server/deploy/data/mbjson/dump-20250716-001001")
    release_file = dump_dir / "release"
    release_index_file = dump_dir / "release.releasegroup-index"

    if not release_file.exists():
        print(f"❌ Release file not found: {release_file}")
        return

    if not release_index_file.exists():
        print(f"❌ Release index file not found: {release_index_file}")
        return

    # Test release index
    release_index = ReleaseGroupReleaseIndex(release_file, release_index_file)

    print(f"\n1. Testing release index lookup...")

    # Get index stats
    stats = release_index.get_index_stats()
    print(f"  Release groups indexed: {stats['release_groups']:,}")
    print(f"  Total releases: {stats['total_entries']:,}")
    print(f"  Average releases per group: {stats['avg_releases_per_group']:.1f}")

    # Test finding releases for The Bends
    print(f"\n2. Testing release lookup for The Bends...")

    releases = release_index.find_releases(THE_BENDS_MBID)
    print(f"  Found {len(releases)} releases for The Bends")

    for i, release in enumerate(releases):
        print(f"    Release {i+1}: {release.get('title', 'Unknown')} (ID: {release.get('id', 'Unknown')})")
        print(f"      Status: {release.get('status', 'Unknown')}")
        print(f"      Date: {release.get('date', 'Unknown')}")
        print(f"      Media count: {len(release.get('media', []))}")

        # Show first few tracks
        all_tracks = []
        for medium in release.get('media', []):
            all_tracks.extend(medium.get('tracks', []))

        print(f"      Total tracks: {len(all_tracks)}")
        for j, track in enumerate(all_tracks[:3]):  # Show first 3 tracks
            print(f"        Track {j+1}: {track.get('title', 'Unknown')}")
        if len(all_tracks) > 3:
            print(f"        ... and {len(all_tracks) - 3} more tracks")

    print(f"\n" + "=" * 80)
    print("TEST COMPLETED")
    print("=" * 80)

if __name__ == "__main__":
    test_release_index()
