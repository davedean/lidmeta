#!/usr/bin/env python3
"""
Debug script to examine the raw release data structure.
"""

import sys
import json
from pathlib import Path

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from lidarr_metadata_server.providers.reverse_index import ReleaseGroupReleaseIndex

# The Bends album MBID for testing
THE_BENDS_MBID = "b8048f24-c026-3398-b23a-b5e50716cbc7"

def debug_release_data():
    """Debug the release data structure."""
    print("=" * 80)
    print("RELEASE DATA DEBUG")
    print("=" * 80)

    dump_dir = Path("/Users/david/Projects/lidarr_musicbrainz_cache/lidarr-metadata-server/deploy/data/mbjson/dump-20250716-001001")
    release_file = dump_dir / "release"
    release_index_file = dump_dir / "release.releasegroup-index"

    # Test release index
    release_index = ReleaseGroupReleaseIndex(release_file, release_index_file)
    releases = release_index.find_releases(THE_BENDS_MBID)

    if not releases:
        print("No releases found!")
        return

    release = releases[0]
    print(f"Release ID: {release.get('id')}")
    print(f"Release title: {release.get('title')}")
    print(f"Release date: {release.get('date')}")
    print(f"Release status: {release.get('status')}")

    print(f"\nMedia count: {len(release.get('media', []))}")

    for i, medium in enumerate(release.get('media', [])):
        print(f"\nMedium {i+1}:")
        print(f"  Format: {medium.get('format')}")
        print(f"  Title: {medium.get('title')}")
        print(f"  Position: {medium.get('position')}")
        print(f"  Track count: {len(medium.get('tracks', []))}")

        for j, track in enumerate(medium.get('tracks', [])[:3]):  # Show first 3 tracks
            print(f"    Track {j+1}: {track.get('title')} (ID: {track.get('id')})")
            print(f"      Number: {track.get('number')}")
            print(f"      Position: {track.get('position')}")
            print(f"      Length: {track.get('length')}")
            print(f"      Recording: {track.get('recording', {}).get('id')}")

    print(f"\n" + "=" * 80)
    print("DEBUG COMPLETED")
    print("=" * 80)

if __name__ == "__main__":
    debug_release_data()
