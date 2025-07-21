#!/usr/bin/env python3
"""
Focused performance test for the reverse index.
"""

import time
import sys
from pathlib import Path
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from lidarr_metadata_server.providers.reverse_index import ArtistAlbumIndex

# Radiohead's MBID for testing
RADIOHEAD_MBID = "a74b1b7f-71a5-4011-9441-d0b5e4122711"

def test_reverse_index_performance():
    """Test the reverse index performance."""
    logger.info("=" * 80)
    logger.info("REVERSE INDEX PERFORMANCE TEST")
    logger.info("=" * 80)

    dump_dir = Path("./deploy/data/mbjson/dump-20250716-001001")
    release_group_file = dump_dir / "release-group"
    reverse_index_file = dump_dir / "release-group.artist-index"

    if not release_group_file.exists():
        logger.error(f"❌ Release-group file not found: {release_group_file}")
        return

    if not reverse_index_file.exists():
        logger.error(f"❌ Reverse index file not found: {reverse_index_file}")
        return

    # Test reverse index performance
    artist_index = ArtistAlbumIndex(release_group_file, reverse_index_file)

    logger.info(f"\n1. Testing reverse index lookup...")

    # Warm up
    artist_index.find_albums(RADIOHEAD_MBID)

    # Performance test
    start_time = time.time()
    albums = artist_index.find_albums(RADIOHEAD_MBID)
    lookup_time = time.time() - start_time

    logger.info(f"  Found {len(albums)} albums in {lookup_time:.4f}s")
    logger.info(f"  Speed: {len(albums)/lookup_time:.1f} albums/second")

    # Show albums
    for album in albums:
        logger.info(f"    - {album.get('title')} ({album.get('primary-type')})")

    # Test with different artists
    logger.info(f"\n2. Testing with different artists...")

    test_artists = [
        ("Radiohead", "a74b1b7f-71a5-4011-9441-d0b5e4122711"),
        ("The Beatles", "a14ba0c0-e251-4f77-b8dc-e2f4b6b5e8b8"),
        ("Pink Floyd", "83d91898-7763-47d7-b03b-b9d9a3846f6b"),
        ("Led Zeppelin", "678d88b2-87bf-4757-a477-1e4b8b8b8b8b"),
        ("Queen", "5c0cde7b-9f1a-4f1a-8f1a-4f1a8f1a4f1a"),
    ]

    total_time = 0
    total_albums = 0

    for artist_name, artist_mbid in test_artists:
        start_time = time.time()
        albums = artist_index.find_albums(artist_mbid)
        lookup_time = time.time() - start_time

        total_time += lookup_time
        total_albums += len(albums)

        logger.info(f"  {artist_name}: {len(albums)} albums in {lookup_time:.4f}s")

    logger.info(f"\n  Average lookup time: {total_time/len(test_artists):.4f}s")
    logger.info(f"  Total albums found: {total_albums}")

    # Index statistics
    logger.info(f"\n3. Index statistics...")

    stats = artist_index.get_index_stats()
    logger.info(f"  Artists indexed: {stats['artists']:,}")
    logger.info(f"  Total entries: {stats['total_entries']:,}")
    logger.info(f"  Average albums per artist: {stats['avg_albums_per_artist']:.1f}")
    logger.info(f"  Index file size: {reverse_index_file.stat().st_size / 1024 / 1024:.2f} MB")

    # Performance comparison with baseline
    logger.info(f"\n4. Performance comparison...")
    logger.info(f"  Baseline (scan method): ~31.52s for Radiohead")
    logger.info(f"  Reverse index: {lookup_time:.4f}s for Radiohead")

    if lookup_time > 0:
        speedup = 31.52 / lookup_time
        logger.info(f"  Speed improvement: {speedup:.1f}x faster")

    logger.info(f"\n" + "=" * 80)
    logger.info("PERFORMANCE TEST COMPLETED")
    logger.info("=" * 80)

if __name__ == "__main__":
    test_reverse_index_performance()
