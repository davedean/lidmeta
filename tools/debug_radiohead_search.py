#!/usr/bin/env python3
"""
Debug script to investigate Radiohead album search results.
"""

import json
from pathlib import Path
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Radiohead's MBID
RADIOHEAD_MBID = "a74b1b7f-71a5-4011-9441-d0b5e4122711"

def search_and_analyze(file_path: str, artist_mbid: str):
    """Search for albums and analyze the results."""
    albums = []
    total_checked = 0

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            total_checked += 1
            try:
                release_group = json.loads(line)

                # Check if this release-group has the artist in its artist-credit
                artist_credits = release_group.get("artist-credit", [])
                for credit in artist_credits:
                    artist = credit.get("artist", {})
                    if artist.get("id") == artist_mbid:
                        albums.append({
                            "id": release_group.get("id"),
                            "title": release_group.get("title"),
                            "primary-type": release_group.get("primary-type"),
                            "secondary-types": release_group.get("secondary-types", []),
                            "first-release-date": release_group.get("first-release-date")
                        })
                        break

            except json.JSONDecodeError:
                continue

    return albums, total_checked

def main():
    """Main function."""
    albums_only_file = "/Users/david/Projects/lidarr_musicbrainz_cache/lidarr-metadata-server/deploy/data/mbjson/dump-20250716-001001/release-group.albums-only"

    logger.info("🔍 Analyzing Radiohead albums in albums-only file...")
    logger.info("")

    albums, total_checked = search_and_analyze(albums_only_file, RADIOHEAD_MBID)

    logger.info(f"Total entries checked: {total_checked:,}")
    logger.info(f"Albums found: {len(albums)}")
    logger.info("")

    logger.info("Found albums:")
    logger.info("-" * 80)
    for i, album in enumerate(albums, 1):
        logger.info(f"{i:2d}. {album['title']}")
        logger.info(f"     ID: {album['id']}")
        logger.info(f"     Type: {album['primary-type']}")
        logger.info(f"     Secondary: {album['secondary-types']}")
        logger.info(f"     Date: {album['first-release-date']}")
        logger.info("")

    # Let's also check what we find in the full file for comparison
    logger.info("=" * 80)
    logger.info("🔍 Checking full file for comparison...")
    logger.info("")

    full_file = "/Users/david/Projects/lidarr_musicbrainz_cache/lidarr-metadata-server/deploy/data/mbjson/dump-20250716-001001/release-group"

    # Just get a sample of the first few Radiohead releases from full file
    full_albums = []
    count = 0

    with open(full_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                release_group = json.loads(line)

                artist_credits = release_group.get("artist-credit", [])
                for credit in artist_credits:
                    artist = credit.get("artist", {})
                    if artist.get("id") == RADIOHEAD_MBID:
                        if count < 20:  # Just show first 20
                            full_albums.append({
                                "title": release_group.get("title"),
                                "primary-type": release_group.get("primary-type"),
                                "secondary-types": release_group.get("secondary-types", [])
                            })
                        count += 1
                        break

            except json.JSONDecodeError:
                continue

            if count >= 20:
                break

    logger.info(f"First 20 Radiohead releases from full file:")
    logger.info("-" * 80)
    for i, album in enumerate(full_albums, 1):
        logger.info(f"{i:2d}. {album['title']} ({album['primary-type']}) - {album['secondary-types']}")

    logger.info("")
    logger.info(f"Total Radiohead releases in full file: {count}")

if __name__ == "__main__":
    main()
