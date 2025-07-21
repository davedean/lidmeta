#!/usr/bin/env python3
"""
Extract artist and release-group data for multiple artists from MusicBrainz dump.
"""

import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

# Updated artist MBIDs based on the fresh dump
ARTISTS = {
    "Bob Dylan": "72c536dc-7137-4477-a521-567eeb840fa8",
    "Billy Joel": "64b94289-9474-4d43-8c93-918ccc1920d1",
    "Radiohead": "a74b1b7f-71a5-4011-9441-d0b5e4122711",
    "Pink Floyd": "83d91898-7763-47d7-b03b-b92132375c47",
    "The Rolling Stones": "b071f9fa-14b0-4217-8e97-eb41da73f598",
    "David Bowie": "5441c29d-3602-4898-b1a1-b77fa23b8e50",
}

def extract_artist_data(artist_name: str, artist_mbid: str, dump_path: str) -> Optional[Dict]:
    """Extract artist data from the dump."""
    logger.info(f"Extracting artist data for {artist_name}...")

    with open(dump_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                artist = json.loads(line.strip())
                if artist.get('id') == artist_mbid:
                    return artist
            except json.JSONDecodeError:
                continue

    logger.warning(f"Artist {artist_name} not found in dump")
    return None

def extract_release_groups(artist_name: str, artist_mbid: str, dump_path: str) -> List[Dict]:
    """Extract release groups for an artist from the dump."""
    logger.info(f"Extracting release-group data for {artist_name}...")

    release_groups = []

    with open(dump_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                release_group = json.loads(line.strip())

                # Check if this release group belongs to our artist
                if 'artist-credit' in release_group:
                    for credit in release_group['artist-credit']:
                        if isinstance(credit, dict) and credit.get('artist', {}).get('id') == artist_mbid:
                            release_groups.append(release_group)
                            break

            except json.JSONDecodeError:
                continue

    return release_groups

def save_data(data: Dict, filepath: str):
    """Save data to JSON file."""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def main():
    """Main extraction function."""
    # Setup paths
    dump_dir = Path("deploy/data/mbjson/dump-20250716-001001")
    output_dir = Path("local/extracted_data")
    output_dir.mkdir(parents=True, exist_ok=True)

    artist_dump = dump_dir / "artist"
    release_group_dump = dump_dir / "release-group"

    if not artist_dump.exists():
        logger.error(f"Artist dump not found: {artist_dump}")
        return

    if not release_group_dump.exists():
        logger.error(f"Release group dump not found: {release_group_dump}")
        return

    # Process each artist
    for artist_name, artist_mbid in ARTISTS.items():
        logger.info(f"Processing {artist_name}...")

        # Extract artist data
        artist_data = extract_artist_data(artist_name, artist_mbid, artist_dump)
        if artist_data:
            artist_filename = f"{artist_name.lower().replace(' ', '_')}_artist.json"
            artist_filepath = output_dir / artist_filename
            save_data(artist_data, artist_filepath)
            logger.info(f"Saved artist data: {artist_filepath}")

        # Extract release groups
        release_groups = extract_release_groups(artist_name, artist_mbid, release_group_dump)
        if release_groups:
            release_group_filename = f"{artist_name.lower().replace(' ', '_')}_release_groups.json"
            release_group_filepath = output_dir / release_group_filename
            save_data(release_groups, release_group_filepath)
            logger.info(f"Saved {len(release_groups)} release groups: {release_group_filepath}")
        else:
            logger.warning(f"No release groups found for {artist_name}")

        # Check if we got any data
        if artist_data or release_groups:
            logger.info(f"✅ Successfully extracted data for {artist_name}")
        else:
            logger.warning(f"⚠️  Partial or no data extracted for {artist_name}")

        logger.info("")  # Empty line for readability

if __name__ == "__main__":
    main()
