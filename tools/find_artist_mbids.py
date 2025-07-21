#!/usr/bin/env python3
"""
Find correct MBIDs for artists in the MusicBrainz dump.
"""

import json
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

def find_artist_mbid(artist_name, dump_path):
    """Find the MBID for an artist by searching through the dump."""
    logger.info(f"Searching for artist: {artist_name}")

    found_artists = []

    with open(dump_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                artist = json.loads(line.strip())

                # Check if this is an artist entry (has 'id' and 'name' fields)
                if 'id' in artist and 'name' in artist:
                    # Check for exact name match
                    if artist['name'].lower() == artist_name.lower():
                        found_artists.append({
                            'name': artist['name'],
                            'id': artist['id'],
                            'type': artist.get('type', 'Unknown'),
                            'disambiguation': artist.get('disambiguation', ''),
                            'line': line_num
                        })

                    # Also check aliases
                    if 'aliases' in artist:
                        for alias in artist['aliases']:
                            if alias.get('name', '').lower() == artist_name.lower():
                                found_artists.append({
                                    'name': artist['name'],
                                    'alias': alias['name'],
                                    'id': artist['id'],
                                    'type': artist.get('type', 'Unknown'),
                                    'disambiguation': artist.get('disambiguation', ''),
                                    'line': line_num
                                })

            except json.JSONDecodeError:
                continue

            # Progress indicator
            if line_num % 100000 == 0:
                logger.info(f"Processed {line_num:,} lines...")

    return found_artists

def main():
    dump_path = Path("deploy/data/mbjson/dump-20250716-001001/artist")

    if not dump_path.exists():
        logger.error(f"Artist dump not found at {dump_path}")
        return

    # Artists to search for
    artists_to_find = [
        "Pink Floyd",
        "The Rolling Stones",
        "David Bowie",
        "Billy Joel",
        "Radiohead",
        "Bob Dylan"
    ]

    print("=" * 80)
    print("SEARCHING FOR ARTIST MBIDs IN MUSICBRAINZ DUMP")
    print("=" * 80)

    for artist_name in artists_to_find:
        print(f"\n🔍 Searching for: {artist_name}")
        print("-" * 50)

        found = find_artist_mbid(artist_name, dump_path)

        if found:
            print(f"✅ Found {len(found)} match(es):")
            for i, artist in enumerate(found, 1):
                print(f"  {i}. Name: {artist['name']}")
                if 'alias' in artist:
                    print(f"     Alias: {artist['alias']}")
                print(f"     MBID: {artist['id']}")
                print(f"     Type: {artist['type']}")
                if artist['disambiguation']:
                    print(f"     Disambiguation: {artist['disambiguation']}")
                print(f"     Line: {artist['line']:,}")
                print()
        else:
            print(f"❌ No matches found for '{artist_name}'")

    print("=" * 80)
    print("SEARCH COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    main()
