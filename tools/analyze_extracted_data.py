#!/usr/bin/env python3
"""
Analyze extracted artist and release-group data to understand structure and quality.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Set

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

def analyze_artist_data(artist_name: str) -> dict:
    """Analyze artist data structure and content."""
    artist_file = f"local/extracted_data/{artist_name.lower().replace(' ', '_')}_artist.json"

    with open(artist_file, 'r', encoding='utf-8') as f:
        artist_data = json.load(f)

    analysis = {
        "name": artist_data.get("name", "Unknown"),
        "type": artist_data.get("type", "Unknown"),
        "country": artist_data.get("country", "Unknown"),
        "begin_date": artist_data.get("begin-date", {}).get("year"),
        "end_date": artist_data.get("end-date", {}).get("year"),
        "disambiguation": artist_data.get("disambiguation", ""),
        "tags_count": len(artist_data.get("tags", [])),
        "genres_count": len(artist_data.get("genres", [])),
        "top_tags": [tag["name"] for tag in artist_data.get("tags", [])[:5]],
        "top_genres": [genre["name"] for genre in artist_data.get("genres", [])[:5]],
        "fields_present": list(artist_data.keys())
    }

    return analysis

def analyze_release_groups(artist_name: str) -> dict:
    """Analyze release group data structure and content."""
    rg_file = f"local/extracted_data/{artist_name.lower().replace(' ', '_')}_release_groups.json"

    with open(rg_file, 'r', encoding='utf-8') as f:
        release_groups = json.load(f)

    # Analyze release group types
    types = {}
    primary_types = {}
    secondary_types = {}

    for rg in release_groups:
        # Count types
        rg_type = rg.get("type", "Unknown")
        types[rg_type] = types.get(rg_type, 0) + 1

        # Count primary types
        primary_type = rg.get("primary-type", "Unknown")
        primary_types[primary_type] = primary_types.get(primary_type, 0) + 1

        # Count secondary types
        for sec_type in rg.get("secondary-types", []):
            secondary_types[sec_type] = secondary_types.get(sec_type, 0) + 1

    analysis = {
        "total_release_groups": len(release_groups),
        "types": types,
        "primary_types": primary_types,
        "secondary_types": secondary_types,
        "sample_titles": [rg["title"] for rg in release_groups[:5]],
        "fields_present": list(release_groups[0].keys()) if release_groups else []
    }

    return analysis

def main():
    """Main analysis function."""
    # Artist MBIDs from our extraction
    ARTISTS = {
        "Bob Dylan": "72c536dc-7137-4477-a521-567eeb840fa8",
        "Billy Joel": "64b94289-9474-4d43-8c93-918ccc1920d1",
        "Radiohead": "a74b1b7f-71a5-4011-9441-d0b5e4122711",
        "Pink Floyd": "83d91898-7763-47d7-b03b-b92132375c47",
        "The Rolling Stones": "b071f9fa-14b0-4217-8e97-eb41da73f598",
        "David Bowie": "5441c29d-3602-4898-b1a1-b77fa23b8e50",
    }

    logger.info("🎵 Analyzing extracted artist and release-group data...")

    results = {}

    for artist_name, mbid in ARTISTS.items():
        try:
            # Check if files exist
            artist_file = f"local/extracted_data/{artist_name.lower().replace(' ', '_')}_artist.json"
            rg_file = f"local/extracted_data/{artist_name.lower().replace(' ', '_')}_release_groups.json"

            if not Path(artist_file).exists():
                logger.warning(f"⚠️  No artist data for {artist_name}")
                continue

            if not Path(rg_file).exists():
                logger.warning(f"⚠️  No release group data for {artist_name}")
                continue

            # Analyze data
            artist_analysis = analyze_artist_data(artist_name)
            rg_analysis = analyze_release_groups(artist_name)

            results[artist_name] = {
                "artist": artist_analysis,
                "release_groups": rg_analysis
            }

            logger.info(f"✅ {artist_name}: {rg_analysis['total_release_groups']} release groups")

        except Exception as e:
            logger.error(f"❌ Error analyzing {artist_name}: {e}")
            continue

    # Summary report
    logger.info("\n📊 Analysis Summary:")
    logger.info("=" * 50)

    for artist_name, data in results.items():
        artist = data["artist"]
        rg = data["release_groups"]

        logger.info(f"\n🎤 {artist_name}")
        logger.info(f"   Type: {artist['type']}")
        logger.info(f"   Country: {artist['country']}")
        logger.info(f"   Years: {artist['begin_date']} - {artist['end_date']}")
        logger.info(f"   Tags: {artist['tags_count']} ({', '.join(artist['top_tags'])})")
        logger.info(f"   Genres: {artist['genres_count']} ({', '.join(artist['top_genres'])})")
        logger.info(f"   Release Groups: {rg['total_release_groups']}")
        logger.info(f"   Primary Types: {dict(list(rg['primary_types'].items())[:3])}")
        logger.info(f"   Sample Titles: {rg['sample_titles'][:3]}")

    # Overall statistics
    if results:
        total_rgs = sum(data["release_groups"]["total_release_groups"] for data in results.values())
        logger.info(f"\n📈 Overall Statistics:")
        logger.info(f"   Total Artists: {len(results)}")
        logger.info(f"   Total Release Groups: {total_rgs}")
        logger.info(f"   Average Release Groups per Artist: {total_rgs/len(results):.1f}")
    else:
        logger.info(f"\n📈 Overall Statistics:")
        logger.info(f"   Total Artists: 0")
        logger.info(f"   No data found")

if __name__ == "__main__":
    main()
