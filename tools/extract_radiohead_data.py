#!/usr/bin/env python3
"""
Extract Radiohead's complete data from all dump files for analysis.
"""

import json
import sys
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Radiohead's MBID
RADIOHEAD_MBID = "a74b1b7f-71a5-4011-9441-d0b5e4122711"

def extract_artist_data(artist_file: Path, artist_mbid: str) -> Optional[Dict[str, Any]]:
    """Extract artist data from the artist dump file."""
    logger.info(f"Extracting artist data for {artist_mbid}")

    if not artist_file.exists():
        logger.error(f"Artist file not found: {artist_file}")
        return None

    try:
        with open(artist_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    artist_data = json.loads(line)
                    if artist_data.get("id") == artist_mbid:
                        logger.info(f"Found artist: {artist_data.get('name', 'Unknown')}")
                        return artist_data
                except json.JSONDecodeError:
                    continue

        logger.warning(f"Artist {artist_mbid} not found in {artist_file}")
        return None

    except Exception as e:
        logger.error(f"Error reading artist file: {e}")
        return None

def extract_release_groups(release_group_file: Path, artist_mbid: str) -> List[Dict[str, Any]]:
    """Extract all release-groups for the artist from the release-group dump file."""
    logger.info(f"Extracting release-groups for artist {artist_mbid}")

    if not release_group_file.exists():
        logger.error(f"Release-group file not found: {release_group_file}")
        return []

    release_groups = []
    total_processed = 0

    try:
        with open(release_group_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                total_processed += 1
                if total_processed % 100000 == 0:
                    logger.info(f"Processed {total_processed:,} release-groups...")

                try:
                    release_group = json.loads(line)

                    # Check if this release-group has the artist in its artist-credit
                    artist_credits = release_group.get("artist-credit", [])
                    for credit in artist_credits:
                        artist = credit.get("artist", {})
                        if artist.get("id") == artist_mbid:
                            release_groups.append(release_group)
                            logger.debug(f"Found release-group: {release_group.get('title', 'Unknown')}")
                            break

                except json.JSONDecodeError:
                    continue

        logger.info(f"Found {len(release_groups)} release-groups for artist {artist_mbid}")
        return release_groups

    except Exception as e:
        logger.error(f"Error reading release-group file: {e}")
        return []

def extract_releases(release_file: Path, release_group_ids: List[str]) -> List[Dict[str, Any]]:
    """Extract releases for the given release-group IDs from the release dump file."""
    logger.info(f"Extracting releases for {len(release_group_ids)} release-groups")

    if not release_file.exists():
        logger.error(f"Release file not found: {release_file}")
        return []

    releases = []
    total_processed = 0

    # Create a set for faster lookup
    release_group_id_set = set(release_group_ids)

    try:
        with open(release_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                total_processed += 1
                if total_processed % 100000 == 0:
                    logger.info(f"Processed {total_processed:,} releases...")

                try:
                    release = json.loads(line)

                    # Check if this release belongs to one of our release-groups
                    release_group = release.get("release-group", {})
                    if release_group.get("id") in release_group_id_set:
                        releases.append(release)
                        logger.debug(f"Found release: {release.get('title', 'Unknown')}")

                except json.JSONDecodeError:
                    continue

        logger.info(f"Found {len(releases)} releases for the release-groups")
        return releases

    except Exception as e:
        logger.error(f"Error reading release file: {e}")
        return []

def extract_radiohead_data(dump_dir: Path = None) -> Dict[str, Any]:
    """Extract all Radiohead data from dump files."""
    if dump_dir is None:
        # Try to find the current dump directory
        base_dir = Path("/app/data/mbjson")
        current_link = base_dir / "current"

        if current_link.exists() and current_link.is_symlink():
            dump_dir = current_link.resolve()
        else:
            # Try local development path
            dump_dir = Path("deploy/data/mbjson/current")
            if not dump_dir.exists():
                dump_dir = Path("deploy/data/mbjson/dump-20250716-001001")

    logger.info(f"Using dump directory: {dump_dir}")

    if not dump_dir.exists():
        logger.error(f"Dump directory not found: {dump_dir}")
        return {}

    # Extract artist data
    artist_data = extract_artist_data(dump_dir / "artist", RADIOHEAD_MBID)
    if not artist_data:
        logger.error("Failed to extract artist data")
        return {}

    # Extract release-group data
    release_groups = extract_release_groups(dump_dir / "release-group", RADIOHEAD_MBID)

    # Extract release data for each release-group
    release_group_ids = [rg["id"] for rg in release_groups]
    releases = extract_releases(dump_dir / "release", release_group_ids)

    return {
        "artist": artist_data,
        "release_groups": release_groups,
        "releases": releases
    }

def save_extracted_data(data: Dict[str, Any], output_dir: Path):
    """Save extracted data to files for analysis."""
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Saving extracted data to {output_dir}")

    # Save artist data
    with open(output_dir / "radiohead_artist.json", "w", encoding='utf-8') as f:
        json.dump(data["artist"], f, indent=2, ensure_ascii=False)

    # Save release-group data
    with open(output_dir / "radiohead_release_groups.json", "w", encoding='utf-8') as f:
        json.dump(data["release_groups"], f, indent=2, ensure_ascii=False)

    # Save release data
    with open(output_dir / "radiohead_releases.json", "w", encoding='utf-8') as f:
        json.dump(data["releases"], f, indent=2, ensure_ascii=False)

    # Save summary
    summary = {
        "artist": {
            "name": data["artist"].get("name", "Unknown"),
            "mbid": data["artist"].get("id"),
            "type": data["artist"].get("type")
        },
        "release_groups": {
            "count": len(data["release_groups"]),
            "types": {}
        },
        "releases": {
            "count": len(data["releases"]),
            "countries": {}
        }
    }

    # Analyze release-group types
    for rg in data["release_groups"]:
        primary_type = rg.get("primary-type", "Unknown")
        summary["release_groups"]["types"][primary_type] = summary["release_groups"]["types"].get(primary_type, 0) + 1

    # Analyze release countries
    for release in data["releases"]:
        country = release.get("country", "Unknown")
        summary["releases"]["countries"][country] = summary["releases"]["countries"].get(country, 0) + 1

    with open(output_dir / "radiohead_summary.json", "w", encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    logger.info("Data extraction complete!")
    logger.info(f"Artist: {summary['artist']['name']}")
    logger.info(f"Release groups: {summary['release_groups']['count']}")
    logger.info(f"Releases: {summary['releases']['count']}")

    # Log release-group types
    logger.info("Release group types:")
    for rg_type, count in summary["release_groups"]["types"].items():
        logger.info(f"  {rg_type}: {count}")

def main():
    """Main function."""
    logger.info("Starting Radiohead data extraction...")

    # Extract data
    data = extract_radiohead_data()

    if not data:
        logger.error("Failed to extract data")
        sys.exit(1)

    # Save to local directory
    output_dir = Path("local/extracted_data")
    save_extracted_data(data, output_dir)

    logger.info("Extraction complete!")

if __name__ == "__main__":
    main()
