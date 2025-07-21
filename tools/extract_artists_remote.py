#!/usr/bin/env python3
"""
Extract releases for specific artists from the remote MusicBrainz release dump.
Run this on the remote server with the full release dump.
"""

import json
import sys
import logging
from pathlib import Path
from typing import Dict, List, Any, Set

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Artist MBIDs to extract
ARTISTS_TO_EXTRACT = {
    "a74b1b7f-71a5-4011-9441-d0b5e4122711": "Radiohead",
    "b7ffd2af-418f-4be2-bdd1-22f8a48614da": "The Beatles",
    "c0b2500e-0cef-4130-869d-732b23ed9df5": "Pink Floyd",
    "5c6acb91-4b9b-4dd6-978d-73e3e9a72e0b": "Queen",
    "678d88b2-87bf-4757-9607-2b86bde9f212": "Led Zeppelin",
    "b071f9fa-14b0-4217-8e97-eb41da73f598": "The Rolling Stones",
    "5441c29d-3602-4898-b1a1-b77fa23b8e50": "David Bowie",
    "72c536dc-7137-4477-a521-567eeb840fa8": "Bob Dylan"
}

def extract_artist_releases(release_file: Path, artist_mbids: Set[str]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Extract releases for specific artists from the release dump file.

    Args:
        release_file: Path to the release dump file
        artist_mbids: Set of artist MBIDs to extract

    Returns:
        Dict mapping artist MBID to list of their releases
    """
    logger.info(f"Extracting releases for {len(artist_mbids)} artists from {release_file}")

    artist_releases = {mbid: [] for mbid in artist_mbids}
    total_processed = 0
    found_releases = 0

    try:
        with open(release_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                total_processed += 1
                if total_processed % 100000 == 0:
                    logger.info(f"Processed {total_processed:,} releases... (found {found_releases} so far)")

                try:
                    release = json.loads(line)

                    # Check if this release has any of our target artists
                    artist_credits = release.get("artist-credit", [])
                    for credit in artist_credits:
                        artist = credit.get("artist", {})
                        artist_mbid = artist.get("id")

                        if artist_mbid in artist_mbids:
                            artist_releases[artist_mbid].append(release)
                            found_releases += 1
                            logger.debug(f"Found release for {artist.get('name', 'Unknown')}: {release.get('title', 'Unknown')}")
                            break  # Only count each release once per artist

                except json.JSONDecodeError:
                    continue

                # Safety check - stop if we've found enough releases
                if found_releases > 10000:  # Arbitrary limit
                    logger.info("Reached safety limit of 10,000 releases, stopping extraction")
                    break

    except Exception as e:
        logger.error(f"Error reading release file: {e}")
        return artist_releases

    logger.info(f"Extraction complete!")
    logger.info(f"Total releases processed: {total_processed:,}")
    logger.info(f"Total releases found: {found_releases}")

    for mbid, releases in artist_releases.items():
        artist_name = ARTISTS_TO_EXTRACT.get(mbid, "Unknown")
        logger.info(f"  {artist_name}: {len(releases)} releases")

    return artist_releases

def save_artist_releases(artist_releases: Dict[str, List[Dict[str, Any]]], output_dir: Path):
    """Save extracted releases to individual files per artist."""
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Saving releases to {output_dir}")

    for mbid, releases in artist_releases.items():
        artist_name = ARTISTS_TO_EXTRACT.get(mbid, "Unknown")
        safe_name = artist_name.lower().replace(" ", "_").replace("&", "and")

        filename = f"{safe_name}_releases.json"
        filepath = output_dir / filename

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(releases, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved {len(releases)} releases for {artist_name} to {filename}")

    # Save summary
    summary = {
        "extraction_info": {
            "total_artists": len(artist_releases),
            "total_releases": sum(len(releases) for releases in artist_releases.values()),
            "artists": {}
        }
    }

    for mbid, releases in artist_releases.items():
        artist_name = ARTISTS_TO_EXTRACT.get(mbid, "Unknown")
        summary["extraction_info"]["artists"][artist_name] = {
            "mbid": mbid,
            "release_count": len(releases),
            "release_titles": [r.get("title", "Unknown") for r in releases[:10]]  # First 10 titles
        }

    with open(output_dir / "extraction_summary.json", 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    logger.info("Extraction summary saved")

def main():
    """Main function."""
    logger.info("Starting remote artist release extraction...")

    # Configuration
    release_file = Path("/media/oldnas3/data/musicbrainz/mbdump/release")
    output_dir = Path("/tmp/extracted_releases")

    if not release_file.exists():
        logger.error(f"Release file not found: {release_file}")
        sys.exit(1)

    # Extract releases for our target artists
    artist_mbids = set(ARTISTS_TO_EXTRACT.keys())
    artist_releases = extract_artist_releases(release_file, artist_mbids)

    # Save the extracted releases
    save_artist_releases(artist_releases, output_dir)

    logger.info("Remote extraction complete!")
    logger.info(f"Files saved to: {output_dir}")
    logger.info("You can now copy these files back to your local machine for processing")

if __name__ == "__main__":
    main()
