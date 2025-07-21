#!/usr/bin/env python3
"""
Test normalization using streaming releases and existing artist/release-group data.
"""

import json
import logging
import time
from pathlib import Path
from typing import Dict, List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

def load_artist_data(artist_mbid: str) -> dict:
    """Load artist data from extracted file."""
    artist_file = f"local/extracted_data/{artist_mbid}_artist.json"
    with open(artist_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_release_group_data(artist_mbid: str) -> List[dict]:
    """Load release group data from extracted file."""
    rg_file = f"local/extracted_data/{artist_mbid}_release_groups.json"
    with open(rg_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_streaming_releases(artist_mbid: str) -> List[dict]:
    """Load streaming releases for an artist."""
    releases_file = f"deploy/data/mbjson/dump-20250716-001001/streaming_releases/{artist_mbid}_releases.json"
    with open(releases_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def normalize_artist_with_streaming_releases(artist_name: str, artist_mbid: str) -> dict:
    """Normalize artist data using streaming releases."""
    logger.info(f"Normalizing {artist_name} with streaming releases...")

    # Load all data
    artist_data = load_artist_data(artist_mbid)
    release_groups = load_release_group_data(artist_mbid)
    releases = load_streaming_releases(artist_mbid)

    logger.info(f"  Artist data: {len(artist_data)} fields")
    logger.info(f"  Release groups: {len(release_groups)}")
    logger.info(f"  Releases: {len(releases)}")

    # Create normalized artist
    normalized_artist = {
        "id": artist_mbid,
        "name": artist_name,
        "type": artist_data.get("type", "Unknown"),
        "country": artist_data.get("country", "Unknown"),
        "begin_date": artist_data.get("begin-date", {}).get("year"),
        "end_date": artist_data.get("end-date", {}).get("year"),
        "disambiguation": artist_data.get("disambiguation", ""),
        "tags": [tag["name"] for tag in artist_data.get("tags", [])],
        "genres": [genre["name"] for genre in artist_data.get("genres", [])],
        "albums": []
    }

    # Process release groups into albums
    for rg in release_groups:
        # Find releases for this release group
        rg_releases = [r for r in releases if r.get("release-group", {}).get("id") == rg["id"]]

        if not rg_releases:
            logger.warning(f"  No releases found for release group {rg['id']}")
            continue

        # Create album from release group and its releases
        album = {
            "id": rg["id"],
            "title": rg["title"],
            "type": rg.get("type", "Unknown"),
            "primary_type": rg.get("primary-type", "Unknown"),
            "secondary_types": rg.get("secondary-types", []),
            "first_release_date": rg.get("first-release-date", ""),
            "releases": []
        }

        # Add release details
        for release in rg_releases:
            release_info = {
                "id": release["id"],
                "title": release["title"],
                "status": release.get("status", "Unknown"),
                "packaging": release.get("packaging", "Unknown"),
                "country": release.get("country", "Unknown"),
                "date": release.get("date", ""),
                "barcode": release.get("barcode", ""),
                "tracks": []
            }

            # Add track information if available
            if "mediums" in release:
                for medium in release["mediums"]:
                    if "tracks" in medium:
                        for track in medium["tracks"]:
                            track_info = {
                                "id": track["id"],
                                "title": track["title"],
                                "length": track.get("length"),
                                "position": track.get("position", 0),
                                "number": track.get("number", "")
                            }
                            release_info["tracks"].append(track_info)

            album["releases"].append(release_info)

        normalized_artist["albums"].append(album)

    return normalized_artist

def save_normalized_artist(artist_name: str, normalized_data: dict) -> str:
    """Save normalized artist data."""
    output_dir = Path("local/normalized_data/streaming_test")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{artist_name.lower().replace(' ', '_')}_normalized.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(normalized_data, f, indent=2, ensure_ascii=False)

    return str(output_file)

def main():
    """Main function to test streaming normalization."""
    # Artist MBIDs from our extraction
    ARTISTS = {
        "Bob Dylan": "72c536dc-7137-4477-a521-567eeb840fa8",
        "Billy Joel": "64b94289-9474-4d43-8c93-918ccc1920d1",
        "Radiohead": "a74b1b7f-71a5-4011-9441-d0b5e4122711",
        "Pink Floyd": "83d91898-7763-47d7-b03b-b92132375c47",
        "The Rolling Stones": "b071f9fa-14b0-4217-8e97-eb41da73f598",
        "David Bowie": "5441c29d-3602-4898-b1a1-b77fa23b8e50",
    }

    logger.info("🎵 Testing streaming normalization...")
    start_time = time.time()

    results = {}

    for artist_name, mbid in ARTISTS.items():
        try:
            # Check if streaming releases exist
            releases_file = f"deploy/data/mbjson/dump-20250716-001001/streaming_releases/{mbid}_releases.json"
            if not Path(releases_file).exists():
                logger.warning(f"⚠️  No streaming releases found for {artist_name}, skipping")
                continue

            # Normalize artist
            normalized = normalize_artist_with_streaming_releases(artist_name, mbid)

            # Save normalized data
            output_file = save_normalized_artist(artist_name, normalized)

            # Calculate stats
            total_releases = sum(len(album["releases"]) for album in normalized["albums"])
            total_tracks = sum(
                len(release["tracks"])
                for album in normalized["albums"]
                for release in album["releases"]
            )

            results[artist_name] = {
                "albums": len(normalized["albums"]),
                "releases": total_releases,
                "tracks": total_tracks,
                "output_file": output_file
            }

            logger.info(f"✅ {artist_name}: {len(normalized['albums'])} albums, {total_releases} releases, {total_tracks} tracks")

        except Exception as e:
            logger.error(f"❌ Error processing {artist_name}: {e}")
            continue

    elapsed_time = time.time() - start_time

    # Summary
    logger.info(f"\n🎵 Streaming normalization completed in {elapsed_time:.1f}s")
    logger.info("Results:")
    for artist_name, stats in results.items():
        logger.info(f"   {artist_name}: {stats['albums']} albums, {stats['releases']} releases, {stats['tracks']} tracks")

if __name__ == "__main__":
    main()
