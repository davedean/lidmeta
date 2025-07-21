#!/usr/bin/env python3
"""
Process raw dump data into normalized, Lidarr-compliant payloads.
"""

import json
import sys
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def format_release_date(date_str: str) -> str:
    """Formats a release date string into YYYY-MM-DD."""
    if not date_str:
        return ""
    try:
        if len(date_str) == 4:  # YYYY
            return f"{date_str}-01-01"
        if len(date_str) == 7:  # YYYY-MM
            return f"{date_str}-01"
        return date_str
    except Exception:
        return ""

def extract_rating(rating_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Extract rating information."""
    if not rating_data:
        return {"count": 0, "value": 0.0}
    return {"count": rating_data.get("vote-count", 0), "value": rating_data.get("value") or 0.0}


def extract_genres(tags: List[Dict[str, Any]]) -> List[str]:
    """Extracts a list of genre names from a list of tag dictionaries."""
    if not tags:
        return []
    return [tag["name"] for tag in tags if tag.get("name")]


def extract_links(relations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Extracts a list of web links from a list of relation dictionaries."""
    if not relations:
        return []
    links = []
    for rel in relations:
        if rel.get("type") and rel.get("url") and rel["url"].get("resource"):
            links.append({"type": rel["type"], "target": rel["url"]["resource"]})
    return links


def _create_normalized_artist_base(artist):
    """Creates a base artist object that matches Lidarr's expected schema, used in both artist and album normalization."""
    logger.debug(f"Normalizing artist: {artist.get('id')}, type: {artist.get('type')}")
    aliases = artist.get("aliases")
    if aliases is None:
        aliases = []

    normalized_artist = {
        "id": artist.get("id"),
        "artistid": artist.get("id"),
        "artistname": artist.get("name"),
        "sortname": artist.get("sort-name"),
        "disambiguation": artist.get("disambiguation"),
        "type": artist.get("type") or "Unknown",
        "gender": artist.get("gender"),
        "country": artist.get("country"),
        "area": artist.get("area").get("name") if artist.get("area") else None,
        "status": "ended" if artist.get("life-span", {}).get("ended") else "active",
        "artistaliases": [a.get("name") for a in aliases if a.get("name")],
        "tags": [t.get("name") for t in artist.get("tags", [])],
        "rating": extract_rating(artist.get("rating")),
        "genres": extract_genres(artist.get("tags", [])),
        "links": extract_links(artist.get("relations", [])),
        "images": [],  # Defaulting to empty as we don't handle images yet
        "overview": artist.get("annotation") or "",
        "oldids": [],  # Not available in the data we are processing
    }
    logger.debug(f"Normalized artist output: {normalized_artist}")
    return normalized_artist


def normalize_radiohead_artist_data(artist, release_groups):
    """Normalizes artist data to match the Lidarr artist schema, including album summaries."""
    if not artist:
        return None

    # Create the base artist object
    normalized_artist = _create_normalized_artist_base(artist)

    # Create album summaries from the artist's release groups, strictly adhering to the spec.
    album_summaries = []
    for rg in release_groups:
        album_summaries.append({
            "Id": rg.get("id"),
            "Title": rg.get("title"),
            "Type": rg.get("primary-type"),
            "SecondaryTypes": rg.get("secondary-types", []),
            "ReleaseStatuses": ["Official"],  # Defaulting as this is not in release-group data
            "OldIds": [], # Not available in release-group data
        })

    # Sort albums by title for consistent output
    album_summaries.sort(key=lambda x: x.get("Title", ""))
    normalized_artist["Albums"] = album_summaries

    return normalized_artist


def normalize_album_data(release_group, artist, releases):
    """Normalizes a release group into the Lidarr album schema."""
    if not release_group or not artist:
        return None

    # Base album structure from the release group, ensuring no null lists
    normalized_album = {
        "id": release_group.get("id"),
        "title": release_group.get("title", ""),
        "artistid": artist.get("id"),  # Ensure top-level artist ID is present
        "type": release_group.get("primary-type", "Album"),
        "disambiguation": release_group.get("disambiguation", ""),
        "overview": release_group.get("annotation") or "",
        "releasedate": format_release_date(release_group.get("first-release-date", "")),
        "rating": extract_rating(release_group.get("rating")) or {"count": 0, "value": 0.0},
        "genres": extract_genres(release_group.get("tags", [])) or [],
        "releases": [],  # Default to empty list, populated below
        "secondarytypes": release_group.get("secondary-types", []),  # must always be present
        "artists": [_create_normalized_artist_base(artist)] or [],
        "images": [],
        "links": extract_links(release_group.get("relations", [])) or [],
        "aliases": [a.get("name") for a in release_group.get("aliases", []) if a.get("name")] or [],
        "oldids": [],
    }

    # If we don't have detailed release data, create a single, realistic placeholder release.
    # This is crucial for local development mode to work without crashing Lidarr.
    if not releases:
        placeholder_tracks = []
        track_count = 1  # A reasonable guess for a standard album
        for i in range(1, track_count + 1):
            placeholder_tracks.append({
                "id": f"placeholder-track-{release_group['id']}-{i}",
                "trackname": f"Track {i}",
                "tracknumber": str(i),
                "trackposition": i,
                "durationms": 0,
                "artistid": artist["id"],
                "recordingid": "",
                "mediumnumber": 1,
                "oldids": [],
                "oldrecordingids": []
            })

        placeholder_release = {
            "id": release_group["id"], # Use RG ID as a stand-in
            "title": release_group.get("title", ""),
            "status": "Official",
            "releasedate": format_release_date(release_group.get("first-release-date", "")),
            "country": [],
            "label": [],
            "disambiguation": "",
            "oldids": [],
            "media": [{"Format": "CD", "Name": "", "Position": 1}],
            "tracks": placeholder_tracks,
            "track_count": track_count
        }
        normalized_album["releases"].append(placeholder_release)
        return normalized_album

    # 2. Find and normalize all releases associated with this release group
    # from the individual releases under it.
    all_release_statuses = set()

    for release in releases:
        # Normalize tracks for this release
        normalized_tracks = []
        for medium in release.get("media", []):
            if "tracks" in medium:
                for track in medium["tracks"]:
                    # Handle both original MusicBrainz format and filtered format
                    # Original format: recording: { id: "..." }
                    # Filtered format: recording_id: "..."
                    rec_id = ""
                    if "recording_id" in track:
                        # Filtered format (from our preprocessing)
                        rec_id = track.get("recording_id", "")
                    else:
                        # Original MusicBrainz format
                        recording = track.get("recording")
                        if recording and recording.get("id"):
                            rec_id = recording.get("id")

                    # Handle artist ID extraction for both formats
                    # Original format: artist-credit: [{ artist: { id: "..." } }]
                    # Filtered format: artist_id: "..."
                    track_artist_id = artist.get("id")  # Default to album artist
                    if "artist_id" in track:
                        # Filtered format (from our preprocessing)
                        track_artist_id = track.get("artist_id") or artist.get("id")
                    elif track.get("artist-credit"):
                        # Original MusicBrainz format
                        artist_credit = track.get("artist-credit", [])
                        if artist_credit and artist_credit[0].get("artist", {}).get("id"):
                            track_artist_id = artist_credit[0]["artist"]["id"]

                    # Handle track number - support both "number" and direct position
                    track_number = track.get("number", str(track.get("position", "")))

                    # Lidarr's track schema
                    track_object = {
                        "id": track.get("id"),
                        "trackname": track.get("title"),
                        "tracknumber": str(track_number) if track_number else "",
                        "trackposition": track.get("position"),
                        "durationms": track.get("length"),
                        "artistid": track_artist_id,
                        "recordingid": rec_id,
                        "mediumnumber": medium.get("position"),
                        "oldids": [],
                        "oldrecordingids": [],
                    }
                    normalized_tracks.append(track_object)

        # Assemble media list for this release (Format/Name/Position)
        media_list = []
        for medium in release.get("media", []):
            media_list.append({
                "Format": medium.get("format", "Unknown"),
                "Name": medium.get("title", ""),
                "Position": medium.get("position")
            })

        # Lidarr's release schema
        release_status = release.get("status", "Unknown")
        all_release_statuses.add(release_status)

        # Handle label extraction for both formats
        # Original format: label-info: [{ label: { name: "..." } }]
        # Filtered format: labels: ["Label Name"]
        labels = []
        if "labels" in release:
            # Filtered format (from our preprocessing)
            labels = release.get("labels", [])
        else:
            # Original MusicBrainz format
            labels = [l["label"]["name"] for l in release.get("label-info", []) if l.get("label")]

        # Handle country for both formats
        # Original format: country: "US"
        # Filtered format: country: ["US"]
        country = release.get("country", [])
        if isinstance(country, str):
            country = [country]
        elif not isinstance(country, list):
            country = []

        normalized_album["releases"].append({
            "id": release.get("id"),
            "title": release.get("title", ""),
            "status": release_status,
            "releasedate": format_release_date(release.get("date", "")),
            "country": country,
            "label": labels,
            "media": media_list,
            "track_count": sum(len(m.get("tracks", [])) for m in release.get("media", [])),
            "tracks": normalized_tracks,
        })

    # Update the album summary with all found release statuses
    if normalized_album.get("artists") and normalized_album["artists"][0].get("Albums"):
        normalized_album["artists"][0]["Albums"][0]["ReleaseStatuses"] = sorted(list(all_release_statuses))

    return normalized_album

# Main block for standalone testing
def main():
    """Main function to run the normalization process for testing."""
    # Define paths relative to the script location or a known structure
    # This part is for local testing and won't be used by the data processor
    script_dir = Path(__file__).parent.parent
    input_dir = script_dir / "local/extracted_data"
    output_dir = script_dir / "local/normalized_data"

    # Load data
    raw_data = load_extracted_data(input_dir)
    if not raw_data:
        sys.exit(1)

    # Normalize artist data
    normalized_artist = normalize_radiohead_artist_data(raw_data["artist"], raw_data["release_groups"])
    logger.debug(f"Normalization complete for: {normalized_artist['artistname']}")

    # Save normalized artist data
    output_dir.mkdir(exist_ok=True)
    artist_output_file = output_dir / f"{normalized_artist['id']}_normalized.json"
    with open(artist_output_file, "w", encoding='utf-8') as f:
        json.dump(normalized_artist, f, indent=2, ensure_ascii=False)
    logger.debug(f"Normalized artist data saved to: {artist_output_file}")

    # Normalize and save individual albums
    albums_dir = output_dir / "albums" / normalized_artist["id"]
    albums_dir.mkdir(parents=True, exist_ok=True)

    for rg in raw_data["release_groups"]:
        album_payload = normalize_album_data(rg, raw_data["artist"], raw_data.get("releases", []))
        album_file = albums_dir / f"{album_payload['id']}.json"
        with open(album_file, "w", encoding='utf-8') as f:
            json.dump(album_payload, f, indent=2, ensure_ascii=False)
    logger.debug(f"Individual album files saved in: {albums_dir}")


def load_extracted_data(input_dir: Path) -> Dict[str, Any]:
    """Load extracted raw data from files for testing."""
    logger.info(f"Loading extracted data from {input_dir}")

    # This is a simplified loader for the test case
    artist_file = input_dir / "radiohead_artist.json"
    if not artist_file.exists():
        logger.error(f"Artist file not found: {artist_file}")
        return {}
    with open(artist_file, 'r', encoding='utf-8') as f:
        artist_data = json.load(f)

    release_groups_file = input_dir / "radiohead_release_groups.json"
    if not release_groups_file.exists():
        logger.error(f"Release-groups file not found: {release_groups_file}")
        return {}
    with open(release_groups_file, 'r', encoding='utf-8') as f:
        release_groups = json.load(f)

    return {"artist": artist_data, "release_groups": release_groups, "releases": []} # No releases needed for this test


if __name__ == "__main__":
    main()
