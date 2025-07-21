#!/usr/bin/env python3
"""
Filter release-group dump to only include Lidarr-compatible release types.

This script reads the release-group dump and creates a filtered version
containing only release-groups that Lidarr would actually use by default.
Based on Lidarr documentation, it defaults to showing only studio albums.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any, List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Lidarr-compatible release types (primary types that Lidarr uses)
# Based on Lidarr docs, it defaults to studio albums but can be configured for others
LIDARR_COMPATIBLE_TYPES = {
    "Album",           # Standard albums (Lidarr's primary focus)
    "EP",              # Extended plays (configurable via Metadata Profiles)
    "Single",          # Singles (configurable via Metadata Profiles)
    "Compilation",     # Compilations (configurable via Metadata Profiles)
    "Soundtrack",      # Soundtracks (configurable via Metadata Profiles)
    "Live",            # Live albums (configurable via Metadata Profiles)
    "Remix",           # Remix albums (configurable via Metadata Profiles)
    "Mixtape",         # Mixtapes (configurable via Metadata Profiles)
    "Demo",            # Demos (configurable via Metadata Profiles)
    "Interview",       # Interviews (configurable via Metadata Profiles)
    "Audiobook"        # Audiobooks (configurable via Metadata Profiles)
}

# Non-Lidarr compatible release types
NON_LIDARR_TYPES = {
    "Other",           # Various non-standard types
    "Broadcast",       # TV/radio broadcasts
    "Spokenword"       # Spoken word recordings
}

def has_release_date(release_group: Dict[str, Any]) -> bool:
    """Check if the release-group has a valid release date."""
    release_date = release_group.get("first-release-date", "")
    return bool(release_date and release_date.strip())

def has_non_standard_secondary_types(release_group: Dict[str, Any]) -> bool:
    """Check if the release-group has non-standard secondary types."""
    secondary_types = release_group.get("secondary-types", [])

    # Non-standard secondary types that indicate non-core releases
    # Based on Lidarr's release status options: Official, Promotion, Bootleg, Pseudo-Release
    non_standard_types = {
        "Bootleg", "Promotional", "DJ-mix", "Split", "Mixtape",
        "Remix", "Compilation", "Live", "Demo", "Interview"
    }

    return any(sec_type in non_standard_types for sec_type in secondary_types)

def is_lidarr_compatible(release_group: Dict[str, Any]) -> bool:
    """
    Check if a release-group is compatible with Lidarr using metadata-based filtering.

    Based on Lidarr documentation:
    - Defaults to showing only studio albums
    - Can be configured to show other types via Metadata Profiles
    - Focuses on "Official" releases rather than Bootleg/Promotion/Pseudo-Release

    Returns True if the release-group is a standard release that Lidarr users would want.
    """
    primary_type = release_group.get("primary-type")
    secondary_types = release_group.get("secondary-types", [])
    title = release_group.get("title", "").lower()

    if not primary_type:
        return False

    # Check if it's in our non-compatible types
    if primary_type in NON_LIDARR_TYPES:
        return False

    # Check if it's in our compatible types
    if primary_type not in LIDARR_COMPATIBLE_TYPES:
        return False

    # For all release types, apply basic metadata filtering
    if has_non_standard_secondary_types(release_group):
        return False

    # Require a release date for all releases
    if not has_release_date(release_group):
        return False

    # For albums (Lidarr's primary focus), be very selective
    if primary_type == "Album":
        # Filter out albums with Live secondary type (live albums)
        if "Live" in secondary_types:
            return False

        # Filter out albums with Compilation secondary type (compilations)
        if "Compilation" in secondary_types:
            return False

        # Filter out albums with Remix secondary type (remix albums)
        if "Remix" in secondary_types:
            return False

        # Filter out obvious tribute/cover albums by title
        title_lower = title.lower()
        if any(tribute_indicator in title_lower for tribute_indicator in [
            "tribute", "plays", "performs", "string quartet", "8-bit", "8 bit",
            "lullaby", "renditions", "re-imagined", "reimagined", "covers",
            "interpretation", "version", "remix", "remixed", "nintendo"
        ]):
            return False

        # Filter out live recordings by title (even if no secondary type)
        if any(live_indicator in title_lower for live_indicator in [
            "live at", "live from", "live in", "live on", "live -", "live:",
            "live recording", "live session", "live performance", "live concert"
        ]):
            return False

    # For singles and EPs, be more permissive but still filter obvious non-standard releases
    if primary_type in ["Single", "EP"]:
        # Filter out releases with Live secondary type (live recordings)
        if "Live" in secondary_types:
            return False

        # Filter out releases with Bootleg secondary type
        if "Bootleg" in secondary_types:
            return False

        # Filter out releases with Promotional secondary type
        if "Promotional" in secondary_types:
            return False

        # Filter out live recordings by title (even if no secondary type)
        title_lower = title.lower()
        if any(live_indicator in title_lower for live_indicator in [
            "live at", "live from", "live in", "live on", "live -", "live:",
            "live recording", "live session", "live performance", "live concert"
        ]):
            return False

    return True

def filter_release_groups(input_file: str, output_file: str) -> None:
    """Filter release-groups from input file to output file."""
    input_path = Path(input_file)
    output_path = Path(output_file)

    if not input_path.exists():
        logger.error(f"Input file does not exist: {input_file}")
        sys.exit(1)

    logger.info(f"Filtering release-groups from {input_file} to {output_file}")
    logger.info(f"Starting to filter release-groups from {input_file}")
    logger.info(f"Input file size: {input_path.stat().st_size / (1024*1024):.2f} MB")

    kept_count = 0
    filtered_count = 0
    error_count = 0

    with open(input_path, 'r', encoding='utf-8') as infile, \
         open(output_path, 'w', encoding='utf-8') as outfile:

        for line_num, line in enumerate(infile, 1):
            line = line.strip()
            if not line:
                continue

            try:
                release_group = json.loads(line)

                if is_lidarr_compatible(release_group):
                    outfile.write(line + '\n')
                    kept_count += 1
                else:
                    filtered_count += 1

            except json.JSONDecodeError as e:
                logger.warning(f"Error parsing JSON on line {line_num}: {e}")
                error_count += 1
                continue

            # Show progress every 10,000 lines
            if line_num % 10000 == 0:
                logger.info(f"Processed {line_num:,} lines, kept {kept_count:,}, filtered {filtered_count:,}")
                sys.stdout.flush()  # Force output to display immediately

    # Log results
    output_size = output_path.stat().st_size / (1024*1024) if output_path.exists() else 0
    input_size = input_path.stat().st_size / (1024*1024)
    size_reduction = ((input_size - output_size) / input_size * 100) if input_size > 0 else 0

    logger.info("=" * 60)
    logger.info("FILTERING RESULTS")
    logger.info("=" * 60)
    logger.info(f"Total processed: {kept_count + filtered_count}")
    logger.info(f"Kept: {kept_count}")
    logger.info(f"Filtered out: {filtered_count}")
    logger.info(f"Errors: {error_count}")
    logger.info(f"Input size: {input_size:.2f} MB")
    logger.info(f"Output size: {output_size:.2f} MB")
    logger.info(f"Size reduction: {size_reduction:.1f}%")
    logger.info("=" * 60)

def main():
    """Main function."""
    if len(sys.argv) != 3:
        logger.info("Usage: python filter_release_groups.py <input_file> <output_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    filter_release_groups(input_file, output_file)

if __name__ == "__main__":
    main()
