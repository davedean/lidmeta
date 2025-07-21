#!/usr/bin/env python3
"""
Filter release-group dump to only include Albums (most restrictive filtering).

This script reads the release-group dump and creates a filtered version
containing only Albums that Lidarr would consider "studio albums" by default.
This is the most restrictive approach, excluding all EPs, Singles, etc.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any, List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def has_release_date(release_group: Dict[str, Any]) -> bool:
    """Check if the release-group has a valid release date."""
    release_date = release_group.get("first-release-date", "")
    return bool(release_date and release_date.strip())

def is_album_only_compatible(release_group: Dict[str, Any]) -> bool:
    """
    Check if a release-group is compatible with Lidarr using ALBUM-ONLY filtering.

    This is the most restrictive approach:
    - Only allows "Album" primary type
    - Filters out all EPs, Singles, Compilations, Live albums, etc.
    - Focuses on what Lidarr considers "studio albums" by default

    Returns True if the release-group is a standard studio album that Lidarr users would want.
    """
    primary_type = release_group.get("primary-type")
    secondary_types = release_group.get("secondary-types", [])
    title = release_group.get("title", "").lower()

    if not primary_type:
        return False

    # ONLY allow Albums - filter out everything else
    if primary_type != "Album":
        return False

    # Filter out albums with non-standard secondary types
    non_standard_types = {
        "Bootleg", "Promotional", "DJ-mix", "Split", "Mixtape",
        "Remix", "Compilation", "Live", "Demo", "Interview"
    }

    if any(sec_type in non_standard_types for sec_type in secondary_types):
        return False

    # Require a release date for all albums
    if not has_release_date(release_group):
        return False

    # Filter out obvious tribute/cover albums by title
    if any(tribute_indicator in title for tribute_indicator in [
        "tribute", "plays", "performs", "string quartet", "8-bit", "8 bit",
        "lullaby", "renditions", "re-imagined", "reimagined", "covers",
        "interpretation", "version", "remix", "remixed", "nintendo"
    ]):
        return False

    # Filter out live recordings by title (even if no secondary type)
    if any(live_indicator in title for live_indicator in [
        "live at", "live from", "live in", "live on", "live -", "live:",
        "live recording", "live session", "live performance", "live concert"
    ]):
        return False

    return True

def filter_albums_only(input_file: str, output_file: str) -> None:
    """Filter release-groups to only include Albums from input file to output file."""
    input_path = Path(input_file)
    output_path = Path(output_file)

    if not input_path.exists():
        logger.error(f"Input file does not exist: {input_file}")
        sys.exit(1)

    logger.info(f"Filtering to ALBUMS ONLY from {input_file} to {output_file}")
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

                if is_album_only_compatible(release_group):
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
    logger.info("ALBUM-ONLY FILTERING RESULTS")
    logger.info("=" * 60)
    logger.info(f"Total processed: {kept_count + filtered_count}")
    logger.info(f"Albums kept: {kept_count}")
    logger.info(f"Filtered out: {filtered_count}")
    logger.info(f"Errors: {error_count}")
    logger.info(f"Input size: {input_size:.2f} MB")
    logger.info(f"Output size: {output_size:.2f} MB")
    logger.info(f"Size reduction: {size_reduction:.1f}%")
    logger.info("=" * 60)

def main():
    """Main function."""
    if len(sys.argv) != 3:
        print("Usage: python filter_albums_only.py <input_file> <output_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    filter_albums_only(input_file, output_file)

if __name__ == "__main__":
    main()
