#!/usr/bin/env python3
"""
Compare normalized data with actual current mapper output.
"""

import json
import sys
import logging
from pathlib import Path
from typing import Dict, Any, List, Set

# Add the project root to the path so we can import the mapper
sys.path.insert(0, str(Path(__file__).parent.parent))

from lidarr_metadata_server.providers.mapper import map_musicbrainz_artist, map_musicbrainz_album

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_json_file(file_path: str) -> Dict[str, Any]:
    """Load JSON file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_field_structure(data: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """Get the complete field structure of data."""
    structure = {
        "fields": set(),
        "field_types": {},
        "sample_values": {}
    }

    def _analyze_recursive(obj, current_prefix):
        if isinstance(obj, dict):
            for key, value in obj.items():
                field_path = f"{current_prefix}.{key}" if current_prefix else key
                structure["fields"].add(field_path)
                structure["field_types"][field_path] = type(value).__name__
                structure["sample_values"][field_path] = value
                _analyze_recursive(value, field_path)
        elif isinstance(obj, list):
            for i, item in enumerate(obj[:3]):  # Only analyze first 3 items
                field_path = f"{current_prefix}[{i}]"
                _analyze_recursive(item, field_path)

    _analyze_recursive(data, prefix)
    return structure

def compare_structures(normalized_structure: Dict[str, Any], mapper_structure: Dict[str, Any]) -> Dict[str, Any]:
    """Compare two field structures."""
    normalized_fields = normalized_structure["fields"]
    mapper_fields = mapper_structure["fields"]

    missing_in_normalized = mapper_fields - normalized_fields
    extra_in_normalized = normalized_fields - mapper_fields
    common_fields = normalized_fields & mapper_fields

    # Check type differences for common fields
    type_differences = {}
    for field in common_fields:
        norm_type = normalized_structure["field_types"].get(field)
        mapper_type = mapper_structure["field_types"].get(field)
        if norm_type != mapper_type:
            type_differences[field] = {
                "normalized": norm_type,
                "mapper": mapper_type,
                "normalized_value": normalized_structure["sample_values"].get(field),
                "mapper_value": mapper_structure["sample_values"].get(field)
            }

    return {
        "missing_in_normalized": sorted(missing_in_normalized),
        "extra_in_normalized": sorted(extra_in_normalized),
        "common_fields": len(common_fields),
        "total_mapper_fields": len(mapper_fields),
        "total_normalized_fields": len(normalized_fields),
        "coverage_percentage": len(common_fields) / len(mapper_fields) * 100 if mapper_fields else 0,
        "type_differences": type_differences
    }

def main():
    """Main comparison function."""
    logger.info("Loading normalized data...")

        # Load our normalized data
    normalized_artist_path = "local/normalized_data/radiohead_normalized_artist.json"

    if not Path(normalized_artist_path).exists():
        logger.error(f"Normalized artist file not found: {normalized_artist_path}")
        return

    normalized_artist = load_json_file(normalized_artist_path)

    # Load a sample album from the albums directory
    albums_dir = Path("local/normalized_data/albums")
    if not albums_dir.exists():
        logger.error(f"Albums directory not found: {albums_dir}")
        return

    album_files = list(albums_dir.glob("*.json"))
    if not album_files:
        logger.error("No album files found")
        return

    normalized_album = load_json_file(str(album_files[0]))

    # Load raw dump data to feed to current mapper
    logger.info("Loading raw dump data...")
    raw_data_path = "local/extracted_data/radiohead_artist.json"
    if not Path(raw_data_path).exists():
        logger.error(f"Raw data file not found: {raw_data_path}")
        return

    raw_data = load_json_file(raw_data_path)

        # Generate current mapper output
    logger.info("Generating current mapper output...")
    mapper_artist = map_musicbrainz_artist(raw_data)

        # For album, we need to load release groups and find one with releases
    release_groups_path = "local/extracted_data/radiohead_release_groups.json"
    if not Path(release_groups_path).exists():
        logger.error(f"Release groups file not found: {release_groups_path}")
        return

    release_groups = load_json_file(release_groups_path)

    # Since we don't have releases, we'll use the dump mapper function
    # Find a sample release group
    sample_rg = release_groups[0] if release_groups else None

    if not sample_rg:
        logger.error("No release groups found")
        return

    # Use the dump mapper since we don't have detailed release data
    from lidarr_metadata_server.providers.mapper import map_musicbrainz_album_dump
    mapper_album = map_musicbrainz_album_dump(sample_rg)

    # Analyze structures
    logger.info("Analyzing field structures...")
    norm_artist_structure = get_field_structure(normalized_artist)
    mapper_artist_structure = get_field_structure(mapper_artist)

    norm_album_structure = get_field_structure(normalized_album)
    mapper_album_structure = get_field_structure(mapper_album)

    # Compare structures
    logger.info("Comparing structures...")
    artist_comparison = compare_structures(norm_artist_structure, mapper_artist_structure)
    album_comparison = compare_structures(norm_album_structure, mapper_album_structure)

    # Print results
    print("\n" + "="*80)
    print("COMPARISON WITH CURRENT MAPPER")
    print("="*80)

    print("\n📊 ARTIST DATA COMPARISON")
    print("-" * 40)
    print(f"Coverage: {artist_comparison['coverage_percentage']:.1f}%")
    print(f"Common fields: {artist_comparison['common_fields']}")
    print(f"Mapper fields: {artist_comparison['total_mapper_fields']}")
    print(f"Normalized fields: {artist_comparison['total_normalized_fields']}")

    if artist_comparison['missing_in_normalized']:
        print(f"\n❌ MISSING in normalized ({len(artist_comparison['missing_in_normalized'])}):")
        for field in artist_comparison['missing_in_normalized'][:10]:  # Show first 10
            print(f"  - {field}")
        if len(artist_comparison['missing_in_normalized']) > 10:
            print(f"  ... and {len(artist_comparison['missing_in_normalized']) - 10} more")

    if artist_comparison['extra_in_normalized']:
        print(f"\n✅ EXTRA in normalized ({len(artist_comparison['extra_in_normalized'])}):")
        for field in artist_comparison['extra_in_normalized'][:10]:  # Show first 10
            print(f"  - {field}")
        if len(artist_comparison['extra_in_normalized']) > 10:
            print(f"  ... and {len(artist_comparison['extra_in_normalized']) - 10} more")

    if artist_comparison['type_differences']:
        print(f"\n⚠️  TYPE DIFFERENCES ({len(artist_comparison['type_differences'])}):")
        for field, diff in list(artist_comparison['type_differences'].items())[:5]:
            print(f"  - {field}: {diff['normalized']} vs {diff['mapper']}")

    print("\n📊 ALBUM DATA COMPARISON")
    print("-" * 40)
    print(f"Coverage: {album_comparison['coverage_percentage']:.1f}%")
    print(f"Common fields: {album_comparison['common_fields']}")
    print(f"Mapper fields: {album_comparison['total_mapper_fields']}")
    print(f"Normalized fields: {album_comparison['total_normalized_fields']}")

    if album_comparison['missing_in_normalized']:
        print(f"\n❌ MISSING in normalized ({len(album_comparison['missing_in_normalized'])}):")
        for field in album_comparison['missing_in_normalized'][:10]:  # Show first 10
            print(f"  - {field}")
        if len(album_comparison['missing_in_normalized']) > 10:
            print(f"  ... and {len(album_comparison['missing_in_normalized']) - 10} more")

    if album_comparison['extra_in_normalized']:
        print(f"\n✅ EXTRA in normalized ({len(album_comparison['extra_in_normalized'])}):")
        for field in album_comparison['extra_in_normalized'][:10]:  # Show first 10
            print(f"  - {field}")
        if len(album_comparison['extra_in_normalized']) > 10:
            print(f"  ... and {len(album_comparison['extra_in_normalized']) - 10} more")

    if album_comparison['type_differences']:
        print(f"\n⚠️  TYPE DIFFERENCES ({len(album_comparison['type_differences'])}):")
        for field, diff in list(album_comparison['type_differences'].items())[:5]:
            print(f"  - {field}: {diff['normalized']} vs {diff['mapper']}")

    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)

    artist_coverage = artist_comparison['coverage_percentage']
    album_coverage = album_comparison['coverage_percentage']

    if artist_coverage >= 95 and album_coverage >= 95:
        print("✅ EXCELLENT compatibility - 95%+ field coverage")
    elif artist_coverage >= 90 and album_coverage >= 90:
        print("✅ GOOD compatibility - 90%+ field coverage")
    elif artist_coverage >= 80 and album_coverage >= 80:
        print("⚠️  ACCEPTABLE compatibility - 80%+ field coverage")
    else:
        print("❌ POOR compatibility - <80% field coverage")

    print(f"Artist coverage: {artist_coverage:.1f}%")
    print(f"Album coverage: {album_coverage:.1f}%")

    critical_missing = []
    for field in artist_comparison['missing_in_normalized'] + album_comparison['missing_in_normalized']:
        if any(critical in field.lower() for critical in ['id', 'title', 'name', 'type', 'status']):
            critical_missing.append(field)

    if critical_missing:
        print(f"\n⚠️  CRITICAL fields missing: {len(critical_missing)}")
        for field in critical_missing[:5]:
            print(f"  - {field}")
    else:
        print("\n✅ No critical fields missing")

if __name__ == "__main__":
    main()
