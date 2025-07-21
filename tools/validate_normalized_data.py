#!/usr/bin/env python3
"""
Validate normalized data against current mapper output.
"""

import json
import sys
import logging
from pathlib import Path
from typing import Dict, Any, List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def compare_artist_data(normalized: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, Any]:
    """Compare normalized artist data with current mapper output."""

    differences = {
        "missing_fields": [],
        "extra_fields": [],
        "value_differences": [],
        "album_count_difference": 0,
        "field_comparisons": {}
    }

    # Check for missing fields in normalized
    for field in current.keys():
        if field not in normalized:
            differences["missing_fields"].append(field)

    # Check for extra fields in normalized
    for field in normalized.keys():
        if field not in current:
            differences["extra_fields"].append(field)

    # Check album count
    normalized_albums = len(normalized.get("albums", []))
    current_albums = len(current.get("albums", []))
    differences["album_count_difference"] = normalized_albums - current_albums

    # Check key field values
    key_fields = ["id", "artistName", "type", "status", "disambiguation"]
    for field in key_fields:
        if field in normalized and field in current:
            normalized_value = normalized[field]
            current_value = current[field]

            # Store comparison for detailed analysis
            differences["field_comparisons"][field] = {
                "normalized": normalized_value,
                "current": current_value,
                "match": normalized_value == current_value
            }

            if normalized_value != current_value:
                differences["value_differences"].append({
                    "field": field,
                    "normalized": normalized_value,
                    "current": current_value
                })

    # Compare genres (order might differ)
    if "genres" in normalized and "genres" in current:
        normalized_genres = set(normalized["genres"])
        current_genres = set(current["genres"])

        differences["field_comparisons"]["genres"] = {
            "normalized": list(normalized_genres),
            "current": list(current_genres),
            "match": normalized_genres == current_genres,
            "normalized_only": list(normalized_genres - current_genres),
            "current_only": list(current_genres - normalized_genres)
        }

        if normalized_genres != current_genres:
            differences["value_differences"].append({
                "field": "genres",
                "normalized": list(normalized_genres),
                "current": list(current_genres),
                "normalized_only": list(normalized_genres - current_genres),
                "current_only": list(current_genres - normalized_genres)
            })

    return differences

def compare_album_data(normalized_albums: List[Dict[str, Any]], current_albums: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compare normalized album data with current mapper output."""

    differences = {
        "album_count_difference": len(normalized_albums) - len(current_albums),
        "album_comparisons": [],
        "missing_albums": [],
        "extra_albums": []
    }

    # Create lookup dictionaries
    normalized_albums_by_id = {album["id"]: album for album in normalized_albums}
    current_albums_by_id = {album["id"]: album for album in current_albums}

    # Find missing and extra albums
    normalized_ids = set(normalized_albums_by_id.keys())
    current_ids = set(current_albums_by_id.keys())

    missing_ids = current_ids - normalized_ids
    extra_ids = normalized_ids - current_ids

    for album_id in missing_ids:
        differences["missing_albums"].append({
            "id": album_id,
            "title": current_albums_by_id[album_id].get("title", "Unknown")
        })

    for album_id in extra_ids:
        differences["extra_albums"].append({
            "id": album_id,
            "title": normalized_albums_by_id[album_id].get("title", "Unknown")
        })

    # Compare common albums
    common_ids = normalized_ids & current_ids
    for album_id in common_ids:
        normalized_album = normalized_albums_by_id[album_id]
        current_album = current_albums_by_id[album_id]

        album_comparison = {
            "id": album_id,
            "title": normalized_album.get("title", "Unknown"),
            "field_differences": []
        }

        # Compare key fields
        key_fields = ["id", "title", "type", "secondaryTypes", "releaseDate"]
        for field in key_fields:
            if field in normalized_album and field in current_album:
                if normalized_album[field] != current_album[field]:
                    album_comparison["field_differences"].append({
                        "field": field,
                        "normalized": normalized_album[field],
                        "current": current_album[field]
                    })

        differences["album_comparisons"].append(album_comparison)

    return differences

def analyze_field_usage(data: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """Analyze field usage in the data structure."""

    usage = {
        "total_fields": 0,
        "non_empty_fields": 0,
        "field_types": {},
        "field_values": {}
    }

    for key, value in data.items():
        field_name = f"{prefix}.{key}" if prefix else key
        usage["total_fields"] += 1

        if value is not None and value != "" and value != [] and value != {}:
            usage["non_empty_fields"] += 1

        # Analyze field type
        field_type = type(value).__name__
        usage["field_types"][field_type] = usage["field_types"].get(field_type, 0) + 1

        # Store sample values for analysis
        if field_type not in usage["field_values"]:
            usage["field_values"][field_type] = []

        if len(usage["field_values"][field_type]) < 3:  # Keep only first 3 examples
            usage["field_values"][field_type].append({
                "field": field_name,
                "value": str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
            })

    return usage

def main():
    """Main validation function."""
    logger.info("Validating normalized data against current mapper output...")

    # Load normalized data
    normalized_file = Path("local/normalized_data/radiohead_normalized_artist.json")
    if not normalized_file.exists():
        logger.error(f"Normalized data file not found: {normalized_file}")
        sys.exit(1)

    with open(normalized_file, 'r', encoding='utf-8') as f:
        normalized_data = json.load(f)

    # Load current mapper output (from fixtures)
    current_file = Path("tests/fixtures/skyhook/artist_a74b1b7f-71a5-4011-9441-d0b5e4122711.json")
    if not current_file.exists():
        logger.warning(f"Current mapper output not found: {current_file}")
        logger.info("Skipping comparison with current mapper output")
        current_data = None
    else:
        with open(current_file, 'r', encoding='utf-8') as f:
            current_data = json.load(f)

    # Analyze field usage in normalized data
    logger.info("Analyzing field usage in normalized data...")
    artist_usage = analyze_field_usage(normalized_data, "artist")

    # Analyze album field usage
    album_usage = {"total_albums": len(normalized_data.get("albums", []))}
    if normalized_data.get("albums"):
        sample_album = normalized_data["albums"][0]
        album_usage.update(analyze_field_usage(sample_album, "album"))

    # Compare with current mapper output if available
    if current_data:
        logger.info("Comparing with current mapper output...")
        artist_differences = compare_artist_data(normalized_data, current_data)
        album_differences = compare_album_data(
            normalized_data.get("albums", []),
            current_data.get("albums", [])
        )

        # Report results
        logger.info(f"\nValidation Results:")
        logger.info(f"  Missing fields: {len(artist_differences['missing_fields'])}")
        logger.info(f"  Extra fields: {len(artist_differences['extra_fields'])}")
        logger.info(f"  Value differences: {len(artist_differences['value_differences'])}")
        logger.info(f"  Album count difference: {artist_differences['album_count_difference']}")

        if artist_differences["missing_fields"]:
            logger.info(f"  Missing fields: {artist_differences['missing_fields']}")

        if artist_differences["extra_fields"]:
            logger.info(f"  Extra fields: {artist_differences['extra_fields']}")

        if artist_differences["value_differences"]:
            logger.info(f"  Value differences:")
            for diff in artist_differences["value_differences"]:
                logger.info(f"    {diff['field']}: {diff['current']} vs {diff['normalized']}")

        # Overall assessment
        total_issues = (len(artist_differences["missing_fields"]) +
                       len(artist_differences["value_differences"]) +
                       abs(artist_differences["album_count_difference"]) +
                       len(album_differences["missing_albums"]) +
                       len(album_differences["extra_albums"]))

        if total_issues == 0:
            logger.info("✅ Validation PASSED - No differences found")
        else:
            logger.info(f"⚠️  Validation found {total_issues} issues")

        # Save detailed comparison
        comparison_results = {
            "artist_differences": artist_differences,
            "album_differences": album_differences,
            "total_issues": total_issues
        }

        output_dir = Path("local/normalized_data")
        with open(output_dir / "validation_results.json", "w", encoding='utf-8') as f:
            json.dump(comparison_results, f, indent=2, ensure_ascii=False)

    # Report field usage analysis
    logger.info(f"\nField Usage Analysis:")
    logger.info(f"  Artist fields: {artist_usage['total_fields']} total, {artist_usage['non_empty_fields']} non-empty")
    logger.info(f"  Album fields: {album_usage.get('total_fields', 0)} total, {album_usage.get('non_empty_fields', 0)} non-empty")
    logger.info(f"  Albums processed: {album_usage['total_albums']}")

    # Save field usage analysis
    usage_analysis = {
        "artist_usage": artist_usage,
        "album_usage": album_usage
    }

    output_dir = Path("local/normalized_data")
    with open(output_dir / "field_usage_analysis.json", "w", encoding='utf-8') as f:
        json.dump(usage_analysis, f, indent=2, ensure_ascii=False)

    logger.info("Validation complete!")

if __name__ == "__main__":
    main()
