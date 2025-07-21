#!/usr/bin/env python3
"""
Detailed field analysis to compare normalized data with current mapper output.
"""

import json
import sys
import logging
from pathlib import Path
from typing import Dict, Any, List, Set

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_field_structure(data: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """Analyze the complete field structure of data."""

    structure = {
        "fields": {},
        "field_types": {},
        "field_counts": {},
        "sample_values": {}
    }

    def _analyze_recursive(obj, current_prefix):
        if isinstance(obj, dict):
            for key, value in obj.items():
                field_path = f"{current_prefix}.{key}" if current_prefix else key

                # Record field
                structure["fields"][field_path] = True

                # Record type
                value_type = type(value).__name__
                if field_path not in structure["field_types"]:
                    structure["field_types"][field_path] = value_type

                # Record sample value
                if field_path not in structure["sample_values"]:
                    if isinstance(value, (str, int, float, bool)):
                        structure["sample_values"][field_path] = str(value)[:100]
                    elif isinstance(value, list):
                        structure["sample_values"][field_path] = f"List[{len(value)} items]"
                    elif isinstance(value, dict):
                        structure["sample_values"][field_path] = f"Dict[{len(value)} keys]"
                    else:
                        structure["sample_values"][field_path] = str(value)[:100]

                # Recursively analyze nested structures
                _analyze_recursive(value, field_path)

        elif isinstance(obj, list):
            for i, item in enumerate(obj[:3]):  # Only analyze first 3 items
                _analyze_recursive(item, f"{current_prefix}[{i}]")

    _analyze_recursive(data, prefix)
    return structure

def compare_field_structures(normalized_structure: Dict[str, Any], current_structure: Dict[str, Any]) -> Dict[str, Any]:
    """Compare field structures between normalized and current data."""

    normalized_fields = set(normalized_structure["fields"].keys())
    current_fields = set(current_structure["fields"].keys())

    missing_fields = current_fields - normalized_fields
    extra_fields = normalized_fields - current_fields
    common_fields = normalized_fields & current_fields

    # Analyze type differences for common fields
    type_differences = {}
    for field in common_fields:
        normalized_type = normalized_structure["field_types"].get(field)
        current_type = current_structure["field_types"].get(field)
        if normalized_type != current_type:
            type_differences[field] = {
                "normalized": normalized_type,
                "current": current_type
            }

    return {
        "missing_fields": sorted(list(missing_fields)),
        "extra_fields": sorted(list(extra_fields)),
        "common_fields": sorted(list(common_fields)),
        "type_differences": type_differences,
        "coverage_percentage": len(common_fields) / len(current_fields) * 100 if current_fields else 0
    }

def analyze_critical_fields(missing_fields: List[str]) -> Dict[str, Any]:
    """Analyze which missing fields might be critical for Lidarr."""

    # Fields that are definitely critical for Lidarr
    critical_fields = {
        "id", "artistName", "title", "type", "releasedate", "status",
        "genres", "images", "links", "overview", "rating"
    }

    # Fields that are likely important
    important_fields = {
        "sortName", "disambiguation", "aliases", "oldIds", "secondaryTypes",
        "releaseStatuses", "country", "label", "media", "tracks"
    }

    # Fields that are probably not critical
    non_critical_fields = {
        "artistAliases", "oldids", "releases", "annotation", "relations",
        "tags", "life-span", "aliases", "label-info", "media", "tracks"
    }

    critical_missing = [f for f in missing_fields if any(cf in f for cf in critical_fields)]
    important_missing = [f for f in missing_fields if any(imf in f for imf in important_fields)]
    non_critical_missing = [f for f in missing_fields if any(ncf in f for ncf in non_critical_fields)]
    unknown_missing = [f for f in missing_fields if f not in critical_missing + important_missing + non_critical_missing]

    return {
        "critical_missing": critical_missing,
        "important_missing": important_missing,
        "non_critical_missing": non_critical_missing,
        "unknown_missing": unknown_missing,
        "risk_assessment": {
            "high_risk": len(critical_missing) > 0,
            "medium_risk": len(important_missing) > 0,
            "low_risk": len(critical_missing) == 0 and len(important_missing) == 0
        }
    }

def main():
    """Main analysis function."""
    logger.info("Performing detailed field analysis...")

    # Load normalized data
    normalized_file = Path("local/normalized_data/radiohead_normalized_artist.json")
    if not normalized_file.exists():
        logger.error(f"Normalized data file not found: {normalized_file}")
        sys.exit(1)

    with open(normalized_file, 'r', encoding='utf-8') as f:
        normalized_data = json.load(f)

    # Load current mapper output
    current_file = Path("tests/fixtures/skyhook/artist_a74b1b7f-71a5-4011-9441-d0b5e4122711.json")
    if not current_file.exists():
        logger.error(f"Current mapper output not found: {current_file}")
        sys.exit(1)

    with open(current_file, 'r', encoding='utf-8') as f:
        current_data = json.load(f)

    # Analyze field structures
    logger.info("Analyzing field structures...")
    normalized_structure = analyze_field_structure(normalized_data, "")
    current_structure = analyze_field_structure(current_data, "")

    # Compare structures
    comparison = compare_field_structures(normalized_structure, current_structure)

    # Analyze critical fields
    critical_analysis = analyze_critical_fields(comparison["missing_fields"])

    # Report results
    logger.info(f"\n=== FIELD ANALYSIS RESULTS ===")
    logger.info(f"Field Coverage: {comparison['coverage_percentage']:.1f}%")
    logger.info(f"Missing Fields: {len(comparison['missing_fields'])}")
    logger.info(f"Extra Fields: {len(comparison['extra_fields'])}")
    logger.info(f"Common Fields: {len(comparison['common_fields'])}")

    logger.info(f"\n=== RISK ASSESSMENT ===")
    logger.info(f"High Risk (Critical fields missing): {critical_analysis['risk_assessment']['high_risk']}")
    logger.info(f"Medium Risk (Important fields missing): {critical_analysis['risk_assessment']['medium_risk']}")
    logger.info(f"Low Risk (Only non-critical fields missing): {critical_analysis['risk_assessment']['low_risk']}")

    if comparison["missing_fields"]:
        logger.info(f"\n=== MISSING FIELDS ===")
        if critical_analysis["critical_missing"]:
            logger.info(f"CRITICAL MISSING ({len(critical_analysis['critical_missing'])}):")
            for field in critical_analysis["critical_missing"]:
                logger.info(f"  ❌ {field}")

        if critical_analysis["important_missing"]:
            logger.info(f"IMPORTANT MISSING ({len(critical_analysis['important_missing'])}):")
            for field in critical_analysis["important_missing"]:
                logger.info(f"  ⚠️  {field}")

        if critical_analysis["non_critical_missing"]:
            logger.info(f"NON-CRITICAL MISSING ({len(critical_analysis['non_critical_missing'])}):")
            for field in critical_analysis["non_critical_missing"]:
                logger.info(f"  ℹ️  {field}")

        if critical_analysis["unknown_missing"]:
            logger.info(f"UNKNOWN IMPORTANCE ({len(critical_analysis['unknown_missing'])}):")
            for field in critical_analysis["unknown_missing"]:
                logger.info(f"  ❓ {field}")

    if comparison["extra_fields"]:
        logger.info(f"\n=== EXTRA FIELDS ===")
        for field in comparison["extra_fields"]:
            logger.info(f"  ➕ {field}")

    if comparison["type_differences"]:
        logger.info(f"\n=== TYPE DIFFERENCES ===")
        for field, types in comparison["type_differences"].items():
            logger.info(f"  🔄 {field}: {types['current']} vs {types['normalized']}")

    # Save detailed analysis
    analysis_results = {
        "comparison": comparison,
        "critical_analysis": critical_analysis,
        "normalized_structure": normalized_structure,
        "current_structure": current_structure
    }

    output_dir = Path("local/normalized_data")
    with open(output_dir / "detailed_field_analysis.json", "w", encoding='utf-8') as f:
        json.dump(analysis_results, f, indent=2, ensure_ascii=False)

    # Final assessment
    logger.info(f"\n=== FINAL ASSESSMENT ===")
    if critical_analysis["risk_assessment"]["high_risk"]:
        logger.error("❌ HIGH RISK: Critical fields are missing. Lidarr may not work properly.")
        return False
    elif critical_analysis["risk_assessment"]["medium_risk"]:
        logger.warning("⚠️  MEDIUM RISK: Some important fields are missing. Lidarr may have limited functionality.")
        return True
    else:
        logger.info("✅ LOW RISK: Only non-critical fields are missing. Lidarr should work fine.")
        return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
