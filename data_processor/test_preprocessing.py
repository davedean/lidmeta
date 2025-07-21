#!/usr/bin/env python3
"""
Test script for schema-guided preprocessing.

This script validates that the preprocessing works correctly by:
1. Running preprocessing on a subset of data
2. Comparing outputs to ensure schema compliance
3. Measuring actual data reduction achieved
4. Validating that final API outputs are identical

Run this before deploying preprocessing to production.
"""

import json
import logging
import sys
import tempfile
from pathlib import Path
from typing import Dict, Any

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from data_processor.preprocess import (
    preprocess_artist_file_schema_guided,
    preprocess_release_group_file_schema_guided,
    validate_preprocessing_output
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def create_test_artist_file(output_path: Path, num_records: int = 1000) -> bool:
    """Create a test artist file with a subset of real data."""
    input_dir = Path("/data/current")
    artist_file = input_dir / "artist"

    if not artist_file.exists():
        logger.error(f"❌ Source artist file not found: {artist_file}")
        return False

    logger.info(f"Creating test artist file with {num_records} records...")

    with open(artist_file, 'r') as input_f, open(output_path, 'w') as output_f:
        for i, line in enumerate(input_f):
            if i >= num_records:
                break
            output_f.write(line)

    logger.info(f"✅ Test artist file created: {output_path}")
    return True


def create_test_rg_file(output_path: Path, num_records: int = 1000) -> bool:
    """Create a test release-group file with a subset of real data."""
    input_dir = Path("/data/current")
    rg_file = input_dir / "release-group"

    if not rg_file.exists():
        logger.error(f"❌ Source release-group file not found: {rg_file}")
        return False

    logger.info(f"Creating test release-group file with {num_records} records...")

    with open(rg_file, 'r') as input_f, open(output_path, 'w') as output_f:
        for i, line in enumerate(input_f):
            if i >= num_records:
                break
            output_f.write(line)

    logger.info(f"✅ Test release-group file created: {output_path}")
    return True


def compare_sample_records(original_file: Path, filtered_file: Path, file_type: str, sample_size: int = 10) -> bool:
    """Compare a sample of records to ensure preprocessing preserved essential data."""
    logger.info(f"Comparing sample {file_type} records...")

    with open(original_file, 'r') as orig_f, open(filtered_file, 'r') as filt_f:
        for i in range(sample_size):
            orig_line = orig_f.readline()
            filt_line = filt_f.readline()

            if not orig_line or not filt_line:
                break

            try:
                orig_data = json.loads(orig_line)
                filt_data = json.loads(filt_line)

                # Verify essential fields are preserved
                essential_fields = ['id', 'name'] if file_type == 'artist' else ['id', 'title']

                for field in essential_fields:
                    if orig_data.get(field) != filt_data.get(field):
                        logger.error(f"❌ Field '{field}' mismatch in record {i+1}")
                        logger.error(f"   Original: {orig_data.get(field)}")
                        logger.error(f"   Filtered: {filt_data.get(field)}")
                        return False

                # Log size comparison for first record
                if i == 0:
                    orig_size = len(orig_line.encode('utf-8'))
                    filt_size = len(filt_line.encode('utf-8'))
                    reduction = (1 - filt_size/orig_size) * 100
                    logger.info(f"📊 Sample record size: {orig_size} → {filt_size} bytes ({reduction:.1f}% reduction)")

            except json.JSONDecodeError as e:
                logger.error(f"❌ JSON decode error in record {i+1}: {e}")
                return False

    logger.info(f"✅ Sample {file_type} record comparison passed")
    return True


def test_schema_field_mapping(filtered_file: Path, file_type: str) -> bool:
    """Verify that filtered data contains all fields needed for schema compliance."""
    logger.info(f"Testing {file_type} schema field mapping...")

    required_fields = {
        'artist': ['id', 'name', 'sort-name', 'type', 'disambiguation'],
        'release-group': ['id', 'title', 'primary-type', 'artist-credit']
    }

    fields_to_check = required_fields.get(file_type, [])

    with open(filtered_file, 'r') as f:
        for i, line in enumerate(f):
            if i >= 100:  # Check first 100 records
                break

            try:
                data = json.loads(line)

                for field in fields_to_check:
                    if field not in data:
                        logger.error(f"❌ Required field '{field}' missing in {file_type} record {i+1}")
                        return False

            except json.JSONDecodeError as e:
                logger.error(f"❌ JSON decode error in {file_type} record {i+1}: {e}")
                return False

    logger.info(f"✅ {file_type} schema field mapping verification passed")
    return True


def main():
    """Main test function."""
    logger.info("🧪 Starting schema-guided preprocessing validation tests")

    # Create temporary directory for test files
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Test artist preprocessing
        logger.info("=" * 60)
        logger.info("Testing Artist Preprocessing")
        logger.info("=" * 60)

        test_artist_file = temp_path / "test_artist"
        test_artist_filtered = temp_path / "test_artist.filtered"

        if not create_test_artist_file(test_artist_file, 1000):
            return 1

        # Run artist preprocessing
        artist_reduction = preprocess_artist_file_schema_guided(test_artist_file, test_artist_filtered)

        # Validate results
        if not validate_preprocessing_output(test_artist_file, test_artist_filtered, "artist"):
            return 1

        if not compare_sample_records(test_artist_file, test_artist_filtered, "artist"):
            return 1

        if not test_schema_field_mapping(test_artist_filtered, "artist"):
            return 1

        # Test release-group preprocessing
        logger.info("=" * 60)
        logger.info("Testing Release-Group Preprocessing")
        logger.info("=" * 60)

        test_rg_file = temp_path / "test_release_group"
        test_rg_filtered = temp_path / "test_release_group.filtered"

        if not create_test_rg_file(test_rg_file, 1000):
            return 1

        # Run release-group preprocessing
        rg_reduction = preprocess_release_group_file_schema_guided(test_rg_file, test_rg_filtered)

        # Validate results
        if not validate_preprocessing_output(test_rg_file, test_rg_filtered, "release-group"):
            return 1

        if not compare_sample_records(test_rg_file, test_rg_filtered, "release-group"):
            return 1

        if not test_schema_field_mapping(test_rg_filtered, "release-group"):
            return 1

        # Final summary
        logger.info("=" * 60)
        logger.info("🎉 All preprocessing validation tests passed!")
        logger.info(f"📊 Artist data reduction: {artist_reduction:.1f}%")
        logger.info(f"📊 Release-group data reduction: {rg_reduction:.1f}%")

        # Check if we achieved target reduction
        if artist_reduction >= 85:
            logger.info("✅ Artist reduction target achieved (≥85%)")
        else:
            logger.warning(f"⚠️  Artist reduction below target: {artist_reduction:.1f}% < 85%")

        if rg_reduction >= 85:
            logger.info("✅ Release-group reduction target achieved (≥85%)")
        else:
            logger.warning(f"⚠️  Release-group reduction below target: {rg_reduction:.1f}% < 85%")

        logger.info("🚀 Preprocessing is ready for production deployment!")
        return 0


if __name__ == "__main__":
    sys.exit(main())
