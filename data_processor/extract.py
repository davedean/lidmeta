#!/usr/bin/env python3
"""
One-time script to extract data from tar.xz archives.

This should be the first step in the data processing pipeline, ensuring that
the uncompressed data files are available for the indexing and processing stages.
"""
import hashlib
import logging
import shutil
import subprocess
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def validate_and_extract_tar_file(tar_path: Path, extract_dir: Path) -> bool:
    """
    Extracts a specific tar.xz file, but only if the final file doesn't
    already exist. This function is idempotent.
    """
    try:
        entity_name = tar_path.stem.replace('.tar', '')
        extracted_file = extract_dir / entity_name

        logger.info(f"Checking for existing file: {extracted_file}")
        if extracted_file.exists() and extracted_file.stat().st_size > 0:
            logger.info(f"✅ {entity_name} already exists ({extracted_file.stat().st_size / (1024*1024):.1f} MB), skipping extraction.")
            return True
        else:
            logger.info(f"File does not exist or is empty, proceeding with extraction...")

        # We are extracting only the specific file we need from the archive.
        # e.g., 'mbdump/artist' from 'artist.tar.xz'
        member_to_extract = f'mbdump/{entity_name}'

        logger.info(f"Extracting '{member_to_extract}' from {tar_path.name}...")

        # Use subprocess to call the `tar` command for efficiency.
        # We extract directly to the final directory to avoid needing to move it.
        # The `-C` flag tells tar to change to the target directory.
        result = subprocess.run(
            ['tar', '-xJf', str(tar_path), '-C', str(extract_dir), member_to_extract],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            logger.error(f"❌ Failed to extract {tar_path.name}. Error: {result.stderr}")
            return False

        # After extraction, the file will be at `extract_dir/mbdump/artist`.
        # We need to move it up to `extract_dir/artist`.
        source_file = extract_dir / "mbdump" / entity_name
        target_file = extract_dir / entity_name

        if source_file.exists():
            shutil.move(str(source_file), str(target_file))
            # Clean up the now-empty mbdump directory
            (extract_dir / "mbdump").rmdir()
            logger.info(f"✅ Successfully extracted and moved {entity_name}.")
            return True
        else:
            logger.error(f"❌ Extracted file '{source_file}' not found after tar command.")
            return False

    except Exception as e:
        logger.error(f"❌ An unexpected error occurred during extraction of {tar_path.name}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def main():
    """Main function to extract all necessary archives."""
    logger.info("🚀 Starting data extraction process...")

    # These paths are relative to the docker-compose environment
    input_dir = Path("/data/current")

    files_to_extract = ["artist.tar.xz", "release-group.tar.xz"]

    for file_name in files_to_extract:
        tar_path = input_dir / file_name
        if not tar_path.exists():
            logger.error(f"❌ Source archive not found: {tar_path}. Cannot proceed.")
            return 1

        if not validate_and_extract_tar_file(tar_path, extract_dir=input_dir):
            logger.error(f"❌ Failed to extract {file_name}. Halting.")
            return 1

    logger.info("🎉 All data extracted successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
