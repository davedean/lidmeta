#!/usr/bin/env python3
"""
Download, verify, and extract MusicBrainz JSON dumps.

Usage:
  python download_mbjson_dump.py --entity artist --output /srv/mbjson/2025-07-16 [--date 20250716-001001]

- Downloads .tar.xz and .asc signature
- Verifies GPG signature
- Extracts NDJSON file to output directory
"""
import argparse
import logging
import os
import sys
import httpx
import subprocess
import tarfile
import lzma
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

MB_BASE_URL = "https://data.metabrainz.org/pub/musicbrainz/data/json-dumps/"
GPG_KEY = "C777580F"


def download_file(url, dest, max_retries=3, max_bytes=None):
    """Download file with retry logic and progress tracking.

    Args:
        url: URL to download
        dest: Destination file path
        max_retries: Number of retry attempts
        max_bytes: If set, only download first N bytes (for dev mode truncation)
    """
    import time
    filename = Path(dest).name

    for attempt in range(max_retries):
        try:
            logger.info(f"Downloading {filename} from {url} (attempt {attempt + 1}/{max_retries})...")

            # Set up headers for range request if truncating
            headers = {}
            if max_bytes:
                headers['Range'] = f'bytes=0-{max_bytes-1}'
                logger.info(f"DEV MODE: Truncating download to first {max_bytes:,} bytes")

            with httpx.stream("GET", url, timeout=30, headers=headers) as r:
                r.raise_for_status()

                # Handle range request response
                if max_bytes and r.status_code == 206:
                    content_range = r.headers.get('content-range', '')
                    logger.info(f"Range request successful: {content_range}")
                    total_size = max_bytes  # Use requested size for progress
                else:
                    total_size = int(r.headers.get('content-length', 0))

                downloaded = 0
                last_progress_time = time.time()
                progress_interval = 30  # Log progress every 30 seconds

                with open(dest, 'wb') as f:
                    for chunk in r.iter_bytes(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)

                            # Log progress every 30 seconds
                            current_time = time.time()
                            if total_size > 0 and (current_time - last_progress_time) >= progress_interval:
                                percent = (downloaded / total_size) * 100
                                # Use logger for progress so it appears in Docker logs
                                logger.info(f"Download progress for {filename}: {percent:.1f}% ({downloaded:,}/{total_size:,} bytes)")
                                last_progress_time = current_time

                logger.info(f"Download completed: {filename} saved to {dest}")
                return

        except Exception as e:
            logger.error(f"\nDownload attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                logger.info("Retrying in 5 seconds...")
                import time
                time.sleep(5)
            else:
                logger.error(f"Failed to download {url} after {max_retries} attempts")
                raise


def verify_signature(tar_path, asc_path):
    """Verify GPG signature (key should already be imported in Docker image)."""
    logger.info(f"[verify_signature] Preparing to verify GPG signature for {tar_path} ...")

    # Try to verify the signature (key should already be imported)
    try:
        logger.info(f"[verify_signature] Starting GPG verification for {tar_path}...")
        result = subprocess.run(
            ["gpg", "--verify", asc_path, tar_path], capture_output=True, text=True
        )
        logger.info(f"[verify_signature] Finished GPG verification for {tar_path}.")

        if result.returncode == 0:
            logger.info("GPG signature verified successfully.")
            return True
        else:
            logger.error(f"GPG verification failed: {result.stderr}")

            # Check if we should continue anyway
            if os.environ.get('MBJSON_SKIP_VERIFICATION', '').lower() in ('1', 'true', 'yes'):
                logger.info("Continuing without signature verification (MBJSON_SKIP_VERIFICATION=1)")
                return True
            else:
                logger.error("To skip verification, set MBJSON_SKIP_VERIFICATION=1")
                return False

    except Exception as e:
        logger.error(f"Error during GPG verification: {e}")
        if os.environ.get('MBJSON_SKIP_VERIFICATION', '').lower() in ('1', 'true', 'yes'):
            logger.info("Continuing without signature verification (MBJSON_SKIP_VERIFICATION=1)")
            return True
        else:
            logger.error("To skip verification, set MBJSON_SKIP_VERIFICATION=1")
            return False


def extract_ndjson_from_tarxz(tarxz_path, output_dir, entity, dev_mode=False, sample_size_mb=100):
    """Extract NDJSON file from tar.xz using system tar for better performance."""
    tarxz_path = Path(tarxz_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # The NDJSON is inside mbdump/{entity}
    member_path = f"mbdump/{entity}"
    logger.info(f"[extract_ndjson_from_tarxz] Preparing to extract {member_path} from {tarxz_path} to {output_dir}...")

    if dev_mode:
        logger.info(f"[extract_ndjson_from_tarxz] DEV MODE: Will extract only ~{sample_size_mb}MB sample")
        return _extract_sample_from_tarxz(tarxz_path, output_dir, entity, sample_size_mb)

    try:
        logger.info(f"[extract_ndjson_from_tarxz] Starting tar extraction for {entity}...")
        # Use system tar for much faster extraction
        result = subprocess.run(
            ["tar", "--extract", "--xz", "--file", str(tarxz_path), "--directory", str(output_dir), member_path]
        )
        logger.info(f"[extract_ndjson_from_tarxz] Finished tar extraction for {entity}.")

        # Verify extraction and move file to expected location
        extracted_file = output_dir / "mbdump" / entity
        expected_file = output_dir / entity

        if extracted_file.exists():
            # Move from mbdump/entity to entity (root level)
            extracted_file.rename(expected_file)
            logger.info(f"✓ Extracted {entity} to {output_dir}")
            return
        else:
            logger.error(f"✗ Extraction completed but file not found: {extracted_file}")
            raise FileNotFoundError(f"Expected file not found after extraction: {extracted_file}")

    except subprocess.CalledProcessError as e:
        logger.error(f"✗ Extraction failed: {e}")
        logger.error(f"stdout: {e.stdout}")
        logger.error(f"stderr: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"✗ Unexpected error during extraction: {e}")
        raise


def _extract_sample_from_tarxz(tarxz_path, output_dir, entity, sample_size_mb=100):
    """Extract a small sample from tar.xz for development/testing."""
    import lzma
    import tarfile

    logger.info(f"[_extract_sample_from_tarxz] Starting sample extraction for {entity}...")

    sample_size_bytes = sample_size_mb * 1024 * 1024
    extracted_size = 0
    record_count = 0

    try:
        with lzma.open(tarxz_path, 'rb') as xz_file:
            with tarfile.open(fileobj=xz_file, mode='r:*') as tar:
                # Try to list members to see if tar is valid
                members = tar.getmembers()
                if not members:
                    raise ValueError("Tar file appears to be empty or corrupted")

                # Find the entity file in the archive
                entity_member = None
                for member in members:
                    if member.name == f"mbdump/{entity}":
                        entity_member = member
                        break

                if not entity_member:
                    raise FileNotFoundError(f"Entity file mbdump/{entity} not found in archive")

                logger.info(f"Found entity file: {entity_member.name} ({entity_member.size:,} bytes)")

                # Extract to memory and write sample
                with tar.extractfile(entity_member) as f:
                    with open(output_dir / entity, 'w', encoding='utf-8') as out_file:
                        for line in f:
                            line_str = line.decode('utf-8')
                            out_file.write(line_str)
                            extracted_size += len(line)
                            record_count += 1

                            # Log progress every 10,000 records (much less frequent)
                            if record_count % 10000 == 0:
                                logger.info(f"  Extraction progress for {entity}: {record_count:,} records, "
                                      f"size: {extracted_size / (1024**2):.1f}MB")

                            # Stop when we reach the target size
                            if extracted_size >= sample_size_bytes:
                                logger.info(f"  Reached target size ({sample_size_mb}MB), stopping extraction")
                                break

        logger.info(f"✓ Extracted sample: {record_count:,} records, "
                   f"{extracted_size / (1024**2):.1f}MB to {output_dir / entity}")

    except (lzma.LZMAError, tarfile.ReadError, OSError) as e:
        logger.error(f"Tar file extraction failed: {e}")
        raise
    except Exception as e:
        logger.error(f"✗ Sample extraction failed: {e}")
        raise


def get_latest_dump_date():
    """Get the latest dump date from MusicBrainz."""
    logger.info("Getting latest dump date from MusicBrainz...")
    try:
        with httpx.Client(timeout=30.0) as client:
            logger.info(f"Making request to {MB_BASE_URL}")
            response = client.get(MB_BASE_URL)
            logger.info(f"Response status: {response.status_code}")
            response.raise_for_status()

            # Look for the latest-is-* file to get the current latest
            import re
            logger.info("Searching for latest dump date...")
            match = re.search(r'latest-is-(\d{8}-\d{6})', response.text)
            if match:
                date = match.group(1)
                logger.info(f"Found latest dump date: {date}")
                return date
            else:
                # Fallback: find the most recent date directory
                logger.info("Using fallback method to find latest date...")
                dates = re.findall(r'href="(\d{8}-\d{6})/"', response.text)
                if dates:
                    date = sorted(dates)[-1]  # Most recent
                    logger.info(f"Found latest dump date (fallback): {date}")
                    return date
            raise RuntimeError("Could not determine latest dump date")
    except Exception as e:
        logger.error(f"Error getting latest dump date: {e}")
        # Don't exit, just return a default date
        logger.info("Using default date: 20250716-001001")
        return "20250716-001001"


def main():
    parser = argparse.ArgumentParser(description="Download and extract MusicBrainz JSON dump.")
    parser.add_argument("--entity", required=True, help="Entity to download (artist, release, etc)")
    parser.add_argument("--output", default="/app/data/mbjson/latest", help="Output directory for extracted NDJSON")
    parser.add_argument("--date", default="latest", help="Dump date/version (e.g. 20250716-001001) or 'latest'")
    parser.add_argument("--skip-verification", action="store_true", help="Skip GPG signature verification")
    parser.add_argument("--dev-mode", action="store_true", help="Extract only a small sample for development")
    parser.add_argument("--sample-size-mb", type=int, default=100, help="Sample size in MB for dev mode (default: 100)")
    args = parser.parse_args()

    entity = args.entity
    output_dir = Path(args.output)
    date = args.date

    # Handle "latest" by getting the actual latest date
    if date == "latest":
        date = get_latest_dump_date()
        logger.info(f"Using latest dump date: {date}")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Compose URLs
    base_url = MB_BASE_URL + f"{date}/"
    tar_name = f"{entity}.tar.xz"
    asc_name = f"{entity}.tar.xz.asc"
    tar_url = base_url + tar_name
    asc_url = base_url + asc_name

    tar_path = output_dir / tar_name
    asc_path = output_dir / asc_name

    # Download files (always download full files to avoid corruption)
    logger.info(f"Downloading full tar file: {tar_name}")
    download_file(tar_url, tar_path)

    # Always download full signature file (it's small)
    download_file(asc_url, asc_path)

    # Verify signature (unless skipped)
    if args.skip_verification:
        logger.info("Skipping GPG signature verification (--skip-verification)")
    else:
        if not verify_signature(str(tar_path), str(asc_path)):
            sys.exit(1)

    # Extract NDJSON
    extract_ndjson_from_tarxz(str(tar_path), output_dir, entity,
                             dev_mode=args.dev_mode, sample_size_mb=args.sample_size_mb)

    logger.info("Done.")

if __name__ == "__main__":
    main()
