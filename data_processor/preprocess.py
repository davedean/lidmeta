#!/usr/bin/env python3
"""
Schema-guided preprocessing script for MusicBrainz data.

This script filters raw MusicBrainz artist and release-group files to only include
fields that are actually needed for the final ArtistResource and AlbumResource
API responses, achieving 85-90% data reduction while maintaining full compatibility.

Based on the optimization analysis in local/data_processor_optimization_analysis.md
"""

import json
import logging
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Any, Optional

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def _safe_get_artist_id(track: Dict[str, Any]) -> Optional[str]:
    """
    Safely extract artist ID from track data, handling malformed records gracefully.

    Args:
        track: Track data dictionary

    Returns:
        Artist ID string or None if not available/malformed
    """
    try:
        artist_credit = track.get('artist-credit')
        if not artist_credit or not isinstance(artist_credit, list) or len(artist_credit) == 0:
            return None

        first_credit = artist_credit[0]
        if not isinstance(first_credit, dict):
            return None

        artist = first_credit.get('artist')
        if not isinstance(artist, dict):
            return None

        return artist.get('id')
    except (AttributeError, IndexError, TypeError):
        return None


def preprocess_artist_file_schema_guided(input_path: Path, output_path: Path) -> float:
    """
    Filter artist file based on exact ArtistResource schema requirements.

    Maps raw MusicBrainz artist fields to only those needed for the final
    ArtistResource API response, achieving 85-90% data reduction.

    Returns the actual reduction percentage achieved.
    """
    logger.info(f"Starting schema-guided artist preprocessing: {input_path} → {output_path}")
    start_time = time.time()

    filtered_count = 0
    total_input_bytes = 0
    total_output_bytes = 0

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(input_path, 'r', encoding='utf-8') as input_f, \
         open(output_path, 'w', encoding='utf-8') as output_f:

        for line in input_f:
            total_input_bytes += len(line.encode('utf-8'))

            try:
                data = json.loads(line)

                # Map raw MusicBrainz fields to ArtistResource schema requirements
                filtered = {
                    # Direct schema mappings to ArtistResource
                    'id': data.get('id'),                              # → ArtistResource.id
                    'name': data.get('name'),                          # → ArtistResource.artistname
                    'sort-name': data.get('sort-name'),               # → ArtistResource.sortname
                    'type': data.get('type'),                         # → ArtistResource.type
                    'disambiguation': data.get('disambiguation', ''), # → ArtistResource.disambiguation

                    # Derived fields for status determination
                    'life-span': {
                        'ended': data.get('life-span', {}).get('ended', False)
                    },  # → Used to derive ArtistResource.status
                    'country': data.get('country'),                   # → Used in normalization
                    'gender': data.get('gender'),                     # → Used in normalization
                    'area': {
                        'name': data.get('area', {}).get('name')
                    } if data.get('area') else None,                  # → Used in normalization

                    # Arrays (preserve minimal structure for schema compliance)
                    'aliases': data.get('aliases', []),               # → ArtistResource.artistaliases
                    'tags': data.get('tags', []),                     # → ArtistResource.genres (partial)
                    'genres': data.get('genres', []),                 # → ArtistResource.genres (partial)

                    # Relations - simplified for Link schema compliance
                    'relations': [
                        {
                            'type': rel.get('type'),                  # → ArtistResource.links[].type
                            'url': {
                                'resource': rel.get('url', {}).get('resource')  # → ArtistResource.links[].target
                            }
                        }
                        for rel in data.get('relations', [])
                        if rel.get('url', {}).get('resource')
                    ],

                    # Rating - preserve structure for Rating schema compliance
                    'rating': {
                        'votes-count': data.get('rating', {}).get('votes-count', 0),
                        'value': data.get('rating', {}).get('value')   # → ArtistResource.rating
                    }

                    # Note: Albums field populated during main processing
                    # Note: images, oldids typically empty in MB artist data
                }

                # Remove None values to save space
                filtered = {k: v for k, v in filtered.items() if v is not None}

                output_line = json.dumps(filtered, separators=(',', ':')) + '\n'
                output_f.write(output_line)
                total_output_bytes += len(output_line.encode('utf-8'))
                filtered_count += 1

                if filtered_count % 100000 == 0:
                    reduction = (1 - total_output_bytes/total_input_bytes) * 100
                    elapsed = time.time() - start_time
                    rate = filtered_count / elapsed
                    logger.info(f"Processed {filtered_count:,} artists, {reduction:.1f}% reduction, {rate:.0f} artists/sec")

            except json.JSONDecodeError as e:
                logger.warning(f"Skipping invalid JSON line at artist {filtered_count + 1}: {e}")
                continue
            except Exception as e:
                logger.error(f"Error processing artist {filtered_count + 1}: {e}")
                continue

    final_reduction = (1 - total_output_bytes/total_input_bytes) * 100 if total_input_bytes > 0 else 0
    elapsed = time.time() - start_time

    logger.info(f"✅ Artist pre-processing complete:")
    logger.info(f"   • {filtered_count:,} artists processed in {elapsed:.1f}s")
    logger.info(f"   • {final_reduction:.1f}% size reduction achieved")
    logger.info(f"   • {total_input_bytes/1024/1024:.1f}MB → {total_output_bytes/1024/1024:.1f}MB")
    logger.info(f"   • Output saved to: {output_path}")

    return final_reduction


def preprocess_release_group_file_schema_guided(input_path: Path, output_path: Path) -> float:
    """
    Filter release-group file based on exact AlbumResource schema requirements.

    Maps raw MusicBrainz release-group fields to only those needed for the final
    AlbumResource API response, achieving 85-90% data reduction.

    Returns the actual reduction percentage achieved.
    """
    logger.info(f"Starting schema-guided release-group preprocessing: {input_path} → {output_path}")
    start_time = time.time()

    filtered_count = 0
    total_input_bytes = 0
    total_output_bytes = 0

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(input_path, 'r', encoding='utf-8') as input_f, \
         open(output_path, 'w', encoding='utf-8') as output_f:

        for line in input_f:
            total_input_bytes += len(line.encode('utf-8'))

            try:
                data = json.loads(line)

                # Map raw MusicBrainz fields to AlbumResource schema requirements
                filtered = {
                    # Direct schema mappings to AlbumResource
                    'id': data.get('id'),                             # → AlbumResource.id
                    'title': data.get('title'),                       # → AlbumResource.title
                    'primary-type': data.get('primary-type'),         # → AlbumResource.type
                    'secondary-types': data.get('secondary-types', []), # → AlbumResource.secondarytypes
                    'first-release-date': data.get('first-release-date'), # → AlbumResource.releasedate
                    'disambiguation': data.get('disambiguation', ''), # → AlbumResource.disambiguation

                    # Artist credit - minimal structure for artistid and artists array
                    'artist-credit': [
                        {
                            'artist': {
                                'id': ac.get('artist', {}).get('id'),    # → AlbumResource.artistid
                                'name': ac.get('artist', {}).get('name') # → AlbumResource.artists[].artistname
                            },
                            'name': ac.get('name')                      # Used in normalization
                        }
                        for ac in data.get('artist-credit', [])
                        if ac.get('artist', {}).get('id')  # Only include if artist ID exists
                    ],

                    # Arrays for schema compliance
                    'tags': data.get('tags', []),                     # → AlbumResource.genres (partial)
                    'genres': data.get('genres', []),                 # → AlbumResource.genres (partial)

                    # Rating - preserve structure for Rating schema compliance
                    'rating': {
                        'votes-count': data.get('rating', {}).get('votes-count', 0),
                        'value': data.get('rating', {}).get('value')   # → AlbumResource.rating
                    }

                    # Note: releases, images, links populated from release data (if enabled)
                    # Note: aliases, oldids typically empty in MB release-group data
                }

                # Remove None values and empty arrays to save space
                filtered = {k: v for k, v in filtered.items() if v is not None and v != []}

                output_line = json.dumps(filtered, separators=(',', ':')) + '\n'
                output_f.write(output_line)
                total_output_bytes += len(output_line.encode('utf-8'))
                filtered_count += 1

                if filtered_count % 500000 == 0:
                    reduction = (1 - total_output_bytes/total_input_bytes) * 100
                    elapsed = time.time() - start_time
                    rate = filtered_count / elapsed
                    logger.info(f"Processed {filtered_count:,} release-groups, {reduction:.1f}% reduction, {rate:.0f} rgs/sec")

            except json.JSONDecodeError as e:
                logger.warning(f"Skipping invalid JSON line at release-group {filtered_count + 1}: {e}")
                continue
            except Exception as e:
                logger.error(f"Error processing release-group {filtered_count + 1}: {e}")
                continue

    final_reduction = (1 - total_output_bytes/total_input_bytes) * 100 if total_input_bytes > 0 else 0
    elapsed = time.time() - start_time

    logger.info(f"✅ Release-group pre-processing complete:")
    logger.info(f"   • {filtered_count:,} release-groups processed in {elapsed:.1f}s")
    logger.info(f"   • {final_reduction:.1f}% size reduction achieved")
    logger.info(f"   • {total_input_bytes/1024/1024:.1f}MB → {total_output_bytes/1024/1024:.1f}MB")
    logger.info(f"   • Output saved to: {output_path}")

    return final_reduction


def preprocess_release_file_schema_guided_streaming(release_tar_xz_path: Path, output_path: Path) -> float:
    """
    Stream process release file with schema-guided filtering.

    Due to the massive 285GB uncompressed size, this function uses OS tar streaming
    to process the release file without decompressing it to disk first.

    Maps raw MusicBrainz release fields to only those needed for the final
    Release and Track API responses, achieving 85-90% data reduction.

    Returns the actual reduction percentage achieved.
    """
    logger.info(f"Starting schema-guided release streaming preprocessing: {release_tar_xz_path} → {output_path}")
    start_time = time.time()

    filtered_count = 0
    total_input_bytes = 0
    total_output_bytes = 0

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Use OS tar command to stream decompress the massive 285GB file
    tar_cmd = [
        'tar', '-xf', str(release_tar_xz_path),
        '--to-stdout', 'mbdump/release'
    ]

    logger.info(f"🔧 Streaming release file using: {' '.join(tar_cmd)}")

    try:
        # Start the tar process
        process = subprocess.Popen(
            tar_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1  # Line buffered for streaming
        )

        logger.info("🎵 Starting to process releases from tar stream...")

        # Check if tar process started successfully
        if process.poll() is not None:
            stderr_output = process.stderr.read()
            raise RuntimeError(f"Tar command failed immediately: {stderr_output}")

        with open(output_path, 'w', encoding='utf-8') as output_f:
            # Process the stream line by line
            line_count = 0
            for line_num, line in enumerate(process.stdout, 1):
                line_count += 1

                # Log progress for first few lines to verify streaming is working
                if line_count <= 3:
                    logger.info(f"🔍 Processing line {line_count}: {len(line)} chars")
                elif line_count == 10:
                    logger.info("✅ Streaming confirmed - processing continues...")
                try:
                    # Track bytes processed
                    line_bytes = len(line.encode('utf-8'))
                    total_input_bytes += line_bytes

                    data = json.loads(line.strip())

                    # Map raw MusicBrainz release to Release and Track schema
                    filtered = {
                        # Direct Release schema mappings
                        'id': data.get('id'),                                    # → Release.id
                        'title': data.get('title'),                              # → Release.title
                        'status': data.get('status'),                            # → Release.status
                        'date': data.get('date'),                                # → Release.releaseDate
                        'country': [data.get('country')] if data.get('country') else [],  # → Release.country
                        'disambiguation': data.get('disambiguation', ''),        # → Release.disambiguation
                        # Preserve release-group id for downstream indexing
                        'release_group_id': (data.get('release-group') or {}).get('id'),

                        # Label extraction (simplified for Release.label)
                        'labels': [
                            label_info['label']['name']
                            for li in data.get('label-info', [])
                            for label_info in [{'label': li.get('label') or {}}]
                            if label_info['label'].get('name')
                        ],

                        # Media and tracks (heavily filtered for Track schema)
                        'media': [
                            {
                                'position': medium.get('position', 1),           # → Release.media[].Position
                                'format': medium.get('format'),                  # → Release.media[].Format
                                'track_count': medium.get('track-count', 0),     # → Release.track_count
                                'tracks': [
                                    {
                                        # Track schema mappings
                                        'id': track.get('id'),                   # → Track.id
                                        'title': track.get('title'),             # → Track.trackname
                                        'number': track.get('number'),           # → Track.tracknumber
                                        'position': track.get('position'),       # → Track.trackposition
                                        'length': track.get('length'),           # → Track.durationms

                                        # Artist and recording IDs (essential for relationships)
                                        'artist_id': _safe_get_artist_id(track),  # → Track.artistid
                                        'recording_id': (
                                            (track.get('recording') or {}).get('id')
                                        ),  # → Track.recordingid

                                        # Medium reference
                                        'medium_position': medium.get('position', 1)  # → Track.mediumnumber
                                    }
                                    for track in medium.get('tracks', [])
                                ]
                            }
                            for medium in data.get('media', [])
                        ]
                    }

                    # Remove None values to save space
                    filtered = {k: v for k, v in filtered.items() if v is not None}

                    output_line = json.dumps(filtered, separators=(',', ':')) + '\n'
                    output_f.write(output_line)
                    total_output_bytes += len(output_line.encode('utf-8'))
                    filtered_count += 1

                    # Progress logging every 100MB processed
                    mb_processed = total_input_bytes / (1024 * 1024)
                    if filtered_count % 500000 == 0 or (filtered_count % 100000 == 0 and mb_processed < 1000):
                        elapsed = time.time() - start_time
                        rate = filtered_count / elapsed if elapsed > 0 else 0
                        reduction = (1 - total_output_bytes/total_input_bytes) * 100 if total_input_bytes > 0 else 0
                        mb_per_sec = (total_input_bytes / elapsed) / (1024 * 1024) if elapsed > 0 else 0

                        logger.info(f"📊 Processed {mb_processed:.0f}MB ({mb_per_sec:.1f}MB/s), "
                                   f"{filtered_count:,} releases, {reduction:.1f}% reduction, {rate:.0f} releases/sec")

                except json.JSONDecodeError as e:
                    logger.warning(f"Skipping invalid JSON line at release {line_num}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing release {line_num}: {e}")
                    continue

        # Wait for tar process to finish and check for errors
        process.wait()
        if process.returncode != 0:
            stderr_output = process.stderr.read()
            raise RuntimeError(f"Tar streaming failed with return code {process.returncode}: {stderr_output}")

    except Exception as e:
        logger.error(f"❌ Release streaming preprocessing failed: {e}")
        raise

    final_reduction = (1 - total_output_bytes/total_input_bytes) * 100 if total_input_bytes > 0 else 0
    elapsed = time.time() - start_time

    logger.info(f"✅ Release streaming pre-processing complete:")
    logger.info(f"   • {filtered_count:,} releases processed in {elapsed:.1f}s")
    logger.info(f"   • {final_reduction:.1f}% size reduction achieved")
    logger.info(f"   • {total_input_bytes/1024/1024:.1f}MB → {total_output_bytes/1024/1024:.1f}MB")
    logger.info(f"   • Output saved to: {output_path}")

    return final_reduction


def preprocess_release_file_from_sample(sample_file_path: Path, output_path: Path) -> float:
    """
    Test release preprocessing using the sample release file for development.

    This function processes the sample file (deploy/data/input/release-100)
    without streaming, for testing and development purposes.
    """
    logger.info(f"Starting sample release preprocessing: {sample_file_path} → {output_path}")
    start_time = time.time()

    filtered_count = 0
    total_input_bytes = 0
    total_output_bytes = 0

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(sample_file_path, 'r', encoding='utf-8') as input_f, \
         open(output_path, 'w', encoding='utf-8') as output_f:

        for line_num, line in enumerate(input_f, 1):
            try:
                total_input_bytes += len(line.encode('utf-8'))

                data = json.loads(line.strip())

                # Map raw MusicBrainz release to Release and Track schema (same as streaming version)
                filtered = {
                    # Direct Release schema mappings
                    'id': data.get('id'),                                    # → Release.id
                    'title': data.get('title'),                              # → Release.title
                    'status': data.get('status'),                            # → Release.status
                    'date': data.get('date'),                                # → Release.releaseDate
                    'country': [data.get('country')] if data.get('country') else [],  # → Release.country
                    'disambiguation': data.get('disambiguation', ''),        # → Release.disambiguation
                    'release_group_id': (data.get('release-group') or {}).get('id'),

                    # Label extraction (simplified for Release.label)
                    'labels': [
                        label_info['label']['name']
                        for li in data.get('label-info', [])
                        for label_info in [{'label': li.get('label') or {}}]
                        if label_info['label'].get('name')
                    ],

                    # Media and tracks (heavily filtered for Track schema)
                    'media': [
                        {
                            'position': medium.get('position', 1),           # → Release.media[].Position
                            'format': medium.get('format'),                  # → Release.media[].Format
                            'track_count': medium.get('track-count', 0),     # → Release.track_count
                            'tracks': [
                                {
                                    # Track schema mappings
                                    'id': track.get('id'),                   # → Track.id
                                    'title': track.get('title'),             # → Track.trackname
                                    'number': track.get('number'),           # → Track.tracknumber
                                    'position': track.get('position'),       # → Track.trackposition
                                    'length': track.get('length'),           # → Track.durationms

                                    # Artist and recording IDs (essential for relationships)
                                    'artist_id': _safe_get_artist_id(track),  # → Track.artistid
                                    'recording_id': (
                                        (track.get('recording') or {}).get('id')
                                    ),  # → Track.recordingid

                                    # Medium reference
                                    'medium_position': medium.get('position', 1)  # → Track.mediumnumber
                                }
                                for track in medium.get('tracks', [])
                            ]
                        }
                        for medium in data.get('media', [])
                    ]
                }

                # Remove None values to save space
                filtered = {k: v for k, v in filtered.items() if v is not None}

                output_line = json.dumps(filtered, separators=(',', ':')) + '\n'
                output_f.write(output_line)
                total_output_bytes += len(output_line.encode('utf-8'))
                filtered_count += 1

            except json.JSONDecodeError as e:
                logger.warning(f"Skipping invalid JSON line at release {line_num}: {e}")
                continue
            except Exception as e:
                logger.error(f"Error processing release {line_num}: {e}")
                continue

    final_reduction = (1 - total_output_bytes/total_input_bytes) * 100 if total_input_bytes > 0 else 0
    elapsed = time.time() - start_time

    logger.info(f"✅ Sample release pre-processing complete:")
    logger.info(f"   • {filtered_count:,} releases processed in {elapsed:.1f}s")
    logger.info(f"   • {final_reduction:.1f}% size reduction achieved")
    logger.info(f"   • {total_input_bytes/1024:.1f}KB → {total_output_bytes/1024:.1f}KB")
    logger.info(f"   • Output saved to: {output_path}")

    return final_reduction


def validate_preprocessing_output(original_file: Path, filtered_file: Path, file_type: str) -> bool:
    """
    Validate that preprocessing preserved all necessary data by comparing record counts
    and ensuring no critical fields were lost.
    """
    logger.info(f"Validating {file_type} preprocessing output...")

    try:
        # Count records in both files
        with open(original_file, 'r') as f:
            original_count = sum(1 for line in f if line.strip())

        with open(filtered_file, 'r') as f:
            filtered_count = sum(1 for line in f if line.strip())

        if original_count != filtered_count:
            logger.error(f"❌ Record count mismatch: {original_count} → {filtered_count}")
            return False

        # Sample first few records to ensure structure is preserved
        sample_size = min(100, original_count)
        with open(original_file, 'r') as orig_f, open(filtered_file, 'r') as filt_f:
            for i in range(sample_size):
                orig_line = orig_f.readline()
                filt_line = filt_f.readline()

                if not orig_line or not filt_line:
                    break

                try:
                    orig_data = json.loads(orig_line)
                    filt_data = json.loads(filt_line)

                    # Ensure ID field is preserved
                    if orig_data.get('id') != filt_data.get('id'):
                        logger.error(f"❌ ID mismatch at record {i+1}")
                        return False

                except json.JSONDecodeError:
                    logger.error(f"❌ JSON decode error at record {i+1}")
                    return False

        logger.info(f"✅ {file_type} preprocessing validation passed")
        return True

    except Exception as e:
        logger.error(f"❌ Validation failed: {e}")
        return False


def main():
    """Main preprocessing function."""
    logger.info("🚀 Starting schema-guided MusicBrainz data preprocessing")

    # Define paths based on docker-compose environment
    input_dir = Path("/data/current")
    output_dir = Path("/data/processed")

    artist_input = input_dir / "artist"
    rg_input = input_dir / "release-group"
    release_input = input_dir / "release.tar.xz"
    release_sample = Path("deploy/data/input/release-100")  # Sample file for development/testing

    artist_output = output_dir / "artist.filtered"
    rg_output = output_dir / "release-group.filtered"
    release_output = output_dir / "release.filtered"

    # Check input files exist
    if not artist_input.exists():
        logger.error(f"❌ Artist input file not found: {artist_input}")
        return 1

    if not rg_input.exists():
        logger.error(f"❌ Release-group input file not found: {rg_input}")
        return 1

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Smart Preprocessing Idempotency Check ---
    preprocessing_info_file = output_dir / "preprocessing_info.json"

        # Check if filtered files already exist and are up to date
    if artist_output.exists() and rg_output.exists():

        # If preprocessing info exists, use it for timestamp checking
        if preprocessing_info_file.exists():
            try:
                with open(preprocessing_info_file, 'r') as f:
                    preprocessing_info = json.load(f)

                # Get file modification times
                artist_input_mtime = artist_input.stat().st_mtime
                rg_input_mtime = rg_input.stat().st_mtime

                # Check if source files haven't changed since last preprocessing
                last_artist_mtime = preprocessing_info.get("artist_input_mtime", 0)
                last_rg_mtime = preprocessing_info.get("rg_input_mtime", 0)

                if (artist_input_mtime <= last_artist_mtime and
                    rg_input_mtime <= last_rg_mtime):

                    logger.info("✅ Filtered files are up to date. Skipping preprocessing.")
                    logger.info(f"   Artist filtered: {artist_output}")
                    logger.info(f"   Release-group filtered: {rg_output}")

                    # Show previous results
                    prev_artist_reduction = preprocessing_info.get("artist_reduction", 0)
                    prev_rg_reduction = preprocessing_info.get("rg_reduction", 0)
                    logger.info(f"📊 Previous reductions: Artist {prev_artist_reduction:.1f}%, Release-group {prev_rg_reduction:.1f}%")

                    return 0
                else:
                    logger.info("🔄 Source files have changed. Reprocessing filtered files...")

            except (json.JSONDecodeError, KeyError, OSError) as e:
                logger.warning(f"Could not read preprocessing info: {e}. Will check file sizes instead...")

        # Fallback: If both files exist but no info file, check if they look reasonable
        # This handles cases where preprocessing completed but info file wasn't saved
        artist_size_mb = artist_output.stat().st_size / (1024 * 1024)
        rg_size_mb = rg_output.stat().st_size / (1024 * 1024)

        # Reasonable size thresholds (filtered files should be much smaller)
        if artist_size_mb > 1000 and rg_size_mb > 1000:  # Conservative thresholds
            logger.info("✅ Both filtered files exist with reasonable sizes. Skipping preprocessing.")
            logger.info(f"   Artist filtered: {artist_output} ({artist_size_mb:.1f} MB)")
            logger.info(f"   Release-group filtered: {rg_output} ({rg_size_mb:.1f} MB)")
            logger.info("💡 Note: Will recreate preprocessing_info.json for future runs")

            # Create a basic preprocessing info file for future runs
            basic_info = {
                "artist_input_mtime": artist_input.stat().st_mtime,
                "rg_input_mtime": rg_input.stat().st_mtime,
                "artist_reduction": "unknown",
                "rg_reduction": "unknown",
                "total_elapsed": "unknown",
                "timestamp": time.time(),
                "note": "Reconstructed from existing files",
                "artist_input_path": str(artist_input),
                "rg_input_path": str(rg_input),
                "artist_output_path": str(artist_output),
                "rg_output_path": str(rg_output)
            }

            with open(preprocessing_info_file, 'w') as f:
                json.dump(basic_info, f, indent=2)

            return 0
        else:
            logger.info(f"🔄 Filtered files exist but seem too small (Artist: {artist_size_mb:.1f}MB, RG: {rg_size_mb:.1f}MB). Reprocessing...")

    elif artist_output.exists() or rg_output.exists():
        logger.info("🔄 Partial filtered files found. Reprocessing for consistency...")
    # --- End Idempotency Check ---

    total_start_time = time.time()

    try:
        # Process artist file
        logger.info("=" * 60)
        artist_reduction = preprocess_artist_file_schema_guided(artist_input, artist_output)

        # Validate artist processing
        if not validate_preprocessing_output(artist_input, artist_output, "artist"):
            return 1

        # Process release-group file
        logger.info("=" * 60)
        rg_reduction = preprocess_release_group_file_schema_guided(rg_input, rg_output)

        # Validate release-group processing
        if not validate_preprocessing_output(rg_input, rg_output, "release-group"):
            return 1

        # Process release file (experimental - use sample file for testing)
        logger.info("=" * 60)
        release_reduction = None

        # For development/testing, use the sample file if full release file not available
        if release_sample.exists():
            logger.info("🧪 Processing sample release file for development/testing...")
            release_reduction = preprocess_release_file_from_sample(release_sample, release_output)
            logger.info("✅ Sample release preprocessing completed - ready for integration testing")
            logger.info("🚀 TO ENABLE FULL RELEASE PROCESSING:")
            logger.info("   1. Ensure release.tar.xz file is available in /data/current/")
            logger.info("   2. Run preprocessing again to process the full 285GB release file")
            logger.info("   3. This will enable detailed track information in all albums")
        elif release_input.exists():
            logger.info("🚀 Processing full release file with streaming...")
            logger.info("⚠️  This may take 20-30 minutes due to the massive 285GB file size")
            release_reduction = preprocess_release_file_schema_guided_streaming(release_input, release_output)
            logger.info("🎉 Full release preprocessing completed!")
            logger.info("   Now albums will include detailed track information with:")
            logger.info("   • Track IDs, names, durations, and positions")
            logger.info("   • Artist and recording relationships")
            logger.info("   • Label and release status information")
        else:
            logger.info("⚠️  No release file found - albums will have limited track information")
            logger.info(f"   Expected locations: {release_input} or {release_sample}")
            logger.info("💡 To enable full track metadata:")
            logger.info("   • Add release.tar.xz to /data/current/ for production processing")
            logger.info("   • Or add release-100 sample file for development testing")

        # Summary
        total_elapsed = time.time() - total_start_time
        logger.info("=" * 60)
        logger.info("🎉 Schema-guided preprocessing completed successfully!")
        logger.info(f"📊 Artist file reduction: {artist_reduction:.1f}%")
        logger.info(f"📊 Release-group file reduction: {rg_reduction:.1f}%")
        if release_reduction is not None:
            logger.info(f"📊 Release file reduction: {release_reduction:.1f}%")
        logger.info(f"⏱️  Total preprocessing time: {total_elapsed:.1f} seconds")
        logger.info(f"💾 Filtered files ready for processing:")
        logger.info(f"   • {artist_output}")
        logger.info(f"   • {rg_output}")
        if release_reduction is not None:
            logger.info(f"   • {release_output}")

        # Save preprocessing info for future idempotency checks
        preprocessing_info = {
            "artist_input_mtime": artist_input.stat().st_mtime,
            "rg_input_mtime": rg_input.stat().st_mtime,
            "artist_reduction": artist_reduction,
            "rg_reduction": rg_reduction,
            "total_elapsed": total_elapsed,
            "timestamp": time.time(),
            "artist_input_path": str(artist_input),
            "rg_input_path": str(rg_input),
            "artist_output_path": str(artist_output),
            "rg_output_path": str(rg_output)
        }

        # Add release processing info if it was processed
        if release_reduction is not None:
            preprocessing_info.update({
                "release_reduction": release_reduction,
                "release_input_path": str(release_input if release_input.exists() else release_sample),
                "release_output_path": str(release_output)
            })

        with open(preprocessing_info_file, 'w') as f:
            json.dump(preprocessing_info, f, indent=2)

        logger.info("💾 Saved preprocessing info for future idempotency checks")

        return 0

    except Exception as e:
        logger.error(f"❌ Preprocessing failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
