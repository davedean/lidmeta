#!/usr/bin/env python3
# TEST COMMENT
"""
Advanced offline data processing script for MusicBrainz dumps.

This script implements the sophisticated processing pipeline documented in our markdown files:
1. Build reverse indexes for efficient random access
2. Stream from compressed files with constant memory usage
3. Process multiple artists in single pass through release data
4. Advanced error handling and progress tracking
5. Resume capability for large datasets

Input: /data/current/ (artist.tar.xz, release-group.tar.xz, release.tar.xz)
Output: /data/processed/ (artist/*, album/*, artist.db, release-group.db)
"""

import asyncio
import logging
import os
import sys
import json
import sqlite3
import tarfile
import hashlib
from pathlib import Path
from typing import Optional, Dict, List, Any, Set, Tuple
from collections import defaultdict
import time
import argparse

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configuration
PROCESSING_CONFIG = {
    "max_artists_to_process": None, #100000,  # Set to an integer to limit processing for testing, None for all
    # Release group filtering - set to None to process all, or list of types to include
    "include_release_types": ["Album"],  # Options: ["Album"], ["Album", "EP"], None for all
    "exclude_secondary_types": ["Live", "Compilation"],  # Secondary types to exclude
    "include_artist_types": None,  # Options: ["Person", "Group"], None for all
    # Index configuration
    "create_name_index": True,  # Create artist name to MBID lookup index
    "enable_release_data": True,  # Enable release data processing (was disabled for dev speed)
    # Subdirectory configuration for filesystem performance
    "use_subdirectories": True,  # Use subdirectories to avoid too many files in one directory
    "subdirectory_depth": 2,  # Number of subdirectory levels (e.g., 2 = a7/4b/)
    "max_files_per_directory": 1000,  # Target max files per directory
    # Index persistence for faster development cycles
    "persist_indexes": True,  # Save/load indexes to/from disk for reuse
    "force_rebuild_indexes": False,  # Force rebuild indexes even if they exist
    # Memory management
    "memory_limit_mb": 6000,  # Memory limit in MB (6GB for 8GB container)
    "enable_memory_monitoring": True,  # Monitor memory usage during processing
    # --- New Feature Flag ---
    "use_full_release_data": True, # Enable release data processing with automatic preprocessing
    # --- Test Flags for Performance Bottleneck Isolation ---
    "test_1_filesystem_bottleneck": False,  # Test 1: Replace individual files with temp file appending
    "test_2_combined_sqlite_progress": False,  # Test 2: Disable SQLite streaming + progress tracking
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)


# --- New Helper Function for Line-based Reading ---

def get_line_by_offset(file_path: Path, offset: int) -> Optional[str]:
    """Efficiently retrieves a specific line from a file using a byte offset."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            f.seek(offset)
            return f.readline().strip()
    except Exception as e:
        logger.error(f"Could not read line from offset {offset} in {file_path}: {e}")
    return None

# --- Utility functions used by the new pipeline ---

# Progress tracking removed - file existence provides resume capability
# Old O(n²) progress tracking was consuming 80% of CPU time for 100k+ artists

def should_include_release_group(data: Dict[str, Any]) -> bool:
    """Filter function for release groups based on configuration."""
    if PROCESSING_CONFIG["include_release_types"]:
        if data.get("primary-type") not in PROCESSING_CONFIG["include_release_types"]: return False
    if PROCESSING_CONFIG["exclude_secondary_types"]:
        secondary_types = data.get("secondary-types", [])
        if any(excluded in secondary_types for excluded in PROCESSING_CONFIG["exclude_secondary_types"]): return False
    return True

def should_include_artist(data: Dict[str, Any]) -> bool:
    """Filter function for artists based on configuration."""
    if not PROCESSING_CONFIG["include_artist_types"]: return True
    return data.get("type") in PROCESSING_CONFIG["include_artist_types"]

def create_artist_search_db(artists: Dict[str, Any], output_dir: Path) -> bool:
    """Create SQLite FTS5 search database for artists."""
    db_path = output_dir / "artist.db"
    if db_path.exists(): db_path.unlink()
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("CREATE VIRTUAL TABLE artists_fts USING fts5(id, name, sort_name, unaccented_name, genres, type, country, disambiguation)")
        for mbid, artist in artists.items():
            cursor.execute("INSERT INTO artists_fts VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (
                mbid, artist.get("artistName", ""), artist.get("sortName", ""), artist.get("artistName", "").lower(),
                json.dumps(artist.get("genres", [])), artist.get("type", ""), "", artist.get("disambiguation", "")
            ))
        conn.commit()
        conn.close()
        logger.info(f"✅ Created artist search database with {len(artists)} artists.")
        return True
    except Exception as e:
        logger.error(f"❌ Error creating artist search database: {e}")
        return False

# def create_release_group_search_db(albums: Dict[str, Any], output_dir: Path) -> bool:
#     """Create SQLite FTS5 search database for release groups."""
#     db_path = output_dir / "release-group.db"
#     if db_path.exists(): db_path.unlink()
#     try:
#         conn = sqlite3.connect(db_path)
#         cursor = conn.cursor()
#         cursor.execute("CREATE VIRTUAL TABLE albums_fts USING fts5(id, title, artist_id, artist_name, unaccented_title, genres, type, release_date)")
#         for mbid, album in albums.items():
#             cursor.execute("INSERT INTO albums_fts VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (
#                 mbid, album.get("title", ""), "", "", album.get("title", "").lower(),
#                 json.dumps(album.get("genres", [])), album.get("type", ""), album.get("releaseDate", "")
#             ))
#         conn.commit()
#         conn.close()
#         logger.info(f"✅ Created release group search database with {len(albums)} albums.")
#         return True
#     except Exception as e:
#         logger.error(f"❌ Error creating release group search database: {e}")
#         return False

def get_subdirectory_path(mbid: str, base_dir: Path, depth: int = 2) -> Path:
    subdirs = [mbid[i*2:i*2+2] for i in range(depth)]
    return base_dir.joinpath(*subdirs)

def ensure_subdirectory_exists(mbid: str, base_dir: Path, depth: int = 2) -> Path:
    subdir_path = get_subdirectory_path(mbid, base_dir, depth)
    subdir_path.mkdir(parents=True, exist_ok=True)
    return subdir_path

def create_streaming_search_dbs(output_dir: Path) -> Tuple[sqlite3.Connection, sqlite3.Connection]:
    """Create search databases for streaming inserts during processing."""
    # Create artist search database
    artist_db_path = output_dir / "artist.db"
    if artist_db_path.exists():
        artist_db_path.unlink()

    artist_conn = sqlite3.connect(artist_db_path)
    artist_cursor = artist_conn.cursor()
    artist_cursor.execute("CREATE VIRTUAL TABLE artists_fts USING fts5(id, name, sort_name, unaccented_name, genres, type, country, disambiguation)")

    # Create album search database
    album_db_path = output_dir / "release-group.db"
    if album_db_path.exists():
        album_db_path.unlink()

    album_conn = sqlite3.connect(album_db_path)
    album_cursor = album_conn.cursor()
    album_cursor.execute("CREATE VIRTUAL TABLE albums_fts USING fts5(id, title, artist_id, artist_name, unaccented_title, genres, type, release_date)")

    logger.info("✅ Initialized streaming search databases")
    return artist_conn, album_conn

def build_file_mapping_incrementally(depth: int = 2):
    """Build file path mapping incrementally without storing full objects."""
    mapping = {"artists": {}, "albums": {}, "subdirectory_depth": depth}
    artist_base = Path("artist")
    album_base = Path("album")

    def add_artist_mapping(artist_id: str):
        path = get_subdirectory_path(artist_id, artist_base, depth)
        mapping["artists"][artist_id] = str(path / f"{artist_id}.json")

    def add_album_mapping(album_id: str):
        path = get_subdirectory_path(album_id, album_base, depth)
        mapping["albums"][album_id] = str(path / f"{album_id}.json")

    return mapping, add_artist_mapping, add_album_mapping

def stream_to_databases(artist_conn: sqlite3.Connection, album_conn: sqlite3.Connection,
                       artist: Dict[str, Any], albums: List[Dict[str, Any]]):
    """Stream single artist and albums to databases immediately."""
    # Insert artist to search database
    artist_cursor = artist_conn.cursor()
    artist_cursor.execute("INSERT INTO artists_fts VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (
        artist["id"],
        artist.get("artistName", ""),
        artist.get("sortName", ""),
        artist.get("artistName", "").lower(),
        json.dumps(artist.get("genres", [])),
        artist.get("type", ""),
        "",
        artist.get("disambiguation", "")
    ))

    # Insert albums to search database
    album_cursor = album_conn.cursor()
    for album in albums:
        album_cursor.execute("INSERT INTO albums_fts VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (
            album["id"],
            album.get("title", ""),
            "",  # artist_id populated later if needed
            "",  # artist_name populated later if needed
            album.get("title", "").lower(),
            json.dumps(album.get("genres", [])),
            album.get("type", ""),
            album.get("releaseDate", "")
        ))

def create_file_path_mapping(processed_artists: Dict[str, Any], processed_albums: Dict[str, Any],
                           output_dir: Path, depth: int = 2) -> Dict[str, Any]:
    mapping = { "artists": {}, "albums": {}, "subdirectory_depth": depth }
    artist_base = Path("artist")
    album_base = Path("album")
    for mbid in processed_artists:
        mapping["artists"][mbid] = str(get_subdirectory_path(mbid, artist_base, depth) / f"{mbid}.json")
    for mbid in processed_albums:
        mapping["albums"][mbid] = str(get_subdirectory_path(mbid, album_base, depth) / f"{mbid}.json")
    return mapping

# --- Refactored Main Processing Logic ---

def process_single_artist(
    artist_id: str,
    artist_offset_index: Dict[str, int],
    rg_offset_index: Dict[str, int],
    artist_to_rgs_index: Dict[str, List[str]],
    artist_file: Path,
    release_group_file: Path,
    output_dir: Path,
    # Optional params for full release processing
    release_offset_index: Optional[Dict[str, int]] = None,
    rg_to_release_ids_index: Optional[Dict[str, List[str]]] = None,
    release_file: Optional[Path] = None
):
    """
    Processes a single artist by performing targeted reads from data files.
    This function has a very low and constant memory footprint.
    """
    try:
        # 1. Get Artist Data
        artist_offset = artist_offset_index.get(artist_id)
        if artist_offset is None:
            logger.warning(f"Could not find artist {artist_id} in offset index. Skipping.")
            return None, None

        artist_line = get_line_by_offset(artist_file, artist_offset)
        if not artist_line:
            logger.warning(f"Could not read line for artist {artist_id}. Skipping.")
            return None, None

        artist_data = json.loads(artist_line)

        # 2. Get Release Group Data for this artist
        rg_ids = artist_to_rgs_index.get(artist_id, [])
        artist_release_groups = []
        for rg_id in rg_ids:
            rg_offset = rg_offset_index.get(rg_id)
            if rg_offset is not None:
                rg_line = get_line_by_offset(release_group_file, rg_offset)
                if rg_line:
                    artist_release_groups.append(json.loads(rg_line))

        # 3. Normalize the artist data (which includes album summaries)
        from data_processor.normalizer import normalize_radiohead_artist_data
        raw_data_for_artist_norm = {
            "artist": artist_data,
            "release_groups": artist_release_groups,
        }
        normalized_artist = normalize_radiohead_artist_data(
            artist_data,
            artist_release_groups
        )
        if not normalized_artist:
            logger.error(f"Normalization failed for artist {artist_id}, skipping.")
            return None, None

        # 4. Write final artist file
        if PROCESSING_CONFIG["test_1_filesystem_bottleneck"]:
            # TEST 1: Append to temporary file instead of individual files
            temp_artist_file = output_dir / "temp_artists.jsonl"
            with open(temp_artist_file, 'a') as f:
                f.write(json.dumps(normalized_artist) + '\n')
        else:
            # Normal operation: individual files
            artist_output_dir = output_dir / "artist"
            artist_path = ensure_subdirectory_exists(artist_id, artist_output_dir) / f"{artist_id}.json"
            with open(artist_path, 'w') as f:
                json.dump(normalized_artist, f)

        # 5. Normalize and write final album files
        normalized_albums = []
        album_output_dir = output_dir / "album"
        for rg_data in artist_release_groups:
            if not should_include_release_group(rg_data): continue

            # Load release data for this release group if available
            releases_for_rg = []
            if (release_offset_index and rg_to_release_ids_index and release_file):
                # Check if release file needs preprocessing
                if not release_file.exists():
                    release_tar_path = input_dir / "release.tar.xz"
                    if release_tar_path.exists():
                        logger.info("📦 Release file not available, but found compressed release.tar.xz")
                        logger.info("💡 Use filtered release file created during index building for optimal performance")
                        # Don't try to process here - the filtered file should be created during index building
                        release_file = None
                    else:
                        release_file = None

                # Load release data if file is now available
                if release_file and release_file.exists():
                    rg_id = rg_data.get("id")
                    release_ids = rg_to_release_ids_index.get(rg_id, [])

                    for release_id in release_ids:
                        release_offset = release_offset_index.get(release_id)
                        if release_offset is not None:
                            release_line = get_line_by_offset(release_file, release_offset)
                            if release_line:
                                try:
                                    release_data = json.loads(release_line)
                                    releases_for_rg.append(release_data)
                                except json.JSONDecodeError:
                                    logger.warning(f"Could not parse release {release_id} for RG {rg_id}")
                                    continue

            from data_processor.normalizer import normalize_album_data
            normalized_album = normalize_album_data(rg_data, artist_data, releases_for_rg)
            if normalized_album:
                if PROCESSING_CONFIG["test_1_filesystem_bottleneck"]:
                    # TEST 1: Append to temporary file instead of individual files
                    temp_album_file = output_dir / "temp_albums.jsonl"
                    with open(temp_album_file, 'a') as f:
                        f.write(json.dumps(normalized_album) + '\n')
                else:
                    # Normal operation: individual files
                    album_path = ensure_subdirectory_exists(normalized_album["id"], album_output_dir) / f"{normalized_album['id']}.json"
                    with open(album_path, 'w') as f:
                        json.dump(normalized_album, f)
                normalized_albums.append(normalized_album)

        return normalized_artist, normalized_albums

    except json.JSONDecodeError as e:
        logger.error(f"❌ Failed to process artist {artist_id}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None, None


async def main(args):
    """Main data processing pipeline using a per-artist streaming model."""
    logger.info("🚀 Starting Memory-Safe Data Processing Pipeline")

    input_dir = Path("/data/current")
    output_dir = Path("/data/processed")
    index_dir = output_dir / "indexes"

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "artist").mkdir(exist_ok=True)
    (output_dir / "album").mkdir(exist_ok=True)

    # 1. Load all pre-built indexes into memory
    logger.info("--- Step 1: Loading pre-built indexes ---")
    release_offset_index = None
    rg_to_release_ids_index = None
    release_file = None

    try:
        with open(index_dir / "artist_to_byte_offset.json", 'r') as f:
            artist_offset_index = json.load(f)
        with open(index_dir / "rg_to_byte_offset.json", 'r') as f:
            rg_offset_index = json.load(f)
        with open(index_dir / "artist_to_rg_ids.json", 'r') as f:
            artist_to_rgs_index = json.load(f)

        if PROCESSING_CONFIG["use_full_release_data"]:
            logger.info("Full release data processing is ENABLED. Loading release indexes...")
            with open(index_dir / "release_to_byte_offset.json", 'r') as f:
                release_offset_index = json.load(f)
            with open(index_dir / "rg_to_release_ids.json", 'r') as f:
                rg_to_release_ids_index = json.load(f)
            release_file = input_dir / "release"
        else:
            logger.info("Full release data processing is DISABLED. Album files will not contain detailed track info.")

        logger.info("✅ All required indexes loaded successfully.")
    except FileNotFoundError as e:
        logger.error(f"❌ Index file not found: {e}. Please run `data_processor/build_indexes.py` first.")
        sys.exit(1)

    # Use filtered files if available, fall back to original files
    artist_file = output_dir / "artist.filtered" if (output_dir / "artist.filtered").exists() else input_dir / "artist"
    release_group_file = output_dir / "release-group.filtered" if (output_dir / "release-group.filtered").exists() else input_dir / "release-group"

    # Update release file handling for filtered support
    if PROCESSING_CONFIG["use_full_release_data"]:
        # Check for release file in order of preference:
        if (output_dir / "release.filtered").exists():
            release_file = output_dir / "release.filtered"
        elif (input_dir / "release").exists():
            release_file = input_dir / "release"
        else:
            # Check for compressed file - we'll handle extraction later if needed
            release_tar_path = input_dir / "release.tar.xz"
            if release_tar_path.exists():
                release_file = input_dir / "release"  # Set expected path after extraction
                logger.info("📦 Found release.tar.xz - will extract if needed during processing")
            else:
                release_file = None
                logger.warning("⚠️  No release file available - albums will have limited track information")

    # Log which files we're using
    if artist_file.name.endswith('.filtered'):
        logger.info("✅ Using pre-processed filtered artist file for optimal performance")
    else:
        logger.info("⚠️  Using original artist file - consider running preprocessing for better performance")

    if release_group_file.name.endswith('.filtered'):
        logger.info("✅ Using pre-processed filtered release-group file for optimal performance")
    else:
        logger.info("⚠️  Using original release-group file - consider running preprocessing for better performance")

    if PROCESSING_CONFIG["use_full_release_data"]:
        if release_file and release_file.name.endswith('.filtered'):
            logger.info("✅ Using pre-processed filtered release file for optimal performance")
        elif release_file and release_file.exists():
            logger.info("ℹ️  Using original release file")
        elif release_file:
            logger.info("📦 Release file will be extracted from release.tar.xz when needed")
        else:
            logger.info("⚠️  No release file available - consider adding release.tar.xz for track metadata")

    # Progress tracking removed - using fast file existence checks instead

    # --- Step 3: Process Artists ---
    logger.info("🚀 Starting artist processing...")

    # Initialize streaming architecture (eliminates memory accumulation)
    if PROCESSING_CONFIG["test_2_combined_sqlite_progress"]:
        # TEST 2: Skip database creation
        artist_db_conn, album_db_conn = None, None
        logger.info("⏭️  Skipping SQLite database creation for Test 2")
    else:
        artist_db_conn, album_db_conn = create_streaming_search_dbs(output_dir)

    file_mapping, add_artist_mapping, add_album_mapping = build_file_mapping_incrementally(PROCESSING_CONFIG["subdirectory_depth"])
    total_albums_processed = 0

    # --- TEST 1: Filesystem Bottleneck Isolation ---
    if PROCESSING_CONFIG["test_1_filesystem_bottleneck"]:
        logger.info("🧪 TEST 1 MODE: Filesystem bottleneck isolation - using temp file appending instead of individual files")
        # Clear any existing temp files
        temp_artist_file = output_dir / "temp_artists.jsonl"
        temp_album_file = output_dir / "temp_albums.jsonl"
        if temp_artist_file.exists():
            temp_artist_file.unlink()
        if temp_album_file.exists():
            temp_album_file.unlink()
        logger.info(f"📁 Temp files will be: {temp_artist_file}, {temp_album_file}")

    # --- TEST 2: Combined SQLite + Progress Bottleneck Isolation ---
    elif PROCESSING_CONFIG["test_2_combined_sqlite_progress"]:
        logger.info("🧪 TEST 2 MODE: Combined SQLite + Progress bottleneck isolation")
        logger.info("❌ SQLite streaming: DISABLED")
        logger.info("❌ Progress tracking: DISABLED")
        logger.info("📁 Using individual JSON files only")

    else:
        logger.info("📁 Normal mode: Creating individual JSON files in subdirectories")

    # Get a list of all artist IDs to be processed
    all_artist_ids = sorted(list(artist_offset_index.keys()))

    processed_artist_count = 0
    limit = PROCESSING_CONFIG.get("max_artists_to_process")
    total_artists_to_process = len(all_artist_ids)
    if limit:
        total_artists_to_process = min(total_artists_to_process, limit)
        logger.info(f"🛑 Processing will be limited to {limit} artists.")

    for artist_id in all_artist_ids:
        if limit and processed_artist_count >= limit:
            logger.info(f"🏁 Reached processing limit of {limit} artists. Stopping.")
            break

        # Fast file existence check for resume capability (replaces expensive progress tracking)
        artist_output_dir = output_dir / "artist"
        artist_output_file = None
        if PROCESSING_CONFIG["use_subdirectories"]:
            depth = PROCESSING_CONFIG["subdirectory_depth"]
            artist_output_file = get_subdirectory_path(artist_id, artist_output_dir, depth) / f"{artist_id}.json"
        else:
            artist_output_file = artist_output_dir / f"{artist_id}.json"

        if artist_output_file.exists():
            logger.debug(f"⏭️ Artist {artist_id} already processed (file exists). Skipping.")
            processed_artist_count += 1  # Count for accurate progress logging
            continue

        # Apply artist type filtering before processing
        artist_offset = artist_offset_index.get(artist_id)
        if artist_offset is not None:
            line = get_line_by_offset(artist_file, artist_offset)
            if line:
                try:
                    if not should_include_artist(json.loads(line)):
                        logger.info(f"⏭️ Artist {artist_id} filtered out by type. Skipping.")
                        continue
                except json.JSONDecodeError as e:
                    logger.warning(f"Could not parse artist data for {artist_id}: {e}. Skipping.")
                    continue

        normalized_artist, normalized_albums = process_single_artist(
            artist_id,
            artist_offset_index,
            rg_offset_index,
            artist_to_rgs_index,
            artist_file,
            release_group_file,
            output_dir,
            release_offset_index=release_offset_index,
            rg_to_release_ids_index=rg_to_release_ids_index,
            release_file=release_file
        )

        if normalized_artist and normalized_albums is not None:
            # Stream to databases immediately (no memory accumulation) - unless Test 2
            if not PROCESSING_CONFIG["test_2_combined_sqlite_progress"]:
                stream_to_databases(artist_db_conn, album_db_conn, normalized_artist, normalized_albums)

            # Build file mapping incrementally
            add_artist_mapping(artist_id)
            for album in normalized_albums:
                add_album_mapping(album["id"])

            # Update counters
            total_albums_processed += len(normalized_albums)
            processed_artist_count += 1

            # Adaptive logging based on dataset size (no expensive progress saving)
            if total_artists_to_process < 20000:
                # For smaller datasets (<20k): log every 2000
                log_interval = 2000
            else:
                # For larger datasets: log every 10000
                log_interval = 10000

            if processed_artist_count % log_interval == 0:
                logger.info(f"Processed {processed_artist_count}/{total_artists_to_process} artists...")
        else:
            logger.warning(f"❌ Failed to process artist {artist_id}")

    logger.info("--- Step 3: Finalizing ---")

    # Commit and close streaming databases - unless Test 2
    if not PROCESSING_CONFIG["test_2_combined_sqlite_progress"]:
        logger.info("Finalizing search databases...")
        artist_db_conn.commit()
        album_db_conn.commit()
        artist_db_conn.close()
        album_db_conn.close()
        logger.info(f"✅ Created artist search database with {processed_artist_count} artists.")
        logger.info(f"✅ Created release group search database with {total_albums_processed} albums.")
    else:
        logger.info("⏭️  Skipping database finalization for Test 2")

    # Save file path mapping (already built incrementally)
    if PROCESSING_CONFIG["use_subdirectories"]:
        logger.info("Saving file path mapping...")
        with open(output_dir / "file_path_mapping.json", 'w') as f:
            json.dump(file_mapping, f, indent=2, ensure_ascii=False)

    logger.info("🎉 Optimized Data Processing Pipeline Completed Successfully!")

    # Simple summary using counters (no expensive progress tracking)
    logger.info(f"📊 Artists processed: {processed_artist_count}/{total_artists_to_process}")
    logger.info(f"📊 Total albums created: {total_albums_processed}")
    logger.info("💡 Resume capability: File existence checks (fast and reliable)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lidarr MusicBrainz Data Processor")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of artists to process for development or testing."
    )
    args = parser.parse_args()
    try:
        # We need to pass the args to the main function
        asyncio.run(main(args))
    except KeyboardInterrupt:
        logger.info("⚠️  Pipeline interrupted by user")
