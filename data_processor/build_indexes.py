#!/usr/bin/env python3
"""
One-time script to build processing indexes for the main data processing pipeline.

This script creates several small index files that map entity MBIDs to their exact
line number in the large, uncompressed data files. This allows the main processor
to perform fast, targeted reads (seeks) to fetch data for a specific entity
without having to scan the entire file.

Run this script once before running the main `run_data_processing.py` script.

Indexes created in `temp_dir`:
- artist_to_line_offset.json: Maps artist MBID to its line number in the artist file.
- rg_to_line_offset.json: Maps release group MBID to its line number in the RG file.
- artist_to_rg_ids.json: Maps artist MBID to a list of its release group MBIDs.
"""
import json
import logging
import sqlite3
import sys
import time
from pathlib import Path
from collections import defaultdict

from unidecode import unidecode

# Add the project root to the Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from data_processor.main import should_include_release_group, PROCESSING_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def build_artist_line_index(artist_file: Path, output_dir: Path):
    """Builds an index mapping artist MBID to its line's byte offset."""
    logger.info(f"Building artist byte offset index from {artist_file.name}...")
    index = {}
    i = 0
    with open(artist_file, 'r', encoding='utf-8') as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            try:
                data = json.loads(line)
                artist_id = data.get("id")
                if artist_id:
                    index[artist_id] = offset
                if (i + 1) % 500000 == 0:
                    logger.info(f"  ...indexed {i+1:,} artists")
            except json.JSONDecodeError:
                continue
            finally:
                i += 1

    output_path = output_dir / "artist_to_byte_offset.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(index, f)
    logger.info(f"✅ Saved artist byte offset index with {len(index):,} entries to {output_path}")


def build_rg_indexes(release_group_file: Path, output_dir: Path):
    """
    Builds two indexes in one pass:
    1. Release Group MBID to its line's byte offset.
    2. Artist MBID to a list of its Release Group MBIDs.
    """
    logger.info(f"Building release group byte offset indexes from {release_group_file.name}...")
    rg_offset_index = {}
    artist_to_rgs = defaultdict(list)
    i = 0

    with open(release_group_file, 'r', encoding='utf-8') as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            try:
                data = json.loads(line)
                rg_id = data.get("id")
                if not rg_id:
                    continue

                # Index 1: RG ID to byte offset
                rg_offset_index[rg_id] = offset

                # Index 2: Artist ID to RG IDs
                if should_include_release_group(data):
                    artist_credit = data.get("artist-credit", [])
                    if artist_credit:
                        artist_id = artist_credit[0].get("artist", {}).get("id")
                        if artist_id:
                            artist_to_rgs[artist_id].append(rg_id)

                if (i + 1) % 500000 == 0:
                    logger.info(f"  ...indexed {i+1:,} release groups")

            except json.JSONDecodeError:
                continue
            finally:
                i += 1

    # Save RG Offset Index
    rg_offset_output_path = output_dir / "rg_to_byte_offset.json"
    with open(rg_offset_output_path, 'w', encoding='utf-8') as f:
        json.dump(rg_offset_index, f)
    logger.info(f"✅ Saved RG byte offset index with {len(rg_offset_index):,} entries to {rg_offset_output_path}")

    # Save Artist to RGs Index
    artist_rg_output_path = output_dir / "artist_to_rg_ids.json"
    with open(artist_rg_output_path, 'w', encoding='utf-8') as f:
        json.dump(artist_to_rgs, f)
    logger.info(f"✅ Saved artist-to-RGs index for {len(artist_to_rgs):,} artists to {artist_rg_output_path}")


def build_release_indexes(release_file: Path, output_dir: Path):
    """
    Builds two indexes related to releases in a single pass:
    1. Release MBID to its line's byte offset.
    2. Release Group MBID to a list of its Release MBIDs.
    """
    release_offset_output_path = output_dir / "release_to_byte_offset.json"

    if release_offset_output_path.exists():
        logger.info(f"-> Release index {release_offset_output_path} already exists. Skipping rebuild.")
        return

    logger.info(f"Building release byte offset indexes from {release_file.name}...")
    release_offset_index = {}
    rg_to_releases = defaultdict(list)
    i = 0

    with open(release_file, 'r', encoding='utf-8') as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            try:
                data = json.loads(line)
                release_id = data.get("id")
                if not release_id:
                    continue

                # Index 1: Release ID to byte offset
                release_offset_index[release_id] = offset

                # Index 2: Release Group ID to Release IDs
                rg_id = data.get("release-group", {}).get("id")
                if rg_id:
                    rg_to_releases[rg_id].append(release_id)

                if (i + 1) % 500000 == 0:
                    logger.info(f"  ...indexed {i+1:,} releases")

            except json.JSONDecodeError:
                continue
            finally:
                i += 1

    # Save Release Offset Index
    with open(release_offset_output_path, 'w', encoding='utf-8') as f:
        json.dump(release_offset_index, f)
    logger.info(f"✅ Saved release byte offset index with {len(release_offset_index):,} entries to {release_offset_output_path}")

    # Save RG to Releases Index
    rg_releases_output_path = output_dir / "rg_to_release_ids.json"
    with open(rg_releases_output_path, 'w', encoding='utf-8') as f:
        json.dump(rg_to_releases, f)
    logger.info(f"✅ Saved RG-to-Releases index for {len(rg_to_releases):,} release groups to {rg_releases_output_path}")

def build_artist_search_index(artist_dump_file: Path, output_dir: Path):
    """Build a compact read-only FTS5 index of all artists."""
    import sqlite3, json, logging
    from unidecode import unidecode
    logger = logging.getLogger(__name__)

    db_path = output_dir / ".." / "artist.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if db_path.exists():
        logger.info(f"→ {db_path} already exists – skipping rebuild.")
        return

    # 1. Build in WAL mode for speed.
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")          # fast bulk insert
    cur.execute("PRAGMA synchronous=OFF;")
    cur.execute("PRAGMA temp_store=MEMORY;")

    # 2. Schema.
    cur.execute("""
        CREATE VIRTUAL TABLE artists_fts USING fts5(
            id UNINDEXED,
            name,
            sort_name,
            unaccented_name
        );
    """)

    # 3. Bulk load – keep a single write transaction open.
    conn.execute("BEGIN;")
    with open(artist_dump_file, encoding="utf-8") as fp:
        for n, line in enumerate(fp, 1):
            try:
                data = json.loads(line)
                aid, name = data.get("id"), data.get("name")
                if not aid or not name:
                    continue
                cur.execute(
                    "INSERT INTO artists_fts(id,name,sort_name,unaccented_name)"
                    "VALUES(?,?,?,?)",
                    (aid, name, data.get("sort-name"), unidecode(name)),
                )
                if n % 100_000 == 0:
                    logger.info(f"  …inserted {n:,} rows")
            except (json.JSONDecodeError, sqlite3.Error):
                continue
    conn.commit()

    # 4. Compact the FTS index.
    cur.execute("INSERT INTO artists_fts(artists_fts) VALUES('optimize');")

    # 5. Flush the WAL back, truncate it, then switch to DELETE mode.
    cur.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    cur.execute("PRAGMA journal_mode=DELETE;")

    # 6. (Optional) shrink the database file.
    cur.execute("VACUUM;")

    conn.close()
    logger.info(f"✅ Built search index with {n:,} artists at {db_path}")


# creates WAL/SHM mode DB which can't be mounted R/O
# def build_artist_search_index(artist_dump_file: Path, output_dir: Path):
#     """Builds an FTS5 search index for artists from the raw dump file."""
#     logger.info(f"Building artist search index from dump file: {artist_dump_file.name}...")
#     db_path = output_dir / ".." /  "artist.db"

#     # Ensure the parent directory for the database exists
#     db_path.parent.mkdir(parents=True, exist_ok=True)

#     if db_path.exists():
#         logger.info(f"-> Search index {db_path} already exists. Skipping rebuild.")
#         return

#     conn = sqlite3.connect(db_path)
#     cursor = conn.cursor()

#     # Create the FTS table
#     cursor.execute("""
#         CREATE VIRTUAL TABLE artists_fts USING fts5(
#             id UNINDEXED,
#             name,
#             sort_name,
#             unaccented_name
#         )
#     """)

#     # --- Data Insertion ---
#     artists_processed = 0
#     with open(artist_dump_file, 'r', encoding='utf-8') as f:
#         for line in f:
#             try:
#                 data = json.loads(line)

#                 # Prepare data for insertion
#                 artist_id = data.get("id")
#                 name = data.get("name")
#                 if not artist_id or not name:
#                     continue

#                 unaccented_name = unidecode(name)

#                 cursor.execute(
#                     """
#                     INSERT INTO artists_fts (id, name, sort_name, unaccented_name)
#                     VALUES (?, ?, ?, ?)
#                     """,
#                     (
#                         artist_id,
#                         name,
#                         data.get("sort-name"),
#                         unaccented_name,
#                     ),
#                 )
#                 artists_processed += 1
#                 if artists_processed % 500000 == 0:
#                     logger.info(f"  ...indexed {artists_processed:,} artists")

#             except json.JSONDecodeError:
#                 # logger.warning(f"Could not decode JSON from line")
#                 continue
#             except sqlite3.Error as e:
#                 logger.error(f"Failed to insert artist {artist_id}: {e}")
#                 continue
#     # --- End Insertion ---

#     conn.commit()
#     conn.close()

#     logger.info(f"✅ Built artist search index with {artists_processed:,} artists at {db_path}")


def main():
    """Main function to build all indexes."""
    logger.info("🚀 Starting index building process...")

    # These paths are relative to the docker-compose environment
    input_dir = Path("/data/current")
    output_dir = Path("/data/processed/indexes")
    # The search index uses normalized data, which is one level up
    processed_dir = Path("/data/processed")
    search_output_dir = processed_dir / "search"

        # Use filtered files if available, fall back to original files
    artist_file = processed_dir / "artist.filtered" if (processed_dir / "artist.filtered").exists() else input_dir / "artist"
    release_group_file = processed_dir / "release-group.filtered" if (processed_dir / "release-group.filtered").exists() else input_dir / "release-group"
    release_file = processed_dir / "release.filtered" if (processed_dir / "release.filtered").exists() else input_dir / "release"

    # Log which files we're using
    if artist_file.name.endswith('.filtered'):
        logger.info("✅ Using pre-processed filtered artist file for index building")
    else:
        logger.info("⚠️  Using original artist file for index building")

    if release_group_file.name.endswith('.filtered'):
        logger.info("✅ Using pre-processed filtered release-group file for index building")
    else:
        logger.info("⚠️  Using original release-group file for index building")

    if PROCESSING_CONFIG["use_full_release_data"]:
        if release_file.name.endswith('.filtered'):
            logger.info("✅ Using pre-processed filtered release file for index building")
        else:
            logger.info("⚠️  Using original release file for index building")

        # --- Smart Idempotency Check with Source File Tracking ---
    # Core indexes (always required)
    core_indexes = [
        output_dir / "artist_to_byte_offset.json",
        output_dir / "rg_to_byte_offset.json",
        output_dir / "artist_to_rg_ids.json"
    ]

    # Release indexes (optional - only required if release processing is working)
    release_indexes = [
        output_dir / "release_to_byte_offset.json",
        output_dir / "rg_to_release_ids.json"
    ]

    # Check core indexes separately from release indexes
    core_indexes_exist = all(f.exists() for f in core_indexes)
    release_indexes_exist = all(f.exists() for f in release_indexes)

    # For idempotency, only require core indexes - release indexes are handled separately
    required_indexes = core_indexes

    # Check which files we're currently using
    using_filtered_files = artist_file.name.endswith('.filtered') or release_group_file.name.endswith('.filtered')

        # Debug: Show status of core and release indexes
    if core_indexes_exist:
        logger.info(f"✅ All {len(core_indexes)} core index files exist")
    else:
        missing_core = [f for f in core_indexes if not f.exists()]
        logger.info(f"🔍 Missing core index files ({len(missing_core)}/{len(core_indexes)}):")
        for missing in missing_core:
            logger.info(f"   ❌ {missing.name}")

    if PROCESSING_CONFIG["use_full_release_data"]:
        if release_indexes_exist:
            logger.info(f"✅ All {len(release_indexes)} release index files exist")
        else:
            missing_release = [f for f in release_indexes if not f.exists()]
            logger.info(f"🔍 Missing release index files ({len(missing_release)}/{len(release_indexes)}):")
            for missing in missing_release:
                logger.info(f"   ❌ {missing.name}")

    # Track what files were used to build existing indexes
    index_source_file = output_dir / "index_source.json"

    if not index_source_file.exists():
        logger.info(f"📝 Index source file not found: {index_source_file.name}")

    # Check idempotency for core indexes only
    if core_indexes_exist and index_source_file.exists():
        # Load the source info for existing indexes
        try:
            with open(index_source_file, 'r') as f:
                source_info = json.load(f)

            indexes_built_from_filtered = source_info.get("using_filtered_files", False)

            if using_filtered_files == indexes_built_from_filtered:
                logger.info("✅ Compatible core index files already exist. Skipping core index building.")
                logger.info(f"   Source: {'Filtered files' if using_filtered_files else 'Original files'}")

                # Core indexes are good, but check release indexes separately
                build_core_indexes = False
            else:
                logger.info("🔄 Core index source mismatch detected - rebuilding core indexes")
                logger.info(f"   Current files: {'Filtered' if using_filtered_files else 'Original'}")
                logger.info(f"   Existing indexes built from: {'Filtered' if indexes_built_from_filtered else 'Original'}")
                # Remove existing core indexes to force rebuild with correct source
                for index_file in core_indexes:
                    if index_file.exists():
                        index_file.unlink()
                if index_source_file.exists():
                    index_source_file.unlink()
                build_core_indexes = True
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Could not read index source info: {e}. Rebuilding core indexes.")
            # Remove existing core indexes to be safe
            for index_file in core_indexes:
                if index_file.exists():
                    index_file.unlink()
            if index_source_file.exists():
                index_source_file.unlink()
            build_core_indexes = True
    elif core_indexes_exist:
        # Core indexes exist but no source info - assume they're from original files
        if using_filtered_files:
            logger.info("🔄 Using filtered files but core indexes lack source info - rebuilding for safety")
            for index_file in core_indexes:
                index_file.unlink()
            build_core_indexes = True
        else:
            logger.info("✅ Existing core index files assumed compatible with original files. Skipping core index building.")
            build_core_indexes = False
    else:
        # Core indexes don't exist - need to build them
        logger.info("📝 Core index files don't exist - building them")
        build_core_indexes = True

    # Ensure input files exist (after extraction)
    if not artist_file.exists() or not release_group_file.exists():
        logger.error(f"❌ Input files not found in {input_dir}. Ensure you have run the initial extraction step.")
        logger.error("You may need to run the main data processor once to trigger the extraction.")
        return 1

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Reading from: {input_dir}")
    logger.info(f"Writing indexes to: {output_dir}")

    # Build core indexes only if needed
    if build_core_indexes:
        logger.info("🔧 Building core indexes...")
        build_artist_line_index(artist_file, output_dir)
        build_rg_indexes(release_group_file, output_dir)
    else:
        logger.info("⏭️  Skipping core index building (already exist and compatible)")

    # Build the FTS search index from the raw artist dump file (has its own idempotency)
    build_artist_search_index(artist_file, search_output_dir)

    if PROCESSING_CONFIG["use_full_release_data"]:
        logger.info("Full release data processing is ENABLED. Checking release indexes...")

        # Smart idempotency check for release indexes
        build_release_indexes_needed = True
        release_source_file = output_dir / "release_index_source.json"

        if release_indexes_exist and release_source_file.exists():
            try:
                with open(release_source_file, 'r') as f:
                    release_source_info = json.load(f)

                last_release_file = release_source_info.get("release_file", "")
                current_release_file = str(release_file)

                # Check if same source file and file hasn't changed
                if (last_release_file == current_release_file and
                    Path(current_release_file).exists()):

                    # Check modification time if available
                    last_mtime = release_source_info.get("release_file_mtime", 0)
                    current_mtime = Path(current_release_file).stat().st_mtime

                    if current_mtime <= last_mtime:
                        logger.info("✅ Compatible release index files already exist and are up to date. Skipping release index building.")
                        logger.info(f"   Source: {Path(current_release_file).name}")
                        build_release_indexes_needed = False
                    else:
                        logger.info("🔄 Release file has been modified - rebuilding release indexes")
                else:
                    logger.info("🔄 Release source file mismatch - rebuilding release indexes")
                    logger.info(f"   Previous: {Path(last_release_file).name if last_release_file else 'None'}")
                    logger.info(f"   Current: {Path(current_release_file).name}")

            except (json.JSONDecodeError, KeyError, OSError) as e:
                logger.warning(f"Could not read release index source info: {e}. Rebuilding release indexes.")
                build_release_indexes_needed = True
        elif release_indexes_exist:
            logger.info("🔍 Release indexes exist but no source info - checking if rebuild needed")
            # If indexes exist but no source info, be conservative and rebuild if using different file type
            if release_file.name.endswith('.filtered'):
                logger.info("🔄 Using filtered file but release indexes lack source info - rebuilding for safety")
                build_release_indexes_needed = True
            else:
                logger.info("✅ Assuming existing release indexes are compatible. Skipping rebuild.")
                build_release_indexes_needed = False
        else:
            logger.info("📝 Release index files don't exist - building them")
            build_release_indexes_needed = True

        if build_release_indexes_needed:
            # Check for release file in order of preference:
            # 1. Filtered release file (fastest processing)
            # 2. Decompressed release file (already extracted)
            # 3. Compressed release.tar.xz file (extract on demand)

            if release_file.exists():
                if release_file.name.endswith('.filtered'):
                    logger.info("✅ Using pre-processed filtered release file for fast index building")
                else:
                    logger.info("ℹ️  Using decompressed release file for index building")

                logger.info("🔧 Building release indexes...")
                build_release_indexes(release_file, output_dir)

                # Save release index source info
                release_source_info = {
                    "release_file": str(release_file),
                    "release_file_mtime": release_file.stat().st_mtime,
                    "timestamp": time.time(),
                    "release_file_type": "filtered" if release_file.name.endswith('.filtered') else "original"
                }

                with open(release_source_file, 'w') as f:
                    json.dump(release_source_info, f, indent=2)

                logger.info("✅ Release indexes built successfully and source info saved")

            else:
                # Check for compressed file and auto-run preprocessing
                release_tar_path = input_dir / "release.tar.xz"
                if release_tar_path.exists():
                    logger.info("📦 Found compressed release.tar.xz file (285GB)")
                    logger.info("🚀 Running automatic preprocessing to create filtered release file...")
                    logger.info("   This avoids extracting 285GB and creates a 96% smaller filtered file")

                    # Auto-run preprocessing to create filtered file
                    try:
                        from data_processor.preprocess import preprocess_release_file_schema_guided_streaming
                        filtered_release_path = processed_dir / "release.filtered"

                        logger.info(f"🔄 Processing {release_tar_path} → {filtered_release_path}")
                        logger.info("⏳ This may take 20-30 minutes for the full 285GB file...")

                        reduction = preprocess_release_file_schema_guided_streaming(release_tar_path, filtered_release_path)

                        logger.info(f"✅ Preprocessing complete! {reduction:.1f}% data reduction achieved")
                        logger.info(f"📁 Filtered release file: {filtered_release_path}")

                        # Now use the filtered file for index building
                        if filtered_release_path.exists():
                            logger.info("🔧 Building release indexes from filtered file...")
                            build_release_indexes(filtered_release_path, output_dir)

                            # Save release index source info
                            release_source_info = {
                                "release_file": str(filtered_release_path),
                                "release_file_mtime": filtered_release_path.stat().st_mtime,
                                "timestamp": time.time(),
                                "release_file_type": "filtered",
                                "created_from_compressed": str(release_tar_path)
                            }

                            with open(release_source_file, 'w') as f:
                                json.dump(release_source_info, f, indent=2)

                            logger.info("✅ Release indexes built successfully from filtered data")
                        else:
                            logger.error("❌ Failed to create filtered release file")
                            return 1

                    except Exception as e:
                        logger.error(f"❌ Failed to run preprocessing: {e}")
                        logger.error("💡 You can run preprocessing manually:")
                        logger.error("   docker-compose -f docker-compose.flat-file.yml run data-processor python data_processor/preprocess.py")
                        return 1
                else:
                    logger.error("❌ No release file found. Looked for:")
                    logger.error(f"   • Filtered: {release_file}")
                    logger.error(f"   • Decompressed: {input_dir / 'release'}")
                    logger.error(f"   • Compressed: {release_tar_path}")
                    logger.error("")
                    logger.error("💡 To enable release processing:")
                    logger.error("   • Add release.tar.xz to /data/current/ (285GB compressed)")
                    logger.error("   • System will automatically create filtered file")
                    return 1
        else:
            logger.info("⏭️  Skipping release index building (already exist and compatible)")
    else:
        logger.info("Full release data processing is DISABLED. Skipping release index build.")


        # Save source information for future compatibility checks (only if we built core indexes)
    if build_core_indexes:
        source_info = {
            "using_filtered_files": using_filtered_files,
            "artist_file": str(artist_file),
            "release_group_file": str(release_group_file),
            "timestamp": time.time(),
            "processing_config": {
                "use_full_release_data": PROCESSING_CONFIG["use_full_release_data"]
            }
        }

        # Include release file info if release processing is enabled
        if PROCESSING_CONFIG["use_full_release_data"]:
            source_info["release_file"] = str(release_file)

        with open(index_source_file, 'w') as f:
            json.dump(source_info, f, indent=2)

        logger.info("💾 Saved index source information for future compatibility checks")

    if build_core_indexes:
        logger.info("🎉 Core indexes built successfully!")
    else:
        logger.info("✅ Index building completed (core indexes skipped, release indexes handled separately)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
