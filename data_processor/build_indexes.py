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
    rg_releases_output_path = output_dir / "rg_to_release_ids.json"

    # Skip rebuilding only if *both* release indexes already exist.
    if release_offset_output_path.exists() and rg_releases_output_path.exists():
        logger.info("✅ Both release index files already exist – skipping rebuild.")
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
                rg_id = data.get("release-group", {}).get("id") or data.get("release_group_id")
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
                aid = data.get("id")
                name = data.get("name") or data.get("artistName") or data.get("artistname")
                if not aid or not name:
                    continue
                sort_name = data.get("sort-name") or data.get("sortName") or data.get("sortname")
                cur.execute(
                    "INSERT INTO artists_fts(id,name,sort_name,unaccented_name)"
                    "VALUES(?,?,?,?)",
                    (aid, name, sort_name, unidecode(name)),
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


def should_rebuild_core_indexes(output_dir: Path, artist_file: Path, release_group_file: Path) -> bool:
    """Determine if core indexes need to be rebuilt based on existence and source."""
    core_indexes = [
        output_dir / "artist_to_byte_offset.json",
        output_dir / "rg_to_byte_offset.json",
        output_dir / "artist_to_rg_ids.json"
    ]
    index_source_file = output_dir / "index_source.json"
    core_indexes_exist = all(f.exists() for f in core_indexes)

    if not core_indexes_exist:
        logger.info("📝 Core indexes don't exist - rebuilding required.")
        return True

    if not index_source_file.exists():
        logger.warning("Core indexes exist but source info is missing - rebuilding for safety.")
        return True

    try:
        with open(index_source_file, 'r') as f:
            source_info = json.load(f)

        # Check if the source files recorded in the manifest match the current source files
        last_artist_file = source_info.get("artist_file")
        last_rg_file = source_info.get("release_group_file")

        if last_artist_file != str(artist_file) or last_rg_file != str(release_group_file):
            logger.info("🔄 Source file mismatch detected - rebuilding core indexes.")
            logger.info(f"   Artist file changed: {last_artist_file} -> {artist_file}")
            logger.info(f"   Release group file changed: {last_rg_file} -> {release_group_file}")
            return True

    except (json.JSONDecodeError, KeyError) as e:
        logger.warning(f"Could not read index source info: {e}. Rebuilding core indexes.")
        return True

    logger.info("✅ Compatible core indexes already exist. Skipping rebuild.")
    return False

def should_rebuild_release_indexes(output_dir: Path, release_file: Path) -> bool:
    """Determine if release indexes need to be rebuilt based on existence and source."""
    if not PROCESSING_CONFIG.get("use_full_release_data"):
        logger.info("Full release data processing is DISABLED. Skipping release index build.")
        return False

    release_indexes = [
        output_dir / "release_to_byte_offset.json",
        output_dir / "rg_to_release_ids.json"
    ]
    release_source_file = output_dir / "release_index_source.json"
    release_indexes_exist = all(f.exists() for f in release_indexes)

    if not release_indexes_exist:
        logger.info("📝 Release indexes don't exist - rebuilding required.")
        return True

    if not release_source_file.exists():
        logger.warning("Release indexes exist but source info is missing - rebuilding for safety.")
        return True

    try:
        with open(release_source_file, 'r') as f:
            source_info = json.load(f)

        last_release_file = source_info.get("release_file")
        last_mtime = source_info.get("release_file_mtime", 0)

        if last_release_file != str(release_file):
            logger.info(f"🔄 Release source file mismatch: {last_release_file} -> {release_file}. Rebuilding.")
            return True

        if release_file.exists() and release_file.stat().st_mtime > last_mtime:
            logger.info(f"🔄 Release file {release_file.name} has been modified. Rebuilding.")
            return True

    except (json.JSONDecodeError, KeyError, OSError) as e:
        logger.warning(f"Could not read release index source info: {e}. Rebuilding release indexes.")
        return True

    logger.info("✅ Compatible release indexes already exist and are up-to-date. Skipping rebuild.")
    return False

def save_core_index_source(output_dir: Path, artist_file: Path, release_group_file: Path):
    """Save metadata about the source files used for the core indexes."""
    source_info = {
        "artist_file": str(artist_file),
        "release_group_file": str(release_group_file),
        "timestamp": time.time(),
    }
    with open(output_dir / "index_source.json", 'w') as f:
        json.dump(source_info, f, indent=2)

def save_release_index_source(output_dir: Path, release_file: Path, **kwargs):
    """Save metadata about the source file used for the release indexes."""
    source_info = {
        "release_file": str(release_file),
        "release_file_mtime": release_file.stat().st_mtime if release_file.exists() else 0,
        "timestamp": time.time(),
        **kwargs
    }
    with open(output_dir / "release_index_source.json", 'w') as f:
        json.dump(source_info, f, indent=2)


def main():
    """Main function to build all indexes."""
    logger.info("🚀 Starting index building process...")

    # These paths are relative to the docker-compose environment
    input_dir = Path("/data/current")
    output_dir = Path("/data/processed/indexes")
    processed_dir = Path("/data/processed")

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Determine File Sources ---
    artist_file = processed_dir / "artist.filtered" if (processed_dir / "artist.filtered").exists() else input_dir / "artist"
    release_group_file = processed_dir / "release-group.filtered" if (processed_dir / "release-group.filtered").exists() else input_dir / "release-group"

    logger.info(f"Using artist file: {artist_file}")
    logger.info(f"Using release group file: {release_group_file}")

    # --- Core Index Building ---
    if should_rebuild_core_indexes(output_dir, artist_file, release_group_file):
        if not artist_file.exists() or not release_group_file.exists():
            logger.error(f"❌ Input files not found. Ensure artist/release-group files exist.")
            return 1

        logger.info("🔧 Building core indexes...")
        build_artist_line_index(artist_file, output_dir)
        build_rg_indexes(release_group_file, output_dir)
        save_core_index_source(output_dir, artist_file, release_group_file)
        logger.info("✅ Core indexes built successfully!")

    # --- Release Index Building ---
    release_file = processed_dir / "release.filtered" if (processed_dir / "release.filtered").exists() else input_dir / "release"
    if should_rebuild_release_indexes(output_dir, release_file):
        logger.info(f"Using release file: {release_file}")

        if not release_file.exists():
            release_tar_path = input_dir / "release.tar.xz"
            if not release_tar_path.exists():
                logger.error("❌ No release data source found ('release', 'release.filtered', or 'release.tar.xz').")
                return 1

            logger.info("🚀 Found compressed release.tar.xz. Running automatic preprocessing...")
            try:
                from data_processor.preprocess import preprocess_release_file_schema_guided_streaming
                filtered_release_path = processed_dir / "release.filtered"
                preprocess_release_file_schema_guided_streaming(release_tar_path, filtered_release_path)
                release_file = filtered_release_path # Use the newly created file
                if not release_file.exists():
                    logger.error("❌ Preprocessing finished but filtered release file not found.")
                    return 1
            except Exception as e:
                logger.error(f"❌ Failed to run preprocessing: {e}")
                return 1

        logger.info("🔧 Building release indexes...")
        build_release_indexes(release_file, output_dir)
        save_release_index_source(output_dir, release_file)
        logger.info("✅ Release indexes built successfully!")

    logger.info("🎉 Index building process completed.")
    return 0


if __name__ == "__main__":
    # Temporarily override config for testing to ensure release data is processed
    PROCESSING_CONFIG['use_full_release_data'] = True
    sys.exit(main())
